#!/usr/bin/env python3
"""
arb_helpers_v3.py (concurrent, rate-limited, retrying)
- PancakeSwap V3 N×N matrix builder with parallel pool discovery & slot0 reads.
- Robust raw slot0 eth_call (bypasses int24 ABI decode issues) with retry/backoff.
- Per-RPC rate limiting (--rpc-qps) to avoid -32005 "limit exceeded".
- Caches pool addresses & slot0 within a run to reduce pressure on the RPC.
- Pivot-based synth edges to fill missing pairs.
- Size-aware PnL via Quoter (optional).
- Router gas estimate for exactInput (optional).
- CSV logging.
"""

import os, sys, json, csv, time, argparse, random
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from itertools import permutations
from typing import Dict, Tuple, List, Optional

import numpy as np
from web3 import Web3
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import BoundedSemaphore, Lock

getcontext().prec = 50

# -----------------------------
# Parsing helpers
# -----------------------------
def parse_interval_to_seconds(s: str) -> int:
    s = (s or "").strip().lower()
    if s.endswith("s"):
        return int(float(s[:-1]))
    if s.endswith("m"):
        return int(float(s[:-1]) * 60)
    if s.endswith("h"):
        return int(float(s[:-1]) * 3600)
    if s.endswith("d"):
        return int(float(s[:-1]) * 86400)
    return int(float(s or "0"))

def parse_args():
    import argparse

    p = argparse.ArgumentParser()
    # ---- scanning & loops ----
    p.add_argument("--interval", type=float, default=0.0)
    p.add_argument("--loops", type=int, default=1)
    p.add_argument("--top", type=int, default=10)
    p.add_argument("--threshold-bps", type=float, default=0.0,
                   help="legacy raw edge cutoff in bps (kept for compat)")
    p.add_argument("--min-edge-bps", type=float, default=0.0,
                   help="minimum raw matrix edge (bps) before size-quoting")

    # ---- i/o & debug ----
    p.add_argument("--progress", action="store_true")
    p.add_argument("--progress-every", type=int, default=50)
    p.add_argument("--debug-pools", action="store_true")
    p.add_argument("--debug-sim", action="store_true")

    # ---- discovery / synthesis ----
    p.add_argument("--no-synth", action="store_true",
                   help="disable synthetic pivots; only real on-chain hops")

    # ---- tokens & network ----
    p.add_argument("--tokens", default="tokens_from_pools.json")
    p.add_argument("--rpc", default=None, help="RPC URL (or use env like BSC_RPC_URL)")
    p.add_argument("--factory-v3", required=True)
    p.add_argument("--quoter", required=True)
    p.add_argument("--router-v3", required=True)
    p.add_argument("--fee-tiers", default="500", help="comma list e.g. 100,500,2500,10000")
    p.add_argument("--pivot", default=None)

    # ---- sizing & filters ----
    p.add_argument("--notional-usd", type=float, default=0.0)
    p.add_argument("--min-reserve-usd", type=float, default=0.0)

    # ---- csv / logs ----
    p.add_argument("--csv", default=None)

    # ---- gas & wallet ----
    p.add_argument("--estimate-gas", dest="estimate_gas", action="store_true")
    p.add_argument("--no-estimate-gas", dest="estimate_gas", action="store_false")
    p.set_defaults(estimate_gas=False)

    p.add_argument("--from-addr", default=None)
    p.add_argument("--gas-price-gwei", type=float, default=None)
    p.add_argument("--gas-limit", type=int, default=500000)
    p.add_argument("--gas-multiplier", type=float, default=1.0)
    p.add_argument("--slippage-bps", type=int, default=10)
    p.add_argument("--deadline-seconds", type=int, default=45)

    # ---- concurrency / rpc QoS ----
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--rpc-qps", type=float, default=5.0)
    p.add_argument("--timeout", type=float, default=12.0)
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--decimals-parallel", type=int, default=16)

    # ---- execution control ----
    p.add_argument("--dry-run", action="store_true",
                   help="Build/quote/estimate only; do NOT broadcast")
    p.add_argument("--execute", action="store_true",
                   help="Allow broadcasting when checks pass")
    p.add_argument("--pk-env", default="PK",
                   help="Env var name that holds your hex private key")
    p.add_argument("--min-pnl-usd", type=float, default=0.0,
                   help="Require net PnL >= this to execute")
    p.add_argument("--max-gwei", type=float, default=0.0,
                   help="Skip execution if gas price exceeds this (0=ignore)")

    args = p.parse_args()
    return args


# -----------------------------
# Simple process-wide rate limiter
# -----------------------------
class QPSLimiter:
    def __init__(self, qps: float):
        self.min_interval = 1.0 / float(qps)
        self._lock = Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.time()
            dt = now - self._last
            if dt < self.min_interval:
                time.sleep(self.min_interval - dt + random.uniform(0, 0.01))
            self._last = time.time()

limiter = None  # set in main()

def limited_call(fn, *args, retries=5, base_delay=0.35, **kwargs):
    """
    Wrap an RPC call with QPS limiting + exponential backoff on common failures.
    """
    for attempt in range(1, retries+1):
        if limiter:
            limiter.wait()
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e)
            if attempt == retries:
                raise
            # jittered backoff
            delay = base_delay * (2 ** (attempt-1) + random.uniform(0, 0.2))
            # small extra delay if rate-limit code seen
            if "-32005" in msg or "limit exceeded" in msg:
                delay += 0.5
            time.sleep(delay)

# -----------------------------
# Web3 helpers
# -----------------------------
def _inject_poa_middleware(w3: Web3):
    for mod, name in [
        ("web3.middleware","geth_poa_middleware"),
        ("web3.middleware.proof_of_authority","build_poa_middleware"),
        ("web3.middleware.proof_of_authority","POAMiddleware"),
        ("web3.middleware","ExtraDataToPOAMiddleware"),
    ]:
        try:
            m = __import__(mod, fromlist=[name])
            w3.middleware_onion.inject(getattr(m, name), layer=0)
            break
        except Exception:
            pass

def _is_connected(w3: Web3) -> bool:
    try:
        return w3.is_connected()
    except Exception:
        return w3.isConnected()

def make_web3(rpc_url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 25}))
    _inject_poa_middleware(w3)
    if not _is_connected(w3):
        raise SystemExit("Web3: not connected. Check RPC URL or your network.")
    return w3

# -----------------------------
# ABIs
# -----------------------------
FACTORY_V3_ABI = [
    {"inputs":[{"internalType":"address","name":"tokenA","type":"address"},
               {"internalType":"address","name":"tokenB","type":"address"},
               {"internalType":"uint24","name":"fee","type":"uint24"}],
     "name":"getPool","outputs":[{"internalType":"address","name":"pool","type":"address"}],
     "stateMutability":"view","type":"function"}
]
POOL_V3_ABI_MIN = [
    {"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],
     "stateMutability":"view","type":"function"},
    {"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],
     "stateMutability":"view","type":"function"},
    {"inputs":[],"name":"fee","outputs":[{"internalType":"uint24","name":"","type":"uint24"}],
     "stateMutability":"view","type":"function"}
]
ERC20_ABI = [
    {"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],
     "stateMutability":"view","type":"function"},
    {"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],
     "stateMutability":"view","type":"function"},
]
QUOTER_ABI = [
  {"inputs":[
      {"internalType":"address","name":"tokenIn","type":"address"},
      {"internalType":"address","name":"tokenOut","type":"address"},
      {"internalType":"uint24","name":"fee","type":"uint24"},
      {"internalType":"uint256","name":"amountIn","type":"uint256"},
      {"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],
   "name":"quoteExactInputSingle",
   "outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],
   "stateMutability":"nonpayable","type":"function"}
]
ROUTER_V3_ABI = [
  {"inputs":[{"components":[
          {"internalType":"bytes","name":"path","type":"bytes"},
          {"internalType":"address","name":"recipient","type":"address"},
          {"internalType":"uint256","name":"deadline","type":"uint256"},
          {"internalType":"uint256","name":"amountIn","type":"uint256"},
          {"internalType":"uint256","name":"amountOutMinimum","type":"uint256"}],
       "internalType":"struct ExactInputParams","name":"params","type":"tuple"}],
   "name":"exactInput","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],
   "stateMutability":"payable","type":"function"}
]

# -----------------------------
# Defaults
# -----------------------------
DEFAULT_TOKENS_9 = [
    {"symbol":"WBNB","address":"0xBB4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"},
    {"symbol":"USDT","address":"0x55d398326f99059fF775485246999027B3197955"},
    {"symbol":"BUSD","address":"0xe9e7CEA3Dedca5984780Bafc599bD69ADd087D56"},
    {"symbol":"USDC","address":"0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d"},
    {"symbol":"DAI", "address":"0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3"},
    {"symbol":"ETH", "address":"0x2170Ed0880ac9A755fd29B2688956BD959F933F8"},
    {"symbol":"BTCB","address":"0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c"},
    {"symbol":"CAKE","address":"0x0E09Fabb73Bd3Ade0a17ECC321fD13a19e81cE82"},
    {"symbol":"FDUSD","address":"0x7aB622a8F4eD8C56bf5C67aA4eF8aCf82D54eF2e"},
]
STABLES = {"USDT","USDC","BUSD","DAI","FDUSD"}

# -----------------------------
# Token utils
# -----------------------------
def load_tokens(path: str):
    if not path:
        return DEFAULT_TOKENS_9
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list) or not data:
        raise ValueError("Token JSON must be a non-empty list")
    return data

def to_checksum(a: str) -> str:
    return Web3.to_checksum_address(a)

def erc20_decimals(w3: Web3, addr: str) -> int:
    c = w3.eth.contract(address=to_checksum(addr), abi=ERC20_ABI)
    try:
        return int(limited_call(c.functions.decimals().call))
    except Exception:
        return 18

# -----------------------------
# V3 math
# -----------------------------
def price_from_sqrtPriceX96(sqrtPriceX96: int, dec0: int, dec1: int) -> float:
    num = Decimal(sqrtPriceX96) * Decimal(sqrtPriceX96)
    denom = Decimal(2) ** 192
    ratio = num / denom
    scale = Decimal(10) ** Decimal(dec0 - dec1)
    return float(ratio * scale)

SLOT0_SELECTOR = "0x3850c7bd"

def read_sqrtPriceX96_raw(w3: Web3, pool_addr: str, retries: int = 5) -> Optional[int]:
    for attempt in range(1, retries+1):
        if limiter:
            limiter.wait()
        try:
            data = w3.eth.call({"to": to_checksum(pool_addr), "data": SLOT0_SELECTOR})
            if not data or len(data) < 32:
                raise RuntimeError("empty slot0 data")
            word0 = int.from_bytes(data[:32], byteorder="big")
            sqrtP = word0 & ((1 << 160) - 1)
            return int(sqrtP)
        except Exception:
            if attempt == retries:
                return None
            delay = 0.35 * (2 ** (attempt-1) + random.uniform(0, 0.2))
            time.sleep(delay)

# -----------------------------
# Matrix build (parallel)
# -----------------------------
def build_rate_matrix_v3(
    w3: Web3,
    tokens,
    factory_addr: str,
    fee_tiers,
    pivot_symbol: str,
    debug_pools: bool = False,
    concurrency: int = 24
):
    n = len(tokens)
    names = [t["symbol"].upper() for t in tokens]
    addrs = [to_checksum(t["address"]) for t in tokens]
    addr_map = {names[i]: addrs[i] for i in range(n)}

    # decimals prefetch
    dec_map: Dict[str, int] = {}
    for a in addrs:
        dec_map[a] = erc20_decimals(w3, a)

    factory = w3.eth.contract(address=to_checksum(factory_addr), abi=FACTORY_V3_ABI)

    # caches within this build to cut RPC load
    pool_cache: Dict[Tuple[str, str, int], str] = {}
    slot0_cache: Dict[str, Optional[int]] = {}

    R = np.full((n, n), np.nan, dtype=float)
    best_fee: Dict[Tuple[int, int], int] = {}
    composed_edges: Dict[Tuple[int, int], List[Tuple[int, int]]] = {}
    for i in range(n):
        R[i, i] = 1.0

    # all ordered pairs
    pair_tasks = [(i, j) for i in range(n) for j in range(n) if i != j]

    sem = BoundedSemaphore(concurrency)

    def get_pool_cached(a: str, b: str, fee: int) -> str:
        key = (a, b, int(fee)) if a.lower() < b.lower() else (b, a, int(fee))
        if key in pool_cache:
            return pool_cache[key]
        t0, t1 = (a, b) if a.lower() < b.lower() else (b, a)
        try:
            # call factory.getPool with rate limiting
            pool_addr = limited_call(factory.functions.getPool(t0, t1, int(fee)).call)
        except Exception as e:
            if debug_pools:
                print(f"[v3] error getPool({t0},{t1}, fee={fee}): {e}")
            pool_addr = "0x0000000000000000000000000000000000000000"
        pool_cache[key] = pool_addr
        if int(pool_addr, 16) == 0 and debug_pools:
            print(f"[v3] getPool({t0},{t1}, fee={fee}) -> 0x0")
        return pool_addr

    def read_slot0_cached(pool_addr: str) -> Optional[int]:
        if pool_addr in slot0_cache:
            return slot0_cache[pool_addr]
        sqrtP = read_sqrtPriceX96_raw(w3, pool_addr)
        slot0_cache[pool_addr] = sqrtP
        if sqrtP is None and debug_pools:
            print(f"[v3] slot0 raw call failed for pool {pool_addr}")
        return sqrtP

    def solve_pair(i: int, j: int):
        with sem:
            a, b = addrs[i], addrs[j]
            for fee in fee_tiers:
                try:
                    pool_addr = get_pool_cached(a, b, int(fee))
                    if int(pool_addr, 16) == 0:
                        continue
                    pool = w3.eth.contract(address=to_checksum(pool_addr), abi=POOL_V3_ABI_MIN)
                    chain_t0 = to_checksum(limited_call(pool.functions.token0().call))
                    chain_t1 = to_checksum(limited_call(pool.functions.token1().call))
                    dec0 = dec_map.get(chain_t0, 18)
                    dec1 = dec_map.get(chain_t1, 18)
                    sqrtP = read_slot0_cached(pool_addr)
                    if sqrtP is None:
                        continue
                    p_t1_per_t0 = price_from_sqrtPriceX96(int(sqrtP), dec0, dec1)
                    fmult = 1.0 - (int(fee) / 1_000_000.0)
                    if to_checksum(a) == chain_t0 and to_checksum(b) == chain_t1:
                        r_ab = p_t1_per_t0 * fmult
                        r_ba = (1.0 / p_t1_per_t0) * fmult if p_t1_per_t0 > 0 else float("nan")
                    elif to_checksum(a) == chain_t1 and to_checksum(b) == chain_t0:
                        r_ab = (1.0 / p_t1_per_t0) * fmult if p_t1_per_t0 > 0 else float("nan")
                        r_ba = p_t1_per_t0 * fmult
                    else:
                        if debug_pools:
                            print(f"[v3] token0/1 mismatch for pool {pool_addr}")
                        continue
                    return (i, j, float(r_ab), float(r_ba), int(fee))
                except Exception as e:
                    if debug_pools:
                        print(f"[v3] error for pair {names[i]}-{names[j]} fee={fee}: {e}")
                    continue
            return (i, j, float("nan"), float("nan"), None)

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(solve_pair, i, j) for (i, j) in pair_tasks]
        for fut in as_completed(futures):
            i, j, r_ab, r_ba, fee = fut.result()
            if fee is not None and np.isfinite(r_ab) and np.isfinite(r_ba):
                R[i, j] = r_ab
                R[j, i] = r_ba
                best_fee[(i, j)] = best_fee[(j, i)] = fee
            else:
                R[i, j] = float("nan")

    # Pivot synth
    pivot_idx = names.index(pivot_symbol) if pivot_symbol and pivot_symbol in names else None
    if pivot_idx is not None:
        for i in range(n):
            for j in range(n):
                if i == j:
                    R[i, j] = 1.0
                    continue
                if not np.isfinite(R[i, j]):
                    if np.isfinite(R[i, pivot_idx]) and np.isfinite(R[pivot_idx, j]):
                        R[i, j] = R[i, pivot_idx] * R[pivot_idx, j]
                        composed_edges[(i, j)] = [(i, pivot_idx), (pivot_idx, j)]
                        best_fee[(i, j)] = -1

    return names, addrs, R, addr_map, composed_edges, best_fee, dec_map

# -----------------------------
# USD estimates (rough)
# -----------------------------
def estimate_usd_price_for_symbol(names, addrs, R, sym: str) -> Optional[float]:
    if sym.upper() in {"USDT", "USDC", "BUSD", "DAI", "FDUSD"}:
        return 1.0
    if sym not in names:
        return None
    i = names.index(sym)
    vals = []
    for j, t in enumerate(names):
        if t in {"USDT", "USDC", "BUSD", "DAI", "FDUSD"}:
            rate = R[i, j]
            if np.isfinite(rate) and rate > 0:
                vals.append(rate)
    return float(np.median(vals)) if vals else None

# -----------------------------
# Pretty print matrix
# -----------------------------
def pretty_print_matrix(names, R):
    width = max(6, max(len(n) for n in names))
    print("\nEffective spot matrix R (PCS V3, fee applied or synth via pivot):")
    header = " " * (width + 1) + "  ".join(f"{n:>{width}}" for n in names)
    print(header)
    for i, row in enumerate(R):
        def cell(x):
            return f"{x:>{width}.6f}" if np.isfinite(x) else f"{'NaN':>{width}}"
        print(f"{names[i]:>{width}} " + "  ".join(cell(x) for x in row))

# -----------------------------
# Cycle scanner
# -----------------------------
def scan_triangular_opps(names, R, threshold_bps: float):
    n = len(names)
    res = []
    for i, j, k in permutations(range(n), 3):
        a, b, c = R[i, j], R[j, k], R[k, i]
        if not (np.isfinite(a) and np.isfinite(b) and np.isfinite(c)):
            continue
        prod = a * b * c
        prof = prod - 1.0
        if prof > (threshold_bps / 10_000.0):
            res.append({
                "route": (names[i], names[j], names[k], names[i]),
                "factors": (a, b, c),
                "product": prod,
                "profit_pct": prof * 100
            })
    # dedupe isomorphic cycles (same tokens, different start)
    ded = {}
    for r in res:
        key = tuple(sorted(r["route"][:3]))
        if key not in ded or r["product"] > ded[key]["product"]:
            ded[key] = r
    return sorted(ded.values(), key=lambda x: x["product"], reverse=True)

# -----------------------------
# Quoter + Router helpers
# -----------------------------
def quoter_amount_out(w3, quoter_addr, token_in, token_out, fee, amount_in_wei):
    q = w3.eth.contract(address=to_checksum(quoter_addr), abi=QUOTER_ABI)
    try:
        return limited_call(q.functions.quoteExactInputSingle(
            to_checksum(token_in), to_checksum(token_out), int(fee), int(amount_in_wei), 0
        ).call)
    except Exception:
        return None

def encode_path(addresses, fees):
    assert len(addresses) >= 2 and len(fees) == len(addresses) - 1
    b = b""
    for i in range(len(fees)):
        b += bytes.fromhex(addresses[i][2:])
        b += int(fees[i]).to_bytes(3, "big")
    b += bytes.fromhex(addresses[-1][2:])
    return b

def router_estimate_gas_exact_input(
    w3,
    router_addr,
    path_addrs,
    path_fees,
    amount_in,
    amount_out_min,
    recipient,
    deadline_sec,
    from_addr
):
    router = w3.eth.contract(
        address=Web3.to_checksum_address(router_addr),
        abi=ROUTER_V3_ABI
    )
    path_bytes = encode_path(
        [Web3.to_checksum_address(a) for a in path_addrs],
        path_fees
    )
    params = (
        path_bytes,
        Web3.to_checksum_address(recipient),
        int(time.time() + int(deadline_sec)),
        int(amount_in),
        int(amount_out_min)
    )
    try:
        return limited_call(
            router.functions.exactInput(params).estimate_gas,
            {"from": Web3.to_checksum_address(from_addr), "value": 0}
        )
    except Exception:
        return None

def expand_cycle_with_fees(cycle_syms, names, composed_edges, best_fee_map):
    idx = {names[i]: i for i in range(len(names))}
    hops_syms = []
    hop_fees = []
    for u, v in zip(cycle_syms[:-1], cycle_syms[1:]):
        i, j = idx[u], idx[v]
        if (i, j) in best_fee_map and best_fee_map.get((i, j)) not in (None, -1):
            hops_syms.append(u)
            hop_fees.append(best_fee_map[(i, j)])
        elif (i, j) in composed_edges:
            hops_syms.append(u)
            e1, e2 = composed_edges[(i, j)]
            hop_fees.append(best_fee_map.get(e1, 500))
            hops_syms.append(names[e1[1]])
            hop_fees.append(best_fee_map.get(e2, 500))
        else:
            hops_syms.append(u)
            hop_fees.append(500)
    hops_syms.append(cycle_syms[-1])
    return hops_syms, hop_fees

# -----------------------------
# CLI entry (called by pancake_live_arb_v3.py)
# -----------------------------
def main():
    global limiter
    args = parse_args()
    rpc = (args.rpc or os.environ.get("BSC_RPC_URL", "").strip())
    if not rpc:
        print("Set --rpc or BSC_RPC_URL", file=sys.stderr); sys.exit(2)
    w3 = make_web3(rpc)

    limiter = QPSLimiter(args.rpc_qps)

    tokens = load_tokens(args.tokens)
    if len(tokens) < 3:
        print("Need at least 3 tokens.", file=sys.stderr); sys.exit(2)

    names, addrs, R, addr_map, composed_edges, best_fee_map, dec_map = build_rate_matrix_v3(
        w3, tokens, args.factory_v3, args.fee_tiers, args.pivot.upper().strip() if args.pivot else "",
        args.debug_pools, args.concurrency
    )
    pretty_print_matrix(names, R)

    usd_price_map = {sym: estimate_usd_price_for_symbol(names, addrs, R, sym) for sym in names}

    # Baseline gas display
    if args.estimate_gas:
        try:
            gas_price_wei = int(args.gas_price_gwei * 1e9) if args.gas_price_gwei > 0 else int(w3.eth.gas_price)
        except Exception:
            gas_price_wei = int(3e9)
        gas_units = int(max(1, args.gas_limit) * max(0.1, args.gas_multiplier))
        bnb_usd = usd_price_map.get("WBNB", None)
        msg = f"Gas (baseline): ~{gas_units} @ {gas_price_wei/1e9:.2f} gwei"
        if bnb_usd:
            gas_cost_bnb = (gas_price_wei * gas_units) / 1e18
            msg += f" ≈ {gas_cost_bnb:.6f} BNB (${gas_cost_bnb*bnb_usd:.2f})"
        print(msg)

    opps = scan_triangular_opps(names, R, args.threshold_bps)
    if not opps:
        print("\nNo profitable triangular cycles at the current snapshot.")
        return

    show = min(args.top, len(opps))
    print(f"\nProfitable triangular cycles found: {len(opps)} (showing top {show})")

    csv_rows = []
    for idx, opp in enumerate(opps[:show], 1):
        route_syms = list(opp["route"])
        print(f"{idx:2d}. {' -> '.join(route_syms)} | factors: "
              f"[{opp['factors'][0]:.6f}, {opp['factors'][1]:.6f}, {opp['factors'][2]:.6f}] "
              f"| product: {opp['product']:.6f} | profit: {opp['profit_pct']:.4f}%")

        path_syms, path_fees = expand_cycle_with_fees(route_syms, names, composed_edges, best_fee_map)

        sim_text = "size-aware: skipped"
        net_text = ""
        router_gas_units = None

        if args.notional_usd and args.notional_usd > 0:
            A = path_syms[0]
            pA = usd_price_map.get(A, None)
            if not pA or pA <= 0:
                sim_text = "size-aware: skipped (no_usd_price_for_start)"
            else:
                decA = dec_map[addr_map[A]]
                amt_in_wei_start = int(Decimal(args.notional_usd / pA) * (Decimal(10) ** decA))
                amt_in_wei = amt_in_wei_start
                ok = True
                for (u, v, fee) in zip(path_syms[:-1], path_syms[1:], path_fees):
                    out = quoter_amount_out(w3, args.quoter, addr_map[u], addr_map[v], int(fee), int(amt_in_wei))
                    if out is None or out <= 0:
                        ok = False
                        sim_text = f"size-aware[{'->'.join(path_syms)}]: quoter_failed({u}->{v}@{fee})"
                        break
                    amt_in_wei = out
                if ok:
                    final_decA = dec_map[addr_map[A]]
                    final_amt_a = float(Decimal(amt_in_wei) / (Decimal(10) ** final_decA))
                    final_usd = final_amt_a * pA
                    pnl_usd = final_usd - args.notional_usd
                    pnl_pct = (pnl_usd / args.notional_usd) * 100.0
                    sim_text = (f"size-aware[{'->'.join(path_syms)}]: start ${args.notional_usd:.2f} "
                                f"→ end ${final_usd:.2f} (PnL ${pnl_usd:.2f}, {pnl_pct:.4f}%)")

                    if args.estimate_gas and args.from_addr:
                        try:
                            min_out = int(Decimal(amt_in_wei) * (Decimal(1) - Decimal(args.slippage_bps) / Decimal(10_000)))
                            router_gas_units = router_estimate_gas_exact_input(
                                w3, args.router_v3, [addr_map[s] for s in path_syms], path_fees,
                                amount_in=int(amt_in_wei_start),
                                amount_out_min=min_out, recipient=args.from_addr,
                                deadline_sec=int(args.deadline_seconds), from_addr=args.from_addr
                            )
                            if router_gas_units:
                                gas_price_wei_eff = int(args.gas_price_gwei * 1e9) if args.gas_price_gwei > 0 else int(w3.eth.gas_price)
                                bnb_usd = usd_price_map.get("WBNB", None)
                                if bnb_usd:
                                    gas_cost_usd_rt = (router_gas_units * gas_price_wei_eff) / 1e18 * bnb_usd
                                    net_usd = pnl_usd - gas_cost_usd_rt
                                    net_pct = (net_usd / args.notional_usd) * 100.0
                                    net_text = f", net after gas: ${net_usd:.2f} ({net_pct:.4f}%) [gas {router_gas_units}u]"
                                else:
                                    net_text = f", gas {router_gas_units}u (WBNB USD unknown)"
                            else:
                                net_text = "; router_gas: n/a (estimate failed)"
                        except Exception as e:
                            net_text = f"; router_gas: n/a ({e})"

        print(f"     {sim_text}{net_text}")

        if args.csv:
            csv_rows.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "route": " -> ".join(route_syms),
                "path": " -> ".join(path_syms),
                "product": f"{opp['product']:.10f}",
                "profit_pct": f"{opp['profit_pct']:.6f}",
                "f1": f"{opp['factors'][0]:.10f}",
                "f2": f"{opp['factors'][1]:.10f}",
                "f3": f"{opp['factors'][2]:.10f}",
                "router_gas_units": str(router_gas_units) if router_gas_units else "",
            })

    if args.csv and csv_rows:
        fields = ["timestamp","route","path","product","profit_pct","f1","f2","f3","router_gas_units"]
        exists = os.path.exists(args.csv)
        with open(args.csv, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            if not exists: w.writeheader()
            for r in csv_rows: w.writerow(r)

if __name__ == "__main__":
    main()
