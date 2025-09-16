
from __future__ import annotations
from typing import Dict, Tuple, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import BoundedSemaphore
import time, random
import numpy as np
from web3 import Web3
from .utils import to_checksum, limited_call, erc20_decimals, QPSLimiter, limiter, STABLES
from .math import price_from_sqrtPriceX96

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

SLOT0_SELECTOR = "0x3850c7bd"  # slot0()

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

def prefetch_decimals_parallel(w3: Web3, addrs, names, par: int, show_progress: bool):
    dec_map: Dict[str, int] = {}
    total = len(addrs)
    printed = 0

    def one(a):
        return erc20_decimals(w3, a)

    if par <= 1:
        for i, a in enumerate(addrs, 1):
            dec_map[a] = one(a)
            if show_progress:
                print(f"[decimals] {i}/{total} {names[i-1]} -> {dec_map[a]}", flush=True)
        return dec_map

    with ThreadPoolExecutor(max_workers=par) as ex:
        futs = {ex.submit(one, a): idx for idx, a in enumerate(addrs)}
        for done in as_completed(futs):
            idx = futs[done]
            a = addrs[idx]
            try:
                dec_map[a] = int(done.result())
            except Exception:
                dec_map[a] = 18
            if show_progress:
                printed += 1
                print(f"[decimals] {printed}/{total} {names[idx]} -> {dec_map[a]}", flush=True)
    return dec_map

def build_rate_matrix_v3(
    w3: Web3,
    tokens,
    factory_addr: str,
    fee_tiers: List[int],
    pivot_symbol: str,
    debug_pools: bool = False,
    concurrency: int = 24,
    progress: bool = False,
    progress_every: int = 200,
    decimals_parallel: int = 16,
    no_synth: bool = False,
):
    n = len(tokens)
    names = [t["symbol"].upper() for t in tokens]
    addrs = [to_checksum(t["address"]) for t in tokens]
    addr_map = {names[i]: addrs[i] for i in range(n)}

    if progress or debug_pools:
        print(f"[stage] prefetching decimals for {n} tokens (parallel={decimals_parallel})", flush=True)

    dec_map: Dict[str, int] = prefetch_decimals_parallel(
        w3, addrs, names, par=max(1, decimals_parallel), show_progress=(progress or debug_pools)
    )

    pool_cache: Dict[Tuple[str, str, int], str] = {}
    slot0_cache: Dict[str, Optional[int]] = {}

    R = np.full((n, n), np.nan, dtype=float)
    best_fee: Dict[Tuple[int, int], int] = {}
    composed_edges: Dict[Tuple[int, int], List[Tuple[int, int]]] = {}
    for i in range(n):
        R[i, i] = 1.0

    total_pairs = n * (n - 1)
    if progress or debug_pools:
        print(f"[stage] solving {total_pairs} directed pairs across fee tiers {fee_tiers} (concurrency={concurrency})", flush=True)

    pair_tasks = [(i, j) for i in range(n) for j in range(n) if i != j]
    sem = BoundedSemaphore(concurrency)
    factory = w3.eth.contract(address=to_checksum(factory_addr), abi=FACTORY_V3_ABI)

    def get_pool_cached(a: str, b: str, fee: int) -> str:
        key = (a, b, int(fee)) if a.lower() < b.lower() else (b, a, int(fee))
        if key in pool_cache:
            return pool_cache[key]
        t0, t1 = (a, b) if a.lower() < b.lower() else (b, a)
        try:
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
            is_stable = names[i] in STABLES and names[j] in STABLES
            pref = [100, 500, 2500] if is_stable else [2500, 500, 100]
            order = [f for f in pref if f in fee_tiers] + [f for f in fee_tiers if f not in pref]
            for fee in order:
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
                        continue
                    return (i, j, float(r_ab), float(r_ba), int(fee))
                except Exception:
                    continue
            return (i, j, float("nan"), float("nan"), None)

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(solve_pair, i, j) for (i, j) in pair_tasks]
        done_ct = 0; total_ct = len(futures)
        for fut in as_completed(futures):
            i, j, r_ab, r_ba, fee = fut.result()
            done_ct += 1
            if (progress or debug_pools) and (done_ct == 1 or done_ct % max(1, progress_every) == 0 or done_ct == total_ct):
                print(f"[pairs] {done_ct}/{total_ct} solved", flush=True)
            if fee is not None and np.isfinite(r_ab) and np.isfinite(r_ba):
                R[i, j] = r_ab; R[j, i] = r_ba
                best_fee[(i, j)] = best_fee[(j, i)] = fee
            else:
                R[i, j] = float("nan")

    composed_edges: Dict[Tuple[int,int], List[Tuple[int,int]]] = {}
    if not no_synth:
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
                            best_fee[(i, j)] = -1  # synthesized

    return names, addrs, R, addr_map, composed_edges, best_fee, dec_map
