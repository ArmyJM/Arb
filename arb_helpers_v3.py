#!/usr/bin/env python3
"""
arb_helpers_v3.py — modular runner that delegates all logic to pcs_v3.*

What this file does:
- Parses CLI flags
- Sets runtime knobs in pcs_v3.utils (QPS limiter, timeouts, retries)
- Builds the N×N effective rate matrix via pcs_v3.discovery.build_rate_matrix_v3
- Prints the matrix & scans for triangular opportunities (pcs_v3.math)
- Optionally runs size‑aware simulations via QuoterV2 (pcs_v3.quote)
- Optionally estimates gas via SwapRouterV3 for exactInput (pcs_v3.quote)
- (Optional) CSV logging of cycles

All encoders, ABIs, math, and pool discovery have been centralized in pcs_v3.
"""

from __future__ import annotations
import os, sys, json, time, argparse
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from typing import List, Tuple, Dict, Optional

import numpy as np
from web3 import Web3

# Centralized modules
from pcs_v3 import utils as U
from pcs_v3.discovery import build_rate_matrix_v3
from pcs_v3.math import pretty_print_matrix, scan_triangular_opps, estimate_usd_price_for_symbol
from pcs_v3.quote import simulate_path_usd, router_estimate_gas_exact_input, encode_path

getcontext().prec = 50

# -----------------------------
# CLI
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
    ap = argparse.ArgumentParser(
        description="PancakeSwap V3: live N×N matrix + triangular arbitrage scanner (modular pcs_v3.*)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # scanning cadence
    ap.add_argument("--interval", type=str, default="0", help="Polling cadence (e.g. 0, 30s, 1m)")
    ap.add_argument("--loops", type=int, default=1, help="How many scans to run (0 = run forever)")
    ap.add_argument("--top", type=int, default=10, help="Show top K cycles")
    ap.add_argument("--threshold-bps", type=float, default=0.0, help="Minimum cycle edge to report (in bps)")

    # inputs
    ap.add_argument("--tokens", type=str, default="", help="Token list JSON (defaults to stable/majors preset)")
    ap.add_argument("--rpc", type=str, default="", help="BSC RPC URL (e.g. https://bsc.publicnode.com)")

    # PCS v3 contracts
    ap.add_argument("--factory-v3", type=str, required=True, help="Pancake V3: Factory address")
    ap.add_argument("--quoter", type=str, required=True, help="Pancake V3: QuoterV2 address")
    ap.add_argument("--router-v3", type=str, required=True, help="Pancake V3: SwapRouter v3 address")

    # pool settings
    ap.add_argument("--fee-tiers", type=str, default="100,500,2500,10000", help="Comma-separated v3 fee tiers")
    ap.add_argument("--pivot", type=str, default="", help="Symbol to synth edges (e.g. USDT). Empty = none")
    ap.add_argument("--no-synth", action="store_true", help="Disable synthetic edges via pivot composition")

    # notional & CSV
    ap.add_argument("--notional-usd", type=float, default=0.0, help="Size for size-aware Quoter simulation (0=skip)")
    ap.add_argument("--csv", type=str, default="", help="CSV file path to log cycles (optional)")

    # gas & tx sim settings
    ap.add_argument("--estimate-gas", action="store_true", help="Estimate gas via router.exactInput (needs --from-addr)")
    ap.add_argument("--no-estimate-gas", dest="estimate_gas", action="store_false")
    ap.set_defaults(estimate_gas=True)
    ap.add_argument("--from-addr", type=str, default="", help="EOA for gas estimation (no key required)")
    ap.add_argument("--gas-price-gwei", type=float, default=0.0, help="Override gas price (gwei). 0 = fetch from node")
    ap.add_argument("--gas-limit", type=int, default=360000, help="Fallback gas limit for display")
    ap.add_argument("--gas-multiplier", type=float, default=1.2, help="Multiply baseline gas in display")
    ap.add_argument("--slippage-bps", type=float, default=30.0, help="Min-out tolerance used for gas estimate")
    ap.add_argument("--deadline-seconds", type=int, default=600, help="Deadline for router tx builder (seconds)")

    # execution knobs
    ap.add_argument("--concurrency", type=int, default=24, help="Max parallel RPC calls")
    ap.add_argument("--rpc-qps", type=float, default=20.0, help="Throttle eth_call QPS")
    ap.add_argument("--timeout", type=float, default=25.0, help="HTTP timeout (seconds)")
    ap.add_argument("--retries", type=int, default=5, help="Per-call retries with backoff")
    ap.add_argument("--decimals-parallel", type=int, default=16, help="Parallelism for decimals prefetch")

    # debugging / progress
    ap.add_argument("--debug-pools", action="store_true", help="Verbose pool discovery")
    ap.add_argument("--debug-sim", action="store_true", help="Verbose quoter/router simulation")
    ap.add_argument("--progress", action="store_true", help="Show progress (decimals/pairs)")
    ap.add_argument("--progress-every", type=int, default=200, help="Progress tick")

    return ap.parse_args()

# -----------------------------
# helpers
# -----------------------------
def _fmt_gwei(v: int) -> str:
    return f"{v/1e9:.9f}"

def _bnb_price_from_matrix(names: List[str], R: np.ndarray) -> Optional[float]:
    try:
        i = names.index("WBNB"); j = names.index("USDT")
        val = R[i, j]
        return float(val) if np.isfinite(val) and val > 0 else None
    except Exception:
        return None

def _print_gas_baseline(w3: Web3, names: List[str], R: np.ndarray, gas_limit: int, gas_price_gwei: float):
    # Pick gas price: explicit flag, else node suggestion
    gwei = gas_price_gwei if gas_price_gwei > 0 else (w3.eth.gas_price / 1e9)
    gas_bnb = gas_limit * (gwei / 1e9)
    bnb_usd = _bnb_price_from_matrix(names, R) or 0.0
    usd = gas_bnb * bnb_usd if bnb_usd > 0 else 0.0
    print(f"\nGas (baseline): ~{gas_limit:,} @ {gwei:.2f} gwei ≈ {gas_bnb:.6f} BNB (${usd:.2f})")

def _print_cycles(names: List[str], R: np.ndarray, top: int, threshold_bps: float):
    cycles = scan_triangular_opps(names, R, threshold_bps=threshold_bps)
    n = min(top, len(cycles))
    if n == 0:
        print("\nProfitable triangular cycles found: 0")
        return cycles

    print(f"\nProfitable triangular cycles found: {len(cycles)} (showing top {n})")
    for idx, c in enumerate(cycles[:n], 1):
        a,b,c3,_ = c["route"]
        f1,f2,f3 = c["factors"]
        print(f"  {idx}. {a} -> {b} -> {c3} -> {a} | factors: [{f1:.6f}, {f2:.6f}, {f3:.6f}] | product: {c['product']:.6f} | profit: {c['profit_pct']:.4f}%")
    return cycles

def _run_size_aware(
    w3: Web3,
    names: List[str],
    addrs: List[str],
    R: np.ndarray,
    addr_map: Dict[str,str],
    dec_map: Dict[str,int],
    quoter: str,
    router: str,
    from_addr: str,
    notional_usd: float,
    estimate_gas: bool,
    slippage_bps: float,
    deadline_seconds: int,
    cycles: List[Dict],
    allowed_fee_tiers: list[int],
    debug: bool = False,
):
    if notional_usd <= 0:
        return
    # Per-token USD price map using stables cross rates
    usd_price_map: Dict[str, float] = {}
    for sym in names:
        p = estimate_usd_price_for_symbol(names, addrs, R, sym)
        if p: usd_price_map[sym] = p

    print("")  # spacer
    for idx, c in enumerate(cycles, 1):
        route = list(c["route"])
        path_syms = route[:-1]  # drop final repeat
        # choose nominal fee for each hop from best known edges (we don't track exact fee per edge here)
        # For size-aware, use a conservative fee guess: 2500 unless ETH/WBNB/USDT/USDC pair then 500.
        fees: List[int] = []
        for i in range(len(path_syms)-1):
            x, y = path_syms[i], path_syms[i+1]
            if {x,y} & {"USDT","USDC"} and {"USDT","USDC"} - {x,y}:
                fees.append(100)  # stable pair at 0.01 when available
            elif x in {"WBNB","ETH"} or y in {"WBNB","ETH"}:
                fees.append(500)
            else:
                fees.append(2500)

        final_usd, outWei, inWei = simulate_path_usd(
            w3, quoter, path_syms, fees, addr_map, dec_map, usd_price_map, notional_usd, debug=debug
        )
        tag = f"size-aware[{path_syms[0]}->{path_syms[1]}->{path_syms[2]}->{path_syms[0]}]"
        if final_usd is None:
            print(f"     {tag}: quoter_failed({path_syms[0]}->{path_syms[1]}@{fees[0]})")
            continue
        pnl = final_usd - float(notional_usd)
        print(f"     {tag}: start ${notional_usd:,.2f} → end ${final_usd:,.2f} (PnL ${pnl:,.2f})")

        if estimate_gas and from_addr:
            # simplified minOut at 50 bps slippage on quoted out amount
            min_out = int(outWei * (1.0 - (slippage_bps/10_000.0)))
            gas_est = router_estimate_gas_exact_input(
                w3, router, [addr_map[s] for s in path_syms], fees, inWei, min_out, from_addr, deadline_seconds, from_addr
            )
            gas_txt = f"{gas_est:,}" if gas_est else "n/a (estimate failed)"
            print(f"         router_gas: {gas_txt}")

# -----------------------------
# main
# -----------------------------
def main():
    args = parse_args()

    # Configure pcs_v3 runtime knobs
    U.CALL_RETRIES = max(1, int(args.retries))
    U.HTTP_TIMEOUT = float(args.timeout)
    U.limiter = U.QPSLimiter(float(args.rpc_qps))

    # Web3
    rpc_url = args.rpc or os.environ.get("BSC_RPC_URL") or ""
    if not rpc_url:
        print("Web3: no RPC URL provided. Use --rpc or set BSC_RPC_URL", file=sys.stderr)
        sys.exit(2)
    w3 = U.make_web3(rpc_url)
    if not w3.is_connected():
        print("Web3: not connected. Check RPC URL or your network.", file=sys.stderr)
        sys.exit(2)

    print(f"[stage] connected to RPC; qps={float(args.rpc_qps):.1f}, timeout={float(args.timeout):.1f}s, retries={int(args.retries)}")

    # Tokens & fees
    tokens = U.load_tokens(args.tokens)
    fee_tiers = [int(x) for x in (args.fee_tiers or "").replace(" ","").split(",") if x]
    if not fee_tiers:
        fee_tiers = [500, 2500]
    print(f"[stage] loaded {len(tokens)} tokens; fees={fee_tiers}; concurrency={int(args.concurrency)}")

    loops = int(args.loops)
    interval_sec = parse_interval_to_seconds(args.interval)

    scan_idx = 0
    while True:
        scan_idx += 1

        # Build R using modular discovery
        names, addrs, R, addr_map, composed_edges, best_fee, dec_map = build_rate_matrix_v3(
            w3=w3,
            tokens=tokens,
            factory_addr=args.factory_v3,
            fee_tiers=fee_tiers,
            pivot_symbol=(args.pivot or "").upper(),
            debug_pools=bool(args.debug_pools),
            concurrency=int(args.concurrency),
            progress=bool(args.progress),
            progress_every=int(args.progress_every),
            decimals_parallel=int(args.decimals_parallel),
            no_synth=bool(args.no_synth),
        )

        # Print matrix & gas baseline
        pretty_print_matrix(names, R)
        _print_gas_baseline(w3, names, R, int(args.gas_limit), float(args.gas_price_gwei or 0.1))

        # Cycles
        cycles = _print_cycles(names, R, top=int(args.top), threshold_bps=float(args.threshold_bps))

        # Size-aware sim (if requested)
        if args.notional_usd > 0 and len(cycles) > 0:
            _run_size_aware(
                w3, names, addrs, R, addr_map, dec_map,
                args.quoter, args.router_v3, args.from_addr,
                float(args.notional_usd), bool(args.estimate_gas),
                float(args.slippage_bps), int(args.deadline_seconds),
                cycles[: int(args.top)],
                allowed_fee_tiers=fee_tiers,
                debug=bool(args.debug_sim),
            )

        # CSV logging (optional)
        if args.csv:
            try:
                import csv
                hdr = ["timestamp","route","factors","product","profit_pct"]
                write_header = not os.path.exists(args.csv)
                with open(args.csv, "a", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    if write_header: w.writerow(hdr)
                    ts = int(time.time())
                    for c in cycles[: int(args.top)]:
                        w.writerow([ts, "->".join(c["route"]), "|".join(f"{x:.6f}" for x in c["factors"]), f"{c['product']:.6f}", f"{c['profit_pct']:.6f}"])
            except Exception as e:
                print(f"[warn] failed to write CSV: {e}")

        # Loop control
        if loops == 0 or scan_idx < loops:
            if interval_sec > 0:
                time.sleep(interval_sec)
            else:
                # single run: break if loops==1 or continue immediately for loops>1 and interval==0
                if loops == 1:
                    break
        else:
            break

if __name__ == "__main__":
    main()
