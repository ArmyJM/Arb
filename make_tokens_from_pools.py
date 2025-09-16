#!/usr/bin/env python3
# make_tokens_from_pools.py
# Build a deduped tokens.json (symbol, address, decimals) from a PancakeSwap v3 pools dump (BSC)
# Adds --min-tvl-usd filter (and optional --fee-tiers) to avoid illiquid tokens.

import argparse
import concurrent.futures as cf
import json
import os
import sys
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Tuple, Optional

SPINNER_FRAMES = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]

def spinner(stop_event, label="Processing"):
    i = 0
    while not stop_event.is_set():
        sys.stdout.write(f"\r{SPINNER_FRAMES[i % len(SPINNER_FRAMES)]} {label}...")
        sys.stdout.flush()
        time.sleep(0.08)
        i += 1
    sys.stdout.write("\r✓ Done.            \n")
    sys.stdout.flush()

def normalize_symbol(sym: str) -> str:
    if not sym:
        return ""
    return sym.strip()

def parse_fee_tiers(s: Optional[str]) -> Optional[set]:
    if not s:
        return None
    tiers = set()
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            tiers.add(int(part))
        except:
            pass
    return tiers or None

def extract_tvl(pool: Dict[str, Any]) -> float:
    # Support multiple possible subgraph field names
    for key in ("liquidityUSD", "totalValueLockedUSD", "tvlUSD", "trackedLiquidityUSD"):
        v = pool.get(key)
        if v is None:
            continue
        try:
            return float(v)
        except:
            continue
    # Messari-style nested metrics (rare)
    metrics = pool.get("metrics") or {}
    for key in ("tvlUSD", "liquidityUSD", "totalValueLockedUSD"):
        v = metrics.get(key)
        if v is not None:
            try:
                return float(v)
            except:
                pass
    return 0.0

def extract_fee_tier(pool: Dict[str, Any]) -> Optional[int]:
    # Common keys: feeTier (bps), fee, feeTierBps
    for key in ("feeTier", "fee", "feeTierBps"):
        v = pool.get(key)
        if v is None:
            continue
        try:
            return int(v)
        except:
            continue
    return None

def extract_from_pool(pool: Dict[str, Any],
                      min_tvl_usd: float,
                      allowed_fee_tiers: Optional[set]) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Returns a list of two tuples for token0 and token1:
    (address_lower, {symbol, address_checksum_or_as_is, decimals})
    Only if pool passes TVL and optional fee-tier filters.
    """
    tvl = extract_tvl(pool)
    if tvl < min_tvl_usd:
        return []

    if allowed_fee_tiers is not None:
        ft = extract_fee_tier(pool)
        if ft is None or ft not in allowed_fee_tiers:
            return []

    out: List[Tuple[str, Dict[str, Any]]] = []
    for side in ("token0", "token1"):
        tok = pool.get(side) or {}
        addr = tok.get("id") or tok.get("address")
        sym  = tok.get("symbol")
        dec  = tok.get("decimals")

        if not addr or sym is None or dec is None:
            continue

        try:
            dec = int(dec)
        except Exception:
            continue

        addr_key = str(addr).lower()
        out.append( (addr_key, {
            "symbol": normalize_symbol(str(sym)),
            "address": str(addr),
            "decimals": dec
        }) )
    return out

def main():
    ap = argparse.ArgumentParser(description="Generate a deduped tokens.json from PancakeSwap v3 pools (BSC) with min TVL filter.")
    ap.add_argument("--pools", default="pools_pcs_v3_bsc.json",
                    help="Path to pools JSON (from dump_pcs_v3_pools.py).")
    ap.add_argument("--out", default="tokens_from_pools.json",
                    help="Output tokens JSON (will overwrite).")
    ap.add_argument("--workers", type=int, default=8,
                    help="Number of threads to use.")
    ap.add_argument("--sort", choices=["symbol","address"], default="symbol",
                    help="Sort final list by this field.")
    ap.add_argument("--limit", type=int, default=0,
                    help="If >0, truncate output to first N tokens after sorting.")
    ap.add_argument("--min-tvl-usd", type=float, default=0.0,
                    help="Only include tokens that appear in pools with TVL >= this USD amount.")
    ap.add_argument("--fee-tiers", type=str, default="",
                    help="Optional comma-separated list of allowed fee tiers in bps (e.g. 500,2500,10000).")
    args = ap.parse_args()

    if not os.path.isfile(args.pools):
        print(f"[fatal] pools file not found: {args.pools}")
        sys.exit(1)

    try:
        with open(args.pools, "r", encoding="utf-8") as f:
            pools = json.load(f)
        if not isinstance(pools, list):
            raise ValueError("Pools root must be a JSON array.")
    except Exception as e:
        print(f"[fatal] failed to read pools JSON: {e}")
        sys.exit(1)

    allowed_fee_tiers = parse_fee_tiers(args.fee_tiers)

    stop = threading.Event()
    t = threading.Thread(target=spinner, args=(stop,"Parsing & filtering pools"), daemon=True)
    t.start()

    pairs: List[Tuple[str, Dict[str, Any]]] = []
    try:
        extractor = lambda p: extract_from_pool(p, args.min_tvl_usd, allowed_fee_tiers)
        with cf.ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
            for res in ex.map(extractor, pools, chunksize=512):
                if res:
                    pairs.extend(res)
    finally:
        stop.set()
        t.join()

    # Deduplicate by address
    dedup: OrderedDict[str, Dict[str, Any]] = OrderedDict()
    for addr_key, tok in pairs:
        if addr_key in dedup:
            if not dedup[addr_key].get("symbol") and tok.get("symbol"):
                dedup[addr_key]["symbol"] = tok["symbol"]
            if dedup[addr_key].get("decimals", 0) != tok.get("decimals", 0):
                if tok.get("decimals") == 18:
                    dedup[addr_key]["decimals"] = 18
            continue
        dedup[addr_key] = tok

    tokens = list(dedup.values())

    key_fn = (lambda x: (x.get("symbol",""), x.get("address","").lower())) if args.sort == "symbol" \
             else (lambda x: x.get("address","").lower())
    tokens.sort(key=key_fn)
    if args.limit and args.limit > 0:
        tokens = tokens[:args.limit]

    # Overwrite output
    try:
        if os.path.exists(args.out):
            try:
                os.chmod(args.out, 0o666)
            except Exception:
                pass
            try:
                os.remove(args.out)
            except Exception:
                pass
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(tokens, f, indent=2, ensure_ascii=False)
        print(f"[i] Wrote tokens: {args.out} ({len(tokens)} unique tokens)")
    except Exception as e:
        print(f"[fatal] failed to write tokens JSON: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
