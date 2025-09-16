#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dump_pcs_v3_pools.py
----------------------------------
Fetch all PancakeSwap V3 pools on BSC via The Graph with:
- Hardcoded Graph Gateway + API key (can override with --subgraph)
- Auto-detect schema (official "pools" vs Messari "dexAmmPools")
- Efficient pagination (first=1000) + retries & backoff
- JSON + CSV export
- CLI spinner while fetching

Usage examples:
  py -3.13 dump_pcs_v3_pools.py --out pools.json --csv pools.csv
  py -3.13 dump_pcs_v3_pools.py --subgraph https://api.thegraph.com/subgraphs/id/Hv1GncLY5docZoGtXjo4kwbTvxm3MAhVZqBZE4sUT9eZ --out pools.json
"""

import sys
import json
import csv
import time
import argparse
import itertools
import threading
import urllib.request
import urllib.error

# ====== DEFAULTS (user requested hard-coding for convenience) ======
# Official PancakeSwap V3 (BSC) subgraph deployment id:
SUBGRAPH_ID = "Hv1GncLY5docZoGtXjo4kwbTvxm3MAhVZqBZE4sUT9eZ"

# Hardcoded Graph Gateway API key (from the user):
GATEWAY_API_KEY = "7f5f23253f4ffba27995fcef4ddfe6c8"

# Preferred default endpoint (gateway). You can override with --subgraph.
DEFAULT_SUBGRAPH = f"https://gateway.thegraph.com/api/{GATEWAY_API_KEY}/subgraphs/id/{SUBGRAPH_ID}"

# Public fallback (works without key; might rate-limit, but good backup)
PUBLIC_FALLBACK = f"https://api.thegraph.com/subgraphs/id/{SUBGRAPH_ID}"

USER_AGENT = "pcs-v3-pool-dumper/1.0 (+https://example.local)"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": USER_AGENT
}

SPINNER_FRAMES = "|/-\\"


def spinner(prefix: str, stop_event: threading.Event):
    cyc = itertools.cycle(SPINNER_FRAMES)
    while not stop_event.is_set():
        frame = next(cyc)
        sys.stdout.write(f"\r{prefix} {frame}")
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write("\r" + " " * (len(prefix) + 2) + "\r")  # clear line


def http_post_json(url: str, body: dict, timeout: float = 60.0, retries: int = 4):
    data = json.dumps(body).encode("utf-8")
    backoff = 0.5
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, data=data, headers=HEADERS, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
            try:
                out = json.loads(raw.decode("utf-8"))
            except Exception as je:
                raise RuntimeError(f"Non-JSON response (len={len(raw)}): {raw[:200]!r}") from je
            if "errors" in out and out["errors"]:
                raise RuntimeError(f"GraphQL errors from {url}: {out['errors']}")
            return out
        except Exception as e:
            if attempt == retries:
                raise
            time.sleep(backoff)
            backoff *= 2


def try_query_schema(subgraph_url: str):
    """
    Try to detect schema shape.
    Returns one of: 'official', 'messari'
    Raises if neither works.
    """
    probe_pools = {
        "query": """
        query { 
          pools(first: 1) { id } 
        }
        """
    }
    try:
        res = http_post_json(subgraph_url, probe_pools, timeout=40, retries=3)
        return "official"
    except Exception as e1:
        msg = str(e1)
        probe_messari = {
            "query": """
            query {
              dexAmmPools(first: 1) { id }
            }
            """
        }
        try:
            res2 = http_post_json(subgraph_url, probe_messari, timeout=40, retries=3)
            return "messari"
        except Exception as e2:
            raise RuntimeError(f"Schema detection failed on {subgraph_url}:\n- pools error: {msg}\n- dexAmmPools error: {e2}")


def fetch_all_pools(subgraph_url: str, page_size: int = 1000):
    schema = try_query_schema(subgraph_url)
    print(f"[i] Detected schema: {schema}")

    if schema == "official":
        query_tpl = """
        query ($first:Int!, $skip:Int!) {
          pools(first:$first, skip:$skip, orderBy: totalValueLockedUSD, orderDirection: desc) {
            id
            feeTier
            liquidity
            sqrtPrice
            tick
            totalValueLockedUSD
            token0 { id symbol decimals }
            token1 { id symbol decimals }
          }
        }
        """
        field = "pools"
        map_fn = lambda p: {
            "id": p["id"],
            "feeTier": int(p.get("feeTier") or 0),
            "liquidity": int(p.get("liquidity") or 0),
            "sqrtPrice": p.get("sqrtPrice"),
            "tick": int(p.get("tick") or 0) if p.get("tick") is not None else None,
            "tvlUSD": float(p.get("totalValueLockedUSD") or 0.0),
            "token0": {
                "id": p["token0"]["id"],
                "symbol": p["token0"].get("symbol"),
                "decimals": int(p["token0"].get("decimals") or 18),
            },
            "token1": {
                "id": p["token1"]["id"],
                "symbol": p["token1"].get("symbol"),
                "decimals": int(p["token1"].get("decimals") or 18),
            },
        }
    else:
        query_tpl = """
        query ($first:Int!, $skip:Int!) {
          dexAmmPools(first:$first, skip:$skip) {
            id
            feeTier
            liquidity
            sqrtPrice
            tick
            totalValueLockedUSD
            inputTokens { id symbol decimals }
            outputTokens { id symbol decimals }
          }
        }
        """
        field = "dexAmmPools"
        def map_fn(p):
            t0 = p.get("inputTokens", [{}])[0] if p.get("inputTokens") else {}
            t1 = p.get("outputTokens", [{}])[0] if p.get("outputTokens") else {}
            return {
                "id": p["id"],
                "feeTier": int(p.get("feeTier") or 0),
                "liquidity": int(p.get("liquidity") or 0),
                "sqrtPrice": p.get("sqrtPrice"),
                "tick": int(p.get("tick") or 0) if p.get("tick") is not None else None,
                "tvlUSD": float(p.get("totalValueLockedUSD") or 0.0),
                "token0": {
                    "id": t0.get("id"),
                    "symbol": t0.get("symbol"),
                    "decimals": int(t0.get("decimals") or 18) if t0.get("decimals") is not None else 18,
                },
                "token1": {
                    "id": t1.get("id"),
                    "symbol": t1.get("symbol"),
                    "decimals": int(t1.get("decimals") or 18) if t1.get("decimals") is not None else 18,
                },
            }

    pools = []
    skip = 0
    stop_event = threading.Event()
    spin_thread = threading.Thread(target=spinner, args=("Fetching pools...", stop_event), daemon=True)
    spin_thread.start()

    try:
        while True:
            body = {"query": query_tpl, "variables": {"first": page_size, "skip": skip}}
            try:
                out = http_post_json(subgraph_url, body, timeout=60, retries=4)
            except Exception as e:
                if subgraph_url == DEFAULT_SUBGRAPH:
                    print("\n[warn] Gateway failed; trying public API fallback once...")
                    out = http_post_json(PUBLIC_FALLBACK, body, timeout=60, retries=4)
                else:
                    raise

            page = out.get("data", {}).get(field, [])
            if not page:
                break
            mapped = [map_fn(p) for p in page]
            pools.extend(mapped)
            skip += page_size
            sys.stdout.write(f"\rFetched {len(pools)} pools... "); sys.stdout.flush()
    finally:
        stop_event.set()
        spin_thread.join()

    print(f"[i] Total pools fetched: {len(pools)}")
    return pools


def write_json(path: str, pools):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(pools, f, indent=2)
    print(f"[i] Wrote JSON: {path} ({len(pools)} pools)")


def write_csv(path: str, pools):
    cols = ["id", "feeTier", "liquidity", "sqrtPrice", "tick", "tvlUSD",
            "token0_id", "token0_symbol", "token0_decimals",
            "token1_id", "token1_symbol", "token1_decimals"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for p in pools:
            w.writerow([
                p.get("id"),
                p.get("feeTier"),
                p.get("liquidity"),
                p.get("sqrtPrice"),
                p.get("tick"),
                p.get("tvlUSD"),
                p.get("token0", {}).get("id"),
                p.get("token0", {}).get("symbol"),
                p.get("token0", {}).get("decimals"),
                p.get("token1", {}).get("id"),
                p.get("token1", {}).get("symbol"),
                p.get("token1", {}).get("decimals"),
            ])
    print(f"[i] Wrote CSV: {path} ({len(pools)} rows)")


def parse_args():
    ap = argparse.ArgumentParser(description="Dump PancakeSwap V3 pools on BSC via The Graph")
    ap.add_argument("--subgraph", default=DEFAULT_SUBGRAPH,
                    help=f"GraphQL endpoint (default: gateway for PCS v3 BSC). Fallback: {PUBLIC_FALLBACK}")
    ap.add_argument("--out", default="pools_pcs_v3_bsc.json", help="Output JSON file")
    ap.add_argument("--csv", default=None, help="Optional CSV output path")
    ap.add_argument("--page-size", type=int, default=1000, help="Pagination size (default 1000)")
    return ap.parse_args()


def main():
    args = parse_args()
    subgraph = args.subgraph

    print(f"[i] Querying subgraph: {subgraph}")
    try:
        pools = fetch_all_pools(subgraph, page_size=args.page_size)
    except Exception as e:
        print(f"[fatal] subgraph fetch failed: {e}")
        sys.exit(1)

    write_json(args.out, pools)
    if args.csv:
        write_csv(args.csv, pools)


if __name__ == "__main__":
    main()
