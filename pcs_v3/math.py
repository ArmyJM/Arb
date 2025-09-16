
from __future__ import annotations
from decimal import Decimal
from typing import Optional, List
import numpy as np

def price_from_sqrtPriceX96(sqrtPriceX96: int, dec0: int, dec1: int) -> float:
    num = Decimal(sqrtPriceX96) * Decimal(sqrtPriceX96)
    denom = Decimal(2) ** 192
    ratio = num / denom
    scale = Decimal(10) ** Decimal(dec0 - dec1)
    return float(ratio * scale)

def pretty_print_matrix(names: List[str], R: np.ndarray):
    width = max(6, max(len(n) for n in names))
    print("\nEffective spot matrix R (PCS V3, fee applied or synth via pivot):")
    header = " " * (width + 1) + "  ".join(f"{n:>{width}}" for n in names)
    print(header)
    for i, row in enumerate(R):
        def cell(x):
            return f"{x:>{width}.6f}" if np.isfinite(x) else f"{'NaN':>{width}}"
        print(f"{names[i]:>{width}} " + "  ".join(cell(x) for x in row))

def scan_triangular_opps(names: List[str], R: np.ndarray, threshold_bps: float):
    from itertools import permutations
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
                "profit_pct": prof * 100.0
            })
    # dedupe by unordered triple; keep best product
    ded = {}
    for r in res:
        key = tuple(sorted(r["route"][:3]))
        if key not in ded or r["product"] > ded[key]["product"]:
            ded[key] = r
    return sorted(ded.values(), key=lambda x: x["product"], reverse=True)

def estimate_usd_price_for_symbol(names: List[str], addrs: List[str], R: np.ndarray, sym: str) -> Optional[float]:
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
