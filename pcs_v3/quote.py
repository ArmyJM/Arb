from __future__ import annotations
from typing import List, Optional
from decimal import Decimal
from web3 import Web3
from .utils import limited_call

# QuoterV2 minimal ABI
QUOTER_V2_ABI = [
  {
    "inputs":[
      {
        "components":[
          {"internalType":"address","name":"tokenIn","type":"address"},
          {"internalType":"address","name":"tokenOut","type":"address"},
          {"internalType":"uint24","name":"fee","type":"uint24"},
          {"internalType":"uint256","name":"amountIn","type":"uint256"},
          {"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}
        ],
        "internalType":"struct IQuoterV2.QuoteExactInputSingleParams",
        "name":"params",
        "type":"tuple"
      }
    ],
    "name":"quoteExactInputSingle",
    "outputs":[
      {"internalType":"uint256","name":"amountOut","type":"uint256"},
      {"internalType":"uint160","name":"sqrtPriceX96After","type":"uint160"},
      {"internalType":"uint32","name":"initializedTicksCrossed","type":"uint32"},
      {"internalType":"uint256","name":"gasEstimate","type":"uint256"}
    ],
    "stateMutability":"nonpayable","type":"function"
  },
  {
    "inputs":[
      {"internalType":"bytes","name":"path","type":"bytes"},
      {"internalType":"uint256","name":"amountIn","type":"uint256"}
    ],
    "name":"quoteExactInput",
    "outputs":[
      {"internalType":"uint256","name":"amountOut","type":"uint256"},
      {"internalType":"uint160[]","name":"sqrtPriceX96AfterList","type":"uint160[]"},
      {"internalType":"uint32[]","name":"initializedTicksCrossedList","type":"uint32[]"},
      {"internalType":"uint256","name":"gasEstimate","type":"uint256"}
    ],
    "stateMutability":"nonpayable","type":"function"
  }
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

def encode_path(addresses: List[str], fees: List[int]) -> bytes:
    assert len(addresses) >= 2 and len(fees) == len(addresses) - 1
    b = b""
    for i in range(len(fees)):
        b += bytes.fromhex(addresses[i][2:])
        b += int(fees[i]).to_bytes(3, "big")
    b += bytes.fromhex(addresses[-1][2:])
    return b

def quoter_amount_out_single(w3, quoter_addr, token_in, token_out, fee, amount_in_wei) -> Optional[int]:
    q = w3.eth.contract(address=Web3.to_checksum_address(quoter_addr), abi=QUOTER_V2_ABI)
    params = (Web3.to_checksum_address(token_in), Web3.to_checksum_address(token_out), int(fee), int(amount_in_wei), 0)
    try:
        amount_out, _, _, _ = limited_call(q.functions.quoteExactInputSingle(params).call)
        return int(amount_out)
    except Exception:
        return None

def quoter_amount_out_path(w3, quoter_addr, path_addrs: List[str], fees: List[int], amount_in_wei) -> Optional[int]:
    q = w3.eth.contract(address=Web3.to_checksum_address(quoter_addr), abi=QUOTER_V2_ABI)
    path_bytes = encode_path([Web3.to_checksum_address(a) for a in path_addrs], [int(f) for f in fees])
    try:
        amount_out, _, _, _ = limited_call(q.functions.quoteExactInput(path_bytes, int(amount_in_wei)).call)
        return int(amount_out)
    except Exception:
        return None

def router_estimate_gas_exact_input(
    w3, router_addr, path_addrs, path_fees, amount_in, amount_out_min, recipient, deadline_sec, from_addr
) -> Optional[int]:
    router = w3.eth.contract(address=Web3.to_checksum_address(router_addr), abi=ROUTER_V3_ABI)
    path_bytes = encode_path([Web3.to_checksum_address(a) for a in path_addrs], path_fees)
    params = (path_bytes, Web3.to_checksum_address(recipient), int(deadline_sec), int(amount_in), int(amount_out_min))
    try:
        return router.functions.exactInput(params).estimate_gas({"from": Web3.to_checksum_address(from_addr), "value": 0})
    except Exception:
        return None

def simulate_path_usd(
    w3,
    quoter_addr: str,
    path_syms: list[str],
    path_fees: list[int],
    addr_map: dict[str, str],
    dec_map: dict[str, int],
    usd_price_map: dict[str, float],
    notional_usd: float,
    debug: bool = False,
):
    """Return (final_usd, outWei, inWei) using END-token decimals & price."""
    A = path_syms[0]; Z = path_syms[-1]
    pA = usd_price_map.get(A); pZ = usd_price_map.get(Z, pA)
    if not pA or not pZ or pA <= 0 or pZ <= 0:
        return None, None, None

    decA = dec_map[addr_map[A]]
    amt_in_wei_start = int(Decimal(notional_usd) / Decimal(pA) * (Decimal(10) ** decA))

    if len(path_syms) > 2:
        outWei = quoter_amount_out_path(w3, quoter_addr, [addr_map[s] for s in path_syms], path_fees, amt_in_wei_start)
    else:
        outWei = quoter_amount_out_single(w3, quoter_addr, addr_map[path_syms[0]], addr_map[path_syms[1]], int(path_fees[0]), amt_in_wei_start)
    if not outWei or outWei <= 0:
        return None, None, amt_in_wei_start

    decZ = dec_map[addr_map[Z]]
    final_amt_z = Decimal(outWei) / (Decimal(10) ** decZ)
    final_usd = float(final_amt_z * Decimal(pZ))

    if debug:
        print(f"[simulate_path_usd] path={path_syms} fees={path_fees} inWei={amt_in_wei_start} outWei={outWei} A={A} Z={Z} decA={decA} decZ={decZ} pA={pA} pZ={pZ} final_usd={final_usd:.6f}")
    return final_usd, outWei, amt_in_wei_start
