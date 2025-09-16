
from __future__ import annotations
import os, time, random
from threading import Lock
from decimal import Decimal, getcontext
from typing import Optional, Dict, List
from web3 import Web3

getcontext().prec = 50

# Runtime knobs (tuned by caller)
CALL_RETRIES: int = 5
HTTP_TIMEOUT: float = 25.0
limiter: Optional["QPSLimiter"] = None

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

def limited_call(fn, *args, retries=None, base_delay=0.35, **kwargs):
    r = CALL_RETRIES if retries is None else int(retries)
    for attempt in range(1, r+1):
        if limiter:
            limiter.wait()
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt == r:
                raise
            msg = str(e)
            delay = base_delay * (2 ** (attempt-1) + random.uniform(0, 0.2))
            if "-32005" in msg or "limit exceeded" in msg:
                delay += 0.5
            time.sleep(delay)

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

def make_web3(rpc_url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": HTTP_TIMEOUT}))
    _inject_poa_middleware(w3)
    try:
        ok = w3.is_connected()
    except Exception:
        ok = False
    if not ok:
        try:
            w3.provider.make_request("web3_clientVersion", [])
        except Exception as e:
            print("Provider request failed:", repr(e))
        raise SystemExit("Web3: not connected. Check RPC URL or your network.")
    return w3

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

def to_checksum(a: str) -> str:
    return Web3.to_checksum_address(a)

ERC20_ABI = [
    {"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],
     "stateMutability":"view","type":"function"},
    {"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],
     "stateMutability":"view","type":"function"},
]

def erc20_decimals(w3: Web3, addr: str) -> int:
    c = w3.eth.contract(address=to_checksum(addr), abi=ERC20_ABI)
    try:
        return int(limited_call(c.functions.decimals().call))
    except Exception:
        return 18

DEFAULT_TOKENS_9 = [
    {"symbol":"WBNB","address":"0xBB4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c","decimals":18},
    {"symbol":"USDT","address":"0x55d398326f99059fF775485246999027B3197955","decimals":18},
    {"symbol":"BUSD","address":"0xe9e7CEA3Dedca5984780Bafc599bD69ADd087D56","decimals":18},
    {"symbol":"USDC","address":"0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d","decimals":18},
    {"symbol":"DAI", "address":"0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3","decimals":18},
    {"symbol":"ETH", "address":"0x2170Ed0880ac9A755fd29B2688956BD959F933F8","decimals":18},
    {"symbol":"BTCB","address":"0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c","decimals":18},
    {"symbol":"CAKE","address":"0x0E09Fabb73Bd3Ade0a17ECC321fD13a19e81cE82","decimals":18},
    {"symbol":"FDUSD","address":"0x7aB622a8F4eD8C56bf5C67aA4eF8aCf82D54eF2e","decimals":18},
]
STABLES = {"USDT","USDC","BUSD","DAI","FDUSD"}

def load_tokens(path: str):
    if not path:
        return DEFAULT_TOKENS_9
    import json
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list) or not data:
        raise ValueError("Token JSON must be a non-empty list")
    for t in data:
        t.setdefault("decimals", 18)
    return data
