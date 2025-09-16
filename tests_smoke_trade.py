r"""
Smoke test for pcs_v3.trade utilities.
- No real RPC calls
- No broadcasting
- Uses a stubbed Web3-like object

Run (PowerShell):
  py -3 .\tests_smoke_trade.py
"""
from types import SimpleNamespace
from typing import Any, Dict
import time

# Import the module under test
from pcs_v3.trade import ensure_allowance, build_exact_input_tx, send_signed_tx

# --------- Valid dummy hex addresses (20 bytes) ---------
ADDR_TOKEN   = "0x1111111111111111111111111111111111111111"
ADDR_OWNER   = "0x2222222222222222222222222222222222222222"
ADDR_ROUTER  = "0x3333333333333333333333333333333333333333"

# ---------- Stubs ----------

class StubFunctions:
    def __init__(self, parent, allowance_value=0):
        self.parent = parent
        self._allowance_value = int(allowance_value)
        self._approve_spender = None
        self._approve_value = None
        self._approve_txns = []

    # ERC-20 interface
    def allowance(self, owner, spender):
        def call():
            return self._allowance_value
        return SimpleNamespace(call=call)

    def approve(self, spender, value):
        self._approve_spender = spender
        self._approve_value = int(value)
        def build_transaction(params: Dict[str, Any]):
            tx = {
                "to": self.parent.addr,
                "data": "approve(spender,value)",
                **params,
            }
            self._approve_txns.append(tx)
            return tx
        return SimpleNamespace(build_transaction=build_transaction)

    # Router interface
    def exactInput(self, params_tuple):
        def build_transaction(tx_params: Dict[str, Any]):
            tx = {"to": self.parent.addr, "data": "exactInput(params)", **tx_params}
            return tx
        return SimpleNamespace(build_transaction=build_transaction)


class StubContract:
    def __init__(self, addr, allowance_value=0):
        self.addr = addr
        self.functions = StubFunctions(self, allowance_value=allowance_value)


class StubAccount:
    def __init__(self):
        self._last_signed = None

    def sign_transaction(self, tx, pk_hex):
        raw = b"RAW_TX_" + bytes(str(hash(str(tx)) % (10**8)), "utf-8")
        self._last_signed = raw
        return SimpleNamespace(rawTransaction=raw)


class StubEth:
    def __init__(self, allowance_value=0):
        self._nonce_map = {}
        self._sent_raw = []
        self._receipts = {}
        self._gas_price = 2_000_000_000  # 2 gwei
        self._chain_id = 56  # BSC mainnet
        self.allowance_value = allowance_value
        self.account = StubAccount()

    # contract constructor for both ERC20 and Router ABIs
    def contract(self, address, abi):
        return StubContract(address, allowance_value=self.allowance_value)

    @property
    def gas_price(self):
        return self._gas_price

    @property
    def chain_id(self):
        return self._chain_id

    def get_transaction_count(self, addr):
        n = self._nonce_map.get(addr, 0)
        self._nonce_map[addr] = n + 1
        return n

    def send_raw_transaction(self, raw):
        h = ("0x" + hex(abs(hash(raw)) % (1<<256))[2:]).ljust(66, "0")
        self._sent_raw.append(raw)
        self._receipts[h] = SimpleNamespace(status=1, transactionHash=h)
        return bytes.fromhex(h[2:])

    def wait_for_transaction_receipt(self, txh):
        if isinstance(txh, (bytes, bytearray)):
            key = "0x" + txh.hex()
        else:
            key = txh
        time.sleep(0.01)
        return self._receipts.get(key, SimpleNamespace(status=1, transactionHash=key))


class StubWeb3:
    def __init__(self, allowance_value=0):
        self.eth = StubEth(allowance_value=allowance_value)

    @staticmethod
    def to_wei(x, unit):
        if unit == "gwei":
            return int(float(x) * 1e9)
        if unit == "ether":
            return int(float(x) * 1e18)
        return int(x)

    @staticmethod
    def to_checksum_address(addr):
        # mimic Web3 checksum behavior by returning the same (addresses already hex-valid)
        return addr

# ---------- Tests ----------

def assert_true(ok, msg):
    if not ok:
        raise AssertionError(msg)

def smoke_ensure_allowance_sufficient():
    w3 = StubWeb3(allowance_value=10**18)
    ok = ensure_allowance(
        w3, token=ADDR_TOKEN, owner=ADDR_OWNER, spender=ADDR_ROUTER,
        min_needed=10**17, pk_hex=None
    )
    print("[ensure_allowance] sufficient →", ok)
    assert_true(ok is True, "Should be True when current allowance is sufficient")

def smoke_ensure_allowance_needs_tx():
    w3 = StubWeb3(allowance_value=0)
    ok = ensure_allowance(
        w3, token=ADDR_TOKEN, owner=ADDR_OWNER, spender=ADDR_ROUTER,
        min_needed=10**17, pk_hex="11"*32, gas_price_wei=w3.eth.gas_price
    )
    print("[ensure_allowance] approve-tx →", ok)
    assert_true(ok is True, "Should be True after sending approve")

def smoke_build_exact_input_tx():
    w3 = StubWeb3()
    tx = build_exact_input_tx(
        w3, router=ADDR_ROUTER,
        path_bytes=b"\x11\x22\x33", amount_in=12345, min_amount_out=6789,
        recipient=ADDR_OWNER, deadline_ts=int(time.time())+60,
        gas_price_wei=w3.eth.gas_price
    )
    print("[build_exact_input_tx] keys:", sorted(tx.keys()))
    expect = {"from", "nonce", "gas", "gasPrice", "chainId", "value"}
    assert_true(expect.issubset(tx.keys()), "Tx dict missing required keys")

def smoke_send_signed_tx():
    w3 = StubWeb3()
    tx = {"to": ADDR_ROUTER, "from": ADDR_OWNER, "nonce":0, "gas":500000, "gasPrice":w3.eth.gas_price, "chainId":56, "value":0}
    h = send_signed_tx(w3, tx, "22"*32)
    print("[send_signed_tx] hash:", h)
    assert_true(isinstance(h, str) and h.startswith("0x") and len(h) == 66, "Tx hash format invalid")

if __name__ == "__main__":
    print("Running pcs_v3.trade smoke tests (no chain access, no broadcast)")
    smoke_ensure_allowance_sufficient()
    smoke_ensure_allowance_needs_tx()
    smoke_build_exact_input_tx()
    smoke_send_signed_tx()
    print("All smoke tests PASS ✅")
