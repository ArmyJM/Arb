from __future__ import annotations
from typing import Optional
from web3 import Web3

# Minimal ERC20 ABI (allowance/approve/balance/decimals/symbol)
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"}
        ],
        "name": "allowance",
        "outputs": [{"name": "remaining", "type": "uint256"}],
        "type": "function",
        "stateMutability": "view"
    },
    {
        "constant": False,
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_value", "type": "uint256"}
        ],
        "name": "approve",
        "outputs": [{"name": "success", "type": "bool"}],
        "type": "function",
        "stateMutability": "nonpayable"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
        "stateMutability": "view"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
        "stateMutability": "view"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
        "stateMutability": "view"
    },
]

# Minimal Pancake V3 SwapRouter exactInput ABI
ROUTER_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"internalType": "bytes", "name": "path", "type": "bytes"},
                    {"internalType": "address", "name": "recipient", "type": "address"},
                    {"internalType": "uint256", "name": "deadline", "type": "uint256"},
                    {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
                    {"internalType": "uint256", "name": "amountOutMinimum", "type": "uint256"},
                ],
                "internalType": "struct IV3SwapRouter.ExactInputParams",
                "name": "params",
                "type": "tuple",
            }
        ],
        "name": "exactInput",
        "outputs": [{"internalType": "uint256", "name": "amountOut", "type": "uint256"}],
        "stateMutability": "payable",
        "type": "function",
    }
]


def _to_checksum(addr: str) -> str:
    return Web3.to_checksum_address(addr)


def erc20(w3: Web3, addr: str):
    return w3.eth.contract(address=_to_checksum(addr), abi=ERC20_ABI)


def router_contract(w3: Web3, addr: str):
    return w3.eth.contract(address=_to_checksum(addr), abi=ROUTER_ABI)


def ensure_allowance(
    w3: Web3,
    token: str,
    owner: str,
    spender: str,
    min_needed: int,
    pk_hex: Optional[str] = None,
    gas_price_wei: Optional[int] = None,
) -> bool:
    """
    Ensure router 'spender' has allowance >= min_needed for 'token' from 'owner'.
    If not and pk_hex is provided, send an approve txn for 2x min_needed.
    Returns True if sufficient allowance afterwards, False otherwise.
    """
    token = _to_checksum(token)
    owner = _to_checksum(owner)
    spender = _to_checksum(spender)

    c = erc20(w3, token)
    current = c.functions.allowance(owner, spender).call()
    if current >= min_needed:
        return True

    if not pk_hex:
        return False

    # prefer a higher allowance to reduce future approvals (2x min or max uint ~ optional)
    new_allow = int(min_needed) * 2
    tx = c.functions.approve(spender, new_allow).build_transaction(
        {
            "from": owner,
            "nonce": w3.eth.get_transaction_count(owner),
            "gas": 60000,
            "gasPrice": gas_price_wei or w3.eth.gas_price,
            "chainId": w3.eth.chain_id,
        }
    )
    signed = w3.eth.account.sign_transaction(tx, pk_hex if pk_hex.startswith("0x") else "0x" + pk_hex)
    txh = w3.eth.send_raw_transaction(signed.rawTransaction)
    rc = w3.eth.wait_for_transaction_receipt(txh)
    return rc.status == 1


def build_exact_input_tx(
    w3: Web3,
    router: str,
    path_bytes: bytes,
    amount_in: int,
    min_amount_out: int,
    recipient: str,
    deadline_ts: int,
    gas_price_wei: Optional[int] = None,
    nonce: Optional[int] = None,
    value_wei: int = 0,
) -> dict:
    """
    Build a Pancake V3 exactInput transaction using the provided path bytes.
    The caller can adjust gas/nonce before signing & sending.
    """
    r = router_contract(w3, router)
    params = (
        path_bytes,
        _to_checksum(recipient),
        int(deadline_ts),
        int(amount_in),
        int(min_amount_out),
    )
    tx = r.functions.exactInput(params).build_transaction(
        {
            "from": _to_checksum(recipient),
            "nonce": w3.eth.get_transaction_count(_to_checksum(recipient)) if nonce is None else int(nonce),
            "gas": 500_000,  # consider estimating with eth_estimateGas beforehand
            "gasPrice": gas_price_wei or w3.eth.gas_price,
            "chainId": w3.eth.chain_id,
            "value": int(value_wei),
        }
    )
    return tx


def send_signed_tx(w3: Web3, tx: dict, pk_hex: str) -> str:
    """
    Sign and broadcast a raw transaction. Returns the tx hash as 0x-prefixed hex string.
    """
    key = pk_hex if pk_hex.startswith("0x") else "0x" + pk_hex
    signed = w3.eth.account.sign_transaction(tx, key)
    txh = w3.eth.send_raw_transaction(signed.rawTransaction)
    h = txh.hex()
    return h if h.startswith("0x") else ("0x" + h)
