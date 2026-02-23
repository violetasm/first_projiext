
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from web3 import Web3
#from web3.middleware import geth_poa_middleware
import time
from eth_account import Account
from .config import (
    PRIVATE_KEY,                        
    POLYGON_RPC,    
    USDC_ADDRESS,
    MAIN_SPENDER
         
)

# Polygon RPC（免费）
w3 = Web3(Web3.HTTPProvider(POLYGON_RPC))
#w3.middleware_onion.inject(geth_poa_middleware, layer=0)

# 关键合约地址（2025-2026 当前有效）
CTF = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"           # Conditional Tokens (ERC-1155)
EXCHANGE_MAIN = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"   # 主 exchange
EXCHANGE_NEG_RISK = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

# ERC20 approve ABI
ERC20_ABI = [
    {
        "constant": False,
        "inputs": [{"name": "_spender", "type": "address"}, {"name": "_value", "type": "uint256"}],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function"
    }
]

# ERC1155 setApprovalForAll ABI
ERC1155_ABI = [
    {
        "constant": False,
        "inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}],
        "name": "setApprovalForAll",
        "outputs": [],
        "type": "function"
    }
]

def setup_approvals():
    acct = Account.from_key(PRIVATE_KEY)
    contract = w3.eth.contract(address=USDC_ADDRESS, abi=[{
        "constant": False,
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_value", "type": "uint256"}
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function"
    }])

    MAX_UINT = 2**256 - 1

    for spender in [MAIN_SPENDER]:
        try:
            nonce = w3.eth.get_transaction_count(acct.address, 'pending')
            tx = contract.functions.approve(spender, MAX_UINT).build_transaction({
                'from': acct.address,
                'nonce': nonce,
                'gas': 100000,
                'maxFeePerGas': w3.eth.max_priority_fee + w3.eth.gas_price * 2,
                'maxPriorityFeePerGas': w3.eth.max_priority_fee,
                'chainId': 137
            })

            signed = acct.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            print(f"授权已发送给 {spender}: {tx_hash.hex()}")

            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
            print(f"授权确认 - 区块: {receipt.blockNumber}, 状态: {receipt.status}")
            time.sleep(1)

        except Exception as e:
            print(f"授权 {spender} 失败: {e}")

def approve_usdc(w3: Web3, private_key: str, spender: str, token_address: str):
    if not Web3.is_address(spender):
        raise ValueError(f"无效 spender 地址: {spender}")
    if not Web3.is_address(token_address):
        raise ValueError(f"无效 token 地址: {token_address}")

    account = Account.from_key(private_key)
    your_address = account.address

    # ERC20 ABI 最小版（只需 approve）
    erc20_abi = [
        {
            "constant": False,
            "inputs": [
                {"name": "_spender", "type": "address"},
                {"name": "_value", "type": "uint256"}
            ],
            "name": "approve",
            "outputs": [{"name": "", "type": "bool"}],
            "type": "function"
        }
    ]

    contract = w3.eth.contract(address=token_address, abi=erc20_abi)

    max_amount = 2**256 - 1  # 无限额

    txn = contract.functions.approve(spender, max_amount).build_transaction({
        'from': your_address,
        'nonce': w3.eth.get_transaction_count(your_address),
        'gas': 80000,  # approve 通常 50k-80k
        'gasPrice': w3.eth.gas_price,
        'chainId': 137
    })

    signed_txn = account.sign_transaction(txn)
    tx_hash = w3.eth.send_raw_transaction(signed_txn.raw_transaction)

    print(f"批准 {token_address} -> {spender} 交易发送: {tx_hash.hex()}")
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
    if receipt.status == 1:
        print("批准成功！")
    else:
        print("批准失败，检查 gas 或 revert reason")
    return tx_hash.hex()
