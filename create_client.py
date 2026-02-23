
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from py_clob_client.client import ClobClient,BuilderConfig
from .config import HOST, PRIVATE_KEY, CHAIN_ID, FUNDER_ADDRESS, POLYGON_RPC
from web3 import Web3
from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds


async def is_gnosis_safe(address: str) -> bool:
    try:
        w3 = Web3(Web3.HTTPProvider(POLYGON_RPC))
        checksum_address = Web3.to_checksum_address(address)
        code = w3.eth.get_code(checksum_address)
        return code != b'0x'  
    except:
        return False
    

async def get_clob_client() -> ClobClient:  # 可以改成 async 如果需要
    if not all([HOST, PRIVATE_KEY, CHAIN_ID, FUNDER_ADDRESS]):
        raise RuntimeError("缺少必要的环境变量")

    # Step 1: 用 key 先建临时 client 来派生 API creds（必须）
    temp_client = ClobClient(
        host=HOST,
        key=PRIVATE_KEY,
        chain_id=CHAIN_ID,
    )
    api_creds = temp_client.create_or_derive_api_creds()
    temp_client.set_api_creds(api_creds)  

    # Step 2: 判断是否 Safe
    is_proxy_safe = await is_gnosis_safe(FUNDER_ADDRESS)  
    
    builder_creds = BuilderApiKeyCreds(
        key=os.environ["BUILDER_API_KEY"],
        secret=os.environ["BUILDER_SECRET"],
        passphrase=os.environ["BUILDER_PASS_PHRASE"]
    )

    builder_config = BuilderConfig(
        local_builder_creds=builder_creds
    )
    # Step 3: 建最终 client
    #signature_type=2 if is_proxy_safe else 0
    client = ClobClient(
        host=HOST,
        key=PRIVATE_KEY,
        chain_id=CHAIN_ID,
        creds=api_creds,# 重要！带上 L2 creds
        funder=FUNDER_ADDRESS,               
        signature_type=2, # 0外部  1  2内置
        builder_config =builder_config               
        
    )

    print(f"ClobClient 初始化完成，signature_type={'POLY_GNOSIS_SAFE (2)' if is_proxy_safe else 'EOA (0)'}")
    return client



