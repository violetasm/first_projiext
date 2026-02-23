
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import httpx  
from typing import List, Dict, Optional
from .config import TARGET_WALLET
from pathlib import Path
import json
from .TradeManager import TradeManager
from .SqlManager import PolymarketTradeManager,AsyncPolymarketTradeManager


async def inquire_target_wallet(
    target_wallet: Optional[str],
    amount: int = 10,  # 你要的条数，这里固定 10
) -> Optional[List[Dict]]:
    """
    查询目标钱包的最近 amount 条交易（API 默认最新先），
    然后反转列表，让第一条成为最早的交易。
    """
    if not target_wallet:
        print("缺少 TARGET_WALLET")
        return None
    print(target_wallet)
    url = "https://data-api.polymarket.com/trades"
    params = {
        "user": target_wallet.lower(),  
        "limit": amount,                
        "takerOnly": "false"
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()  # 自动抛异常如果 != 200

            trades: List[Dict] = response.json()  # 假设返回 list[dict]
            
            if not trades:
                print("没有找到交易记录")
                return []

            # 反转列表
            trades.reverse()  # 或 
            #trades = trades[::-1]
            #trades.sort(key=lambda x: x.get("timestamp", 0)) 
            
            print(f"成功获取 {len(trades)} 条交易，已反转为最早优先")
            return trades[:amount]  # 确保不超过 amount

        except httpx.HTTPStatusError as e:
            print(f"API 查询失败: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            print(f"请求异常: {str(e)}")
            return None

#sqlite3数据库
'''
sample = {
        "proxyWallet": "0x7d25909486569ae9351a1836bf3791508cacf2d3",
        "side": "SELL",
        "asset": "81261770183438646256276643924804378540794123834334825027933741409618974948603",
        "conditionId": "0xdb1663a98f5050db85188589cddc5586c7749f1483ebef8dc79d366a0b76483d",
        "size": 106,
        "price": 0.76,
        "timestamp": 1770446487,
        "slug": "bitcoin-up-or-down-february-7-1am-et",
        "eventSlug": "bitcoin-up-or-down-february-7-1am-et",
        "transactionHash": "0x7f28fe936d33b08bb02e2b2b534e1cf8bb81b1694ff2fdac39d824eadbb47883"
    }
'''
async def append_trades(manager:AsyncPolymarketTradeManager):
    #  1  get trades
    trades = await inquire_target_wallet(TARGET_WALLET)
    if trades is None or not trades:
        print("本次没有获取到交易数据")
        return False
    for trade in trades:
        new_trade = {
            "proxyWallet": trade.get("proxyWallet"),
            "side": trade.get("side"),
            "asset": trade.get("asset"),
            "conditionId": trade.get("conditionId"),
            "size": trade.get("size"),
            "price": trade.get("price"),
            "timestamp": trade.get("timestamp"),
            "slug": trade.get("slug"),
            "eventSlug": trade.get("eventSlug"),
            "transactionHash": trade.get("transactionHash")
        }
        #print(new_trade)
        #print("\r\t")
        await manager.add_trade(new_trade)

    return True




# json文件版本  遗弃
async def append_trades_abandon() -> bool:
    """
    从 API 获取交易记录，追加到 JSON 文件中。
    返回是否成功追加（至少有一条新记录被写入）。
    """
    trades = await inquire_target_wallet(TARGET_WALLET)
    if trades is None or not trades:
        print("本次没有获取到交易数据")
        return False

    trade_json = TradeManager("./trades_date.json") 
    
    for trade in trades:
        trade_json.append(trade)
        
    if trade_json != None:
        return True
    else:
        return False