
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import threading
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor

from .SqlManager import PolymarketTradeManager,AsyncPolymarketTradeManager
from .inquire_target_wallet import append_trades
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

async def process_trade(client: ClobClient, trade: dict):
    """执行单笔交易"""
    try:
        token_id = str(trade['asset'])  # 必须是字符串
        #is_buy = bool(trade.get('is_buy', 1))  # 1 或 True → BUY
        side = str(trade["side"])
        price = float(trade["price"])*1.03          # e.g. 0.5123
        size = float(5.0)            # 股份数，支持小数 

        #side = BUY if is_buy else SELL
        #side_str = "BUY" if is_buy else "SELL"

        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=side
        )

        signed_order = client.create_order(order_args)
        resp = client.post_order(signed_order, OrderType.GTC)

        print(f"下单成功: {resp}")

    except Exception as e:
        print(f"下单失败: {str(e)}")
        # 可记录失败次数，重试或跳过

# 不再使用 threading.Thread
async def polling_task(interval: float = 10.0):
    sql = AsyncPolymarketTradeManager()
    async with sql:
        while True:
            try:
                await append_trades(sql)
                print("polling 完成一次")
            except Exception as e:
                print(f"polling 异常: {e}")
            await asyncio.sleep(interval)


async def trading_task(client, interval: float = 0.2):
    sql = AsyncPolymarketTradeManager()
    async with sql:
        while True:
            try:
                trade = await sql.get_latest_trade()
                if trade:
                    await process_trade(client, trade)
                    await sql.delete_by_tx_hash(trade['transaction_hash'])
            except Exception as e:
                print(f"trading 异常: {e}")
            await asyncio.sleep(interval)






class BasePolymarketThread(threading.Thread):
    def __init__(self, name="BaseThread"):
        super().__init__(name=name, daemon=True)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        raise NotImplementedError("子类必须实现 run()")


class PollingThread(BasePolymarketThread):
    """查询 + 添加交易线程""" 
    def __init__(self, interval=10.0, loop=None): 
        super().__init__(name="PollingThread") 
        self.interval = interval 
        self.loop = loop or asyncio.get_event_loop() # ❌ 不再在 __init__ 里接收或创建 poly_sql
    def run(self):   # ✅ 改成同步函数
        asyncio.run(self._async_run())

    async def _async_run(self):
        sql_manager = AsyncPolymarketTradeManager()

        print(f"{self.name} 启动")

        async with sql_manager:
            while not self.stop_event.is_set():
                try:
                    await append_trades(sql_manager)
                    print(f"{self.name} 完成一次数据追加")
                except Exception as e:
                    print(f"{self.name} 异常: {e}")

                await asyncio.sleep(self.interval)  # ❗不要用 time.sleep


class TradingThread(BasePolymarketThread):
    """执行交易 + 删除线程""" 
    def __init__(self, client, interval=0.2, loop=None): 
        super().__init__(name="TradingThread") 
        self.client = client 
        self.interval = interval 
        self.loop = loop or asyncio.get_event_loop() # ❌ 不再在 __init__ 里接收或创建 poly_sql
    def run(self):   # ✅ 必须同步
        asyncio.run(self._async_run())

    async def _async_run(self):
        sql_manager = AsyncPolymarketTradeManager()

        print(f"{self.name} 启动")

        async with sql_manager:
            while not self.stop_event.is_set():
                try:
                    trade = await sql_manager.get_latest_trade()

                    if trade:
                        await process_trade(self.client, trade)
                        await sql_manager.delete_by_tx_hash(
                            trade['transaction_hash']
                        )

                except Exception as e:
                    print(f"{self.name} 异常: {e}")

                await asyncio.sleep(self.interval)


