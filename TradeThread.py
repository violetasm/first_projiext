
import threading
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from create_client import get_clob_client  
from SqlManager import PolymarketTradeManager
from inquire_target_wallet import append_trades
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

async def process_trade(client: ClobClient, trade: dict):
    """执行单笔交易"""
    try:
        token_id = str(trade['token_id'])  # 必须是字符串
        is_buy = bool(trade.get('is_buy', 1))  # 1 或 True → BUY
        price = float(trade['price'])          # e.g. 0.5123
        size = float(trade['size'])            # 股份数，支持小数

        side = BUY if is_buy else SELL
        side_str = "BUY" if is_buy else "SELL"

        print(f"[{side_str}] token_id={token_id} | price={price:.4f} | size={size}")

        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=side
        )

        signed_order = client.create_order(order_args)
        resp = client.post_order(signed_order, OrderType.GTC)

        print(f"下单成功: {resp}")

        # 成功 → 从 DB 删除或标记已处理
        # poly_sql.delete_trade(trade['id'])  # 你实现这个
        # 或 poly_sql.mark_done(trade['id'])

    except Exception as e:
        print(f"下单失败: {str(e)}")
        # 可记录失败次数，重试或跳过



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
    def __init__(self, poly_sql, interval=10.0):
        super().__init__(name="PollingThread")
        self.poly_sql = poly_sql
        self.interval = interval

    def run(self):
        print(f"{self.name} 启动，每 {self.interval}s 查询一次")
        while not self.stop_event.is_set():
            try:
                # 因为 append_trades 是 async，我们用 asyncio.run_coroutine_threadsafe
                future = asyncio.run_coroutine_threadsafe(
                    append_trades(self.poly_sql),
                    asyncio.get_event_loop()
                )
                future.result()  # 等待完成
                print(f"{self.name} 完成一次追加")
            except Exception as e:
                print(f"{self.name} 异常: {e}")
            
            time.sleep(self.interval)


class TradingThread(BasePolymarketThread):
    """执行交易 + 删除线程"""
    def __init__(self, client, poly_sql, interval=0.2):
        super().__init__(name="TradingThread")
        self.client = client
        self.poly_sql = poly_sql
        self.interval = interval

    def run(self):
        print(f"{self.name} 启动，每 {self.interval}s 检查一次待执行交易")
        while not self.stop_event.is_set():
            try:
                pending = self.poly_sql.get_pending_trades()
                if pending:
                    print(f"{self.name} 发现 {len(pending)} 条待处理")
                    for trade in pending:
                        # 因为 process_trade 是 async，这里也需要桥接
                        future = asyncio.run_coroutine_threadsafe(
                            process_trade(self.client, trade),
                            asyncio.get_event_loop()
                        )
                        future.result()
                        self.poly_sql.mark_as_processed(trade['id'])
            except Exception as e:
                print(f"{self.name} 异常: {e}")
            
            time.sleep(self.interval)


