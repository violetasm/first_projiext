
'''
{
    √ "proxyWallet": "0x7d25909486569ae9351a1836bf3791508cacf2d3",
    √ "side": "SELL",
    √ "asset": "81261770183438646256276643924804378540794123834334825027933741409618974948603",
    √ "conditionId": "0xdb1663a98f5050db85188589cddc5586c7749f1483ebef8dc79d366a0b76483d",
    √ "size": 106,
    √ "price": 0.76,
    √ "timestamp": 1770446487,
    "title": "Bitcoin Up or Down - February 7, 1AM ET",
    √ "slug": "bitcoin-up-or-down-february-7-1am-et",
    "icon": "https://polymarket-upload.s3.us-east-2.amazonaws.com/BTC+fullsize.png",
    √ "eventSlug": "bitcoin-up-or-down-february-7-1am-et",
    "outcome": "Up",
    "outcomeIndex": 0,
    "name": "1026446010",
    "pseudonym": "Grateful-Wick",
    "bio": "",
    "profileImage": "https://polymarket-upload.s3.us-east-2.amazonaws.com/profile-image-3699116-a2adb630-8b1d-450a-acf0-6dc8931222f1.webp",
    "profileImageOptimized": "",
    √ "transactionHash": "0x7f28fe936d33b08bb02e2b2b534e1cf8bb81b1694ff2fdac39d824eadbb47883"
  }
'''

import sqlite3
import json
from pathlib import Path
from typing import List, Dict, Optional, Union,Tuple
from datetime import datetime,timezone

from typing import Optional, Tuple, List, Union, Dict
from pathlib import Path
import sqlite3
from datetime import datetime, timezone


# primate key  hash  asset无法区分买卖

class PolymarketTradeManager:
    """
    Polymarket 交易记录管理类（主键为 transactionHash 版本）
    支持 增 / 删 / 改 / 查
    只存储核心字段 + 3个布尔标志（默认全 false）
    主键：transaction_hash
    """

    CORE_FIELDS = [
        "proxyWallet", "side", "asset", "conditionId",
        "size", "price", "timestamp", "slug",
        "eventSlug", "transactionHash"
    ]

    def __init__(
        self,
        db_path: Union[str, Path, None] = "polymarket_trades.db",
        use_sqlite: bool = True
    ):
        self.use_sqlite = use_sqlite
        self.conn = None
        self.cursor = None
        self.trades: List[Dict] = []  # 仅内存模式使用

        if use_sqlite:
            self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
            self._init_db()
        # else: 内存模式已初始化为空列表

    def _init_db(self):
        """初始化表，主键为 transaction_hash"""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            proxy_wallet     TEXT,
            side             TEXT CHECK(side IN ('BUY', 'SELL', 'REDEEM', NULL)),
            asset            TEXT,
            condition_id     TEXT,
            size             REAL,
            price            REAL,
            timestamp        INTEGER,
            slug             TEXT,
            event_slug       TEXT,
            transaction_hash TEXT PRIMARY KEY NOT NULL,
            is_buy           BOOLEAN DEFAULT 0,
            is_sell          BOOLEAN DEFAULT 0,
            is_redeem        BOOLEAN DEFAULT 0,
            inserted_at      TEXT DEFAULT (datetime('now'))
        )
        ''')

        # 常用索引（主键已自带索引）
        self.cursor.executescript('''
        CREATE INDEX IF NOT EXISTS idx_wallet      ON trades(proxy_wallet);
        CREATE INDEX IF NOT EXISTS idx_condition   ON trades(condition_id);
        CREATE INDEX IF NOT EXISTS idx_timestamp   ON trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_is_buy      ON trades(is_buy);
        CREATE INDEX IF NOT EXISTS idx_is_sell     ON trades(is_sell);
        ''')
        self.conn.commit()

    def add_trade(self, trade_data: Dict) -> bool:
        """添加记录，主键冲突时自动失败（返回 False）"""
        record = {k: trade_data.get(k) for k in self.CORE_FIELDS}

        tx_hash = record.get("transactionHash")
        if not tx_hash:
            print("缺少 transactionHash，无法添加")
            return False

        # 根据 side 自动设置标志（可选）
        is_buy = is_sell = is_redeem = 0
        ''' 
        if auto_set_flags and record.get("side"):
            side = record["side"].upper()
            if side == "BUY":
                is_buy = 1
            elif side == "SELL":
                is_sell = 1
            elif side == "REDEEM":
                is_redeem = 1
        '''
        

        if self.use_sqlite:
            try:
                self.cursor.execute('''
                INSERT INTO trades (
                    proxy_wallet, side, asset, condition_id, size, price,
                    timestamp, slug, event_slug, transaction_hash,
                    is_buy, is_sell, is_redeem
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    record["proxyWallet"], record["side"], record["asset"],
                    record["conditionId"], record["size"], record["price"],
                    record["timestamp"], record["slug"], record["eventSlug"],
                    tx_hash,
                    is_buy, is_sell, is_redeem
                ))
                self.conn.commit()
                return True
            except sqlite3.IntegrityError:
                # 主键冲突（已存在相同 transaction_hash）
                print(f"已存在相同交易: {tx_hash[:12]}...")
                return False
            except Exception as e:
                print(f"插入失败: {e}")
                return False

        else:
            # 内存模式
            if any(t["transactionHash"] == tx_hash for t in self.trades):
                return False
            record.update({"is_buy": bool(is_buy), "is_sell": bool(is_sell), "is_redeem": bool(is_redeem)})
            self.trades.append(record)
            return True

    def update_trade(self, tx_hash: str, updates: Dict) -> bool:
        """
        修改记录（通过 transactionHash 定位）
        updates 示例: {"price": 0.88, "is_buy": True, "side": "BUY"}
        """
        if not tx_hash:
            print("hash err")
            return False

        # 只允许更新这些字段
        allowed = set(self.CORE_FIELDS) | {"is_buy", "is_sell", "is_redeem"}
        valid_updates = {k: v for k, v in updates.items() if k in allowed}

        if not valid_updates:
            #print("valid_updates err")
            return False

        if self.use_sqlite:
            set_parts = []
            params = []
            for k, v in valid_updates.items():
                db_key = k.replace("_", "") if "_" not in k else k  # 兼容 camel/snake
                set_parts.append(f"{db_key} = ?")
                params.append(v)

            params.append(tx_hash)
            set_clause = ", ".join(set_parts)

            try:
                self.cursor.execute(f'''
                    UPDATE trades SET {set_clause} WHERE transaction_hash = ?
                ''', params)
                updated = self.cursor.rowcount > 0
                if updated:
                    self.conn.commit()
                return updated
            except Exception as e:
                print(f"更新失败: {e}")
                return False
        else:
            for trade in self.trades:
                if trade["transactionHash"] == tx_hash:
                    trade.update(valid_updates)
                    return True
            return False
   
    def get_trade_by_hash(self, tx_hash: str) -> Optional[Dict]:
        """按主键（transactionHash）查询单条记录"""
        if self.use_sqlite:
            self.cursor.execute('''
                SELECT * FROM trades WHERE transaction_hash = ?
            ''', (tx_hash,))
            row = self.cursor.fetchone()
            if row:
                columns = [desc[0] for desc in self.cursor.description]
                return dict(zip(columns, row))
            return None
        else:
            for t in self.trades:
                if t["transactionHash"] == tx_hash:
                    return t.copy()
            return None

    def get_trades(
        self,
        wallet: Optional[str] = None,
        condition_id: Optional[str] = None,
        is_buy: Optional[bool] = None,
        is_sell: Optional[bool] = None,
        limit: int = 100,
        order_by: str = "timestamp DESC"
    ) -> List[Dict]:
        """查询多条记录"""
        if self.use_sqlite:
            query = """
            SELECT proxy_wallet, side, asset, condition_id, size, price,
                   timestamp, slug, event_slug, transaction_hash,
                   is_buy, is_sell, is_redeem
            FROM trades
            """
            params = []
            conditions = []

            if wallet:          conditions.append("proxy_wallet = ?"); params.append(wallet)
            if condition_id:    conditions.append("condition_id = ?"); params.append(condition_id)
            if is_buy is not None:   conditions.append("is_buy = ?");   params.append(1 if is_buy else 0)
            if is_sell is not None:  conditions.append("is_sell = ?");  params.append(1 if is_sell else 0)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += f" ORDER BY {order_by} LIMIT ?"
            params.append(limit)

            self.cursor.execute(query, params)
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

        else:
            result = self.trades[:]
            if wallet:          result = [t for t in result if t.get("proxyWallet") == wallet]
            if condition_id:    result = [t for t in result if t.get("conditionId") == condition_id]
            if is_buy is not None:   result = [t for t in result if t.get("is_buy") == is_buy]
            if is_sell is not None:  result = [t for t in result if t.get("is_sell") == is_sell]

            desc = "DESC" in order_by.upper()
            result.sort(key=lambda x: x.get("timestamp", 0), reverse=desc)
            return result[:limit]

    def delete_by_tx_hash(self, tx_hash: str) -> bool:
        """删除单条（通过主键）"""
        if self.use_sqlite:
            self.cursor.execute("DELETE FROM trades WHERE transaction_hash = ?", (tx_hash,))
            deleted = self.cursor.rowcount > 0
            if deleted:
                self.conn.commit()
            return deleted
        else:
            before = len(self.trades)
            self.trades = [t for t in self.trades if t["transactionHash"] != tx_hash]
            return len(self.trades) < before

    def delete_by_condition(self, condition_id: str) -> int:
        if self.use_sqlite:
            self.cursor.execute("DELETE FROM trades WHERE condition_id = ?", (condition_id,))
            count = self.cursor.rowcount
            if count > 0:
                self.conn.commit()
            return count
        else:
            before = len(self.trades)
            self.trades = [t for t in self.trades if t["conditionId"] != condition_id]
            return before - len(self.trades)

    def count(self) -> int:
        if self.use_sqlite:
            self.cursor.execute("SELECT COUNT(*) FROM trades")
            return self.cursor.fetchone()[0]
        return len(self.trades)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_earliest_trade(self) -> Optional[Dict]:
        """获取最早的一条交易记录（按 timestamp 升序的第一条）"""
        if self.use_sqlite:
            self.cursor.execute('''
                SELECT * FROM trades 
                ORDER BY timestamp ASC 
                LIMIT 1
            ''')
            row = self.cursor.fetchone()
            if row:
                columns = [desc[0] for desc in self.cursor.description]
                return dict(zip(columns, row))
            return None
        
        else:
            if not self.trades:
                return None
            sorted_trades = sorted(self.trades, key=lambda x: x.get("timestamp", 0))
            return sorted_trades[0].copy()

    def get_latest_trade(self) -> Optional[Dict]:
        """获取最新的一条交易记录（按 timestamp 降序的第一条）"""
        if self.use_sqlite:
            self.cursor.execute('''
                SELECT * FROM trades 
                ORDER BY timestamp DESC 
                LIMIT 1
            ''')
            row = self.cursor.fetchone()
            if row:
                columns = [desc[0] for desc in self.cursor.description]
                return dict(zip(columns, row))
            return None
        
        else:  # 内存模式
            if not self.trades:
                return None
            # 按 timestamp 降序排序后取第一条
            sorted_trades = sorted(self.trades, key=lambda x: x.get("timestamp", 0), reverse=True)
            return sorted_trades[0].copy()

    def get_earliest_unprocessed_buy(self) -> Optional[Tuple[Dict, bool]]:
        """
        获取最早的、尚未标记为已买入的交易记录
        
        返回:
            (trade_dict, success) 或 (None, False)
        """
        if self.use_sqlite:
            self.cursor.execute('''
                SELECT * FROM trades 
                WHERE is_buy = 0
                ORDER BY timestamp ASC 
                LIMIT 1
            ''')
            row = self.cursor.fetchone()
            if not row:
                return None, False
            columns = [desc[0] for desc in self.cursor.description]
            trade = dict(zip(columns, row))
            return trade, True
        
        else:  # 内存模式
            candidates = [t for t in self.trades if not t.get("is_buy")]
            if not candidates:
                return None, False
            earliest = min(candidates, key=lambda x: x.get("timestamp", float('inf')))
            return earliest.copy(), True

    def mark_as_buy_processed(self, tx_hash: str) -> bool:
        """
        将指定交易标记为已买入（is_buy = 1）
        """
        if not tx_hash:
            return False

        if self.use_sqlite:
            try:
                self.cursor.execute(
                    "UPDATE trades SET is_buy = 1 WHERE transaction_hash = ? AND is_buy = 0",
                    (tx_hash,)
                )
                updated = self.cursor.rowcount > 0
                if updated:
                    self.conn.commit()
                return updated
            except Exception as e:
                print(f"标记失败: {e}")
                return False
        
        else:
            for trade in self.trades:
                if trade["transactionHash"] == tx_hash and not trade.get("is_buy"):
                    trade["is_buy"] = True
                    return True
            return False

    @staticmethod
    def timestamp_to_str(ts: int) -> str:
        return (datetime.fromtimestamp(ts, tz=timezone.utc)).strftime("%Y-%m-%d %H:%M:%S UTC")


# ────────────────────────────────────────────────
# 示例
# ────────────────────────────────────────────────

if __name__ == "__main__":
    mgr = PolymarketTradeManager()

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

    print("添加结果:", mgr.add_trade(sample))
    print("总记录数:", mgr.count())

    # 修改价格
    mgr.update_trade(sample["transactionHash"], {"price": 0.81})

    # 查询单条
    trade = mgr.get_trade_by_hash(sample["transactionHash"])
    if trade:
        print("查询到交易:", trade["side"], "@", trade["price"],trade["transactionHash"])

    mgr.close()