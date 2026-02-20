
import json
import os
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+
import threading

class TradeManager:
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.lock = threading.Lock()  # 保护文件读写
        self._data = []               # 内存缓存，可选

    def load_all(self) -> list:
        """同步读取全部数据"""
        with self.lock:
            if not self.file_path.exists():
                return []
            try:
                with self.file_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data if isinstance(data, list) else []
            except (json.JSONDecodeError, IOError):
                print(f"文件 {self.file_path} 读取失败或格式错误，返回空列表")
                return []

    def save_all(self, data: list):
        """同步写入全部数据"""
        with self.lock:
            try:
                # 原子写入：先写临时文件再替换
                tmp_path = self.file_path.with_suffix(".tmp")
                with tmp_path.open("w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                tmp_path.replace(self.file_path)
            except Exception as e:
                print(f"写入文件 {self.file_path} 失败: {e}")

    def append(self, trade: dict) -> bool:
        """追加单条交易（自动去重）"""
        data = self.load_all()

        # 去重（以 tx_hash 为例）
        #if any(item.get("tx_hash") == trade.get("tx_hash") for item in data):
           # return False

        data.append(trade)
        self.save_all(data)
        return True

    def add_trade(self, asset, slug, buy_price, condition_id, quantity, tx_hash):
        """兼容你原来的方法"""
        new_trade = {
            "tx_hash": tx_hash,
            "asset": asset,
            "slug": slug,
            "buy_price": float(buy_price),
            "quantity": float(quantity),
            "condition_id": condition_id,
            "timestamp": datetime.now(ZoneInfo("UTC")).isoformat(),
            "retry_count": 0
        }
        return self.append(new_trade)

    def remove_trade(self, tx_hash):
        data = self.load_all()
        data = [item for item in data if item.get("tx_hash") != tx_hash]
        self.save_all(data)



    