# coding: utf-8
import websocket
import threading
import json
import time
import os
import csv
import requests
from datetime import datetime, timezone, timedelta
import gzip
import shutil

class OrderBookRecorder:
    def __init__(self, contract: str, settle: str = "usdt", save_dir: str = "orderbook_data"):
        self.contract = contract
        self.settle = settle
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)
        self.snapshots = []

        self.host_rest = "https://api.gateio.ws"
        self.rest_prefix = "/api/v4"
        self.ws_url = "wss://fx-ws.gateio.ws/v4/ws/usdt"

        self.headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        self.orderbook = {"bids": {}, "asks": {}}
        self.last_timestamp = self.now_timestamp()

        self.ws = None
        self.lock = threading.Lock()

    def now_timestamp(self):
        return int(time.time())

    def fetch_initial_orderbook(self):
        url = f"{self.host_rest}{self.rest_prefix}/futures/{self.settle}/order_book"
        params = {
            "contract": self.contract,
            "limit": 100,
            "with_id": "true"
        }
        resp = requests.get(url, headers=self.headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        with self.lock:
            self.orderbook["bids"] = {float(bid["p"]): float(bid["s"]) for bid in data["bids"]}
            self.orderbook["asks"] = {float(ask["p"]): float(ask["s"]) for ask in data["asks"]}
            self.base_id = data["id"]

        print(f"[{self.contract}] Initial orderbook fetched, base_id: {self.base_id}")

    def on_message(self, ws, message):
        msg = json.loads(message)
        if msg.get("event") != "update":
            return
        result = msg.get("result", {})
        contract = result.get("s")
        if contract != self.contract:
            return

        updates_bids = result.get("b", [])
        updates_asks = result.get("a", [])

        with self.lock:
            for update in updates_bids:
                price = float(update["p"])
                size = float(update["s"])
                if size == 0:
                    self.orderbook["bids"].pop(price, None)
                else:
                    self.orderbook["bids"][price] = size

            for update in updates_asks:
                price = float(update["p"])
                size = float(update["s"])
                if size == 0:
                    self.orderbook["asks"].pop(price, None)
                else:
                    self.orderbook["asks"][price] = size

    def on_error(self, ws, error):
        print(f"[{self.contract}] WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"[{self.contract}] WebSocket closed: {close_status_code} {close_msg}")

    def on_open(self, ws):
        payload = {
            "time": int(time.time()),
            "channel": "futures.order_book_update",
            "event": "subscribe",
            "payload": [self.contract, "100ms", "100"]
        }
        ws.send(json.dumps(payload))
        print(f"[{self.contract}] WebSocket subscribed.")

    def start_ws(self):
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        self.ws.run_forever(ping_interval=20, ping_timeout=10)

    def collect_orderbook_snapshot(self):
        now_ts = self.now_timestamp()
        snapshot = {"timestamp": now_ts}

        def group_orderbook(orderbook_side, reverse):
            sorted_orders = sorted(orderbook_side.items(), reverse=reverse)[:50]
            groups = []
            for i in range(0, 50, 10):
                group = sorted_orders[i:i+10]
                if not group:
                    groups.append(("", ""))
                    continue
                total_size = sum(size for price, size in group)
                if total_size == 0:
                    groups.append(("", ""))
                else:
                    weighted_price = sum(price * size for price, size in group) / total_size
                    groups.append((weighted_price, total_size))
            return groups

        with self.lock:
            bid_groups = group_orderbook(self.orderbook["bids"], reverse=True)
            ask_groups = group_orderbook(self.orderbook["asks"], reverse=False)

        for idx, (price, size) in enumerate(bid_groups):
            snapshot[f"bid_group_{idx}_price"] = price
            snapshot[f"bid_group_{idx}_size"] = size
        for idx, (price, size) in enumerate(ask_groups):
            snapshot[f"ask_group_{idx}_price"] = price
            snapshot[f"ask_group_{idx}_size"] = size

        self.snapshots.append(snapshot)
    
    def flush_snapshots_to_csv(self, date_str):
        if not self.snapshots:
            return

        file_path = os.path.join(self.save_dir, f"{self.contract}_{date_str}.csv")
        file_exists = os.path.isfile(file_path)

        fieldnames = ["timestamp"] + \
                     [f"bid_group_{i}_{x}" for i in range(5) for x in ("price", "size")] + \
                     [f"ask_group_{i}_{x}" for i in range(5) for x in ("price", "size")]

        with open(file_path, "a", newline='', encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()

            for snapshot in self.snapshots:
                writer.writerow(snapshot)

        print(f"[{self.contract}] Flushed {len(self.snapshots)} snapshots to file.")
        self.snapshots.clear()
    
    def validate_orderbook_sync(self):
        try:
            url = f"{self.host_rest}{self.rest_prefix}/futures/{self.settle}/order_book"
            params = {
                "contract": self.contract,
                "limit": 100,
                "with_id": "true"
            }
            resp = requests.get(url, headers=self.headers, params=params, timeout=10)
            resp.raise_for_status()
            latest = resp.json()

            # 把现有本地orderbook和最新orderbook都整理成dict
            with self.lock:
                local_bids = self.orderbook["bids"]
                local_asks = self.orderbook["asks"]

            fetched_bids = {float(bid["p"]): float(bid["s"]) for bid in latest["bids"]}
            fetched_asks = {float(ask["p"]): float(ask["s"]) for ask in latest["asks"]}

            # 简单比较前20档
            local_bid_prices = sorted(local_bids.keys(), reverse=True)[:20]
            fetched_bid_prices = sorted(fetched_bids.keys(), reverse=True)[:20]

            local_ask_prices = sorted(local_asks.keys())[:20]
            fetched_ask_prices = sorted(fetched_asks.keys())[:20]

            # 价格偏差率检测（比如价格差>0.1%就认为不同步）
            def price_diff_exceed(local_prices, fetched_prices, side_name):
                if len(local_prices) == 0 or len(fetched_prices) == 0:
                    return True
                for lp, fp in zip(local_prices, fetched_prices):
                    if abs(lp - fp) / max(lp, fp) > 0.001:
                        print(f"[{self.contract}] {side_name} desync detected: {lp} vs {fp}")
                        return True
                return False

            if price_diff_exceed(local_bid_prices, fetched_bid_prices, "bid") or price_diff_exceed(local_ask_prices, fetched_ask_prices, "ask"):
                print(f"[{self.contract}] Orderbook desynced. Refetching initial orderbook...")
                self.fetch_initial_orderbook()

        except Exception as e:
            print(f"[{self.contract}] Error during orderbook validation: {e}")

    def wait_until_next_minute(self):
        now = datetime.now(timezone.utc)
        next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        wait_seconds = (next_minute - now).total_seconds()
        print(f"[{self.contract}] Waiting {wait_seconds:.1f} seconds to start at next full minute...")
        time.sleep(wait_seconds)

    def compress_csv_file(self, date_str):
        file_path = os.path.join(self.save_dir, f"{self.contract}_{date_str}.csv")
        gz_path = file_path + ".gz"

        if os.path.exists(file_path):
            with open(file_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(file_path)
            print(f"[{self.contract}] Compressed and removed {file_path}")

    def run(self):
        self.fetch_initial_orderbook()
        threading.Thread(target=self.start_ws, daemon=True).start()

        self.wait_until_next_minute()

        validate_counter = 0
        snapshot_counter = 0
        current_date_str = datetime.fromtimestamp(self.now_timestamp(), timezone.utc).strftime("%Y-%m-%d")

        while True:
            now_ts = self.now_timestamp()
            date_str = datetime.fromtimestamp(now_ts, timezone.utc).strftime("%Y-%m-%d")

            # 检查日期是否变化，如果变化了，压缩昨天的文件
            if date_str != current_date_str:
                self.compress_csv_file(current_date_str)
                current_date_str = date_str

            self.collect_orderbook_snapshot()
            snapshot_counter += 1

            if snapshot_counter >= 60:
                self.flush_snapshots_to_csv(date_str)
                snapshot_counter = 0

            validate_counter += 1
            if validate_counter >= 60:
                self.validate_orderbook_sync()
                validate_counter = 0

            next_call = 1 - (time.time() % 1)
            time.sleep(next_call)




