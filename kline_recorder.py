# coding: utf-8
import requests
import time
import csv
import os
import json
from datetime import datetime, timedelta, timezone
import gzip
import shutil

class KlineRecorder:
    def __init__(self, contract: str, settle: str = "usdt", interval: str = "1s", save_dir: str = "klines_data"):
        self.host = "https://api.gateio.ws"
        self.prefix = "/api/v4"
        self.headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        self.contract = contract
        self.settle = settle
        self.interval = interval

        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)

        self.state_file = os.path.join(self.save_dir, f"{self.contract}_state.json")
        self.last_timestamp = self.now_timestamp()

    def now_timestamp(self):
        return int(time.time())

    def fetch_klines(self, from_time, to_time):
        print(from_time, to_time)
        url = f"{self.host}{self.prefix}/futures/{self.settle}/candlesticks"
        params = {
            "contract": self.contract,
            "from": from_time,
            "to": to_time-1,
            "interval": self.interval
        }
        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"[{self.contract}] Error fetching data: {e}")
            return []

    def save_to_csv(self, data, date_str):
        file_path = os.path.join(self.save_dir, f"{self.contract}_{date_str}.csv")
        file_exists = os.path.isfile(file_path)

        with open(file_path, "a", newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            if not file_exists:
                writer.writerow(["timestamp", "open", "high", "low", "close", "volume", "sum"])
            for entry in data:
                writer.writerow([
                    int(entry["t"]),
                    entry["o"],
                    entry["h"],
                    entry["l"],
                    entry["c"],
                    entry.get("v", ""),
                    entry.get("sum", "")
                ])


    def wait_until_next_minute(self):
        now = datetime.now(timezone.utc)
        next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        self.last_timestamp = int(next_minute.timestamp())-120
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
        self.wait_until_next_minute()
        print(f"[{self.contract}] Start recording from {datetime.fromtimestamp(self.last_timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")

        current_date_str = datetime.fromtimestamp(self.last_timestamp, timezone.utc).strftime("%Y-%m-%d")

        while True:
            from_time = self.last_timestamp
            to_time = from_time + 60
            date_str = datetime.fromtimestamp(from_time, timezone.utc).strftime("%Y-%m-%d")

            # check if the date has changed
            if date_str != current_date_str:
                self.compress_csv_file(current_date_str)
                current_date_str = date_str

            data = self.fetch_klines(from_time, to_time)
            if data:
                self.save_to_csv(data, date_str)
                self.last_timestamp = to_time
                print(f"[{self.contract}] Saved {len(data)} klines. Next start: {datetime.fromtimestamp(self.last_timestamp, timezone.utc)}")
            else:
                print(f"[{self.contract}] No data fetched. Retrying in 10 seconds...")
                time.sleep(10)
                continue

            next_call = 60 - (time.time() % 60)
            time.sleep(next_call)

