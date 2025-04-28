# coding: utf-8
from kline_recorder import KlineRecorder
from orderbook_recorder import OrderBookRecorder
import threading

def start_kline_recorder(contract_name):
    kline_recorder = KlineRecorder(contract=contract_name)
    kline_recorder.run()

def start_orderbook_recorder(contract_name):
    orderbook_recorder = OrderBookRecorder(contract=contract_name)
    orderbook_recorder.run()

if __name__ == "__main__":
    contracts = ["ALPACA_USDT", "BTC_USDT", "ETH_USDT", "SOL_USDT"]

    threads = []
    for contract in contracts:
        t1 = threading.Thread(target=start_kline_recorder, args=(contract,))
        t1.start()
        threads.append(t1)
        t2 = threading.Thread(target=start_orderbook_recorder, args=(contract,))
        t2.start()
        threads.append(t2)

    for t in threads:
        t.join()
