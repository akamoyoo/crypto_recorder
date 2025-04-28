# Futures Data Recorder

This project provides two modules to record real-time futures market data from different exchanges:

- **OrderBookRecorder**: Records high-frequency order book snapshots (bids and asks).
- **KlineRecorder**: Records candlestick (kline) data at 1-second intervals.

Both recorders are designed for stable long-term operation, with automatic daily file rotation and compression.

---

## Features

- **Order Book Recording**
  - Subscribes to real-time futures order book updates via WebSocket.
  - Aggregates top 50 bid/ask levels into 5 groups, each using weighted average price.
  - Records snapshots every second.
  - Saves snapshots to CSV files per day.
  - Automatically compresses previous day's data into `.csv.gz` files.

- **Kline Recording**
  - Fetches 1-second interval candlestick data via REST API.
  - Batches and saves data every minute.
  - Organizes data into daily CSV files.
  - Automatically compresses previous day's data into `.csv.gz` files.

- **Robustness**
  - Automatic reconnection for WebSocket if needed.
  - Order book synchronization validation every minute.
  - Minimal memory footprint with periodic flushing.

---

## File Structure

| File Name | Description |
|:---|:---|
| `orderbook_recorder.py` | Implements the OrderBookRecorder class. |
| `kline_recorder.py` | Implements the KlineRecorder class. |
| `main.py` | Launches multiple recorders in parallel for multiple contracts. |

Generated data is stored in:

- `orderbook_data/`
- `klines_data/`

Each subdirectory will contain daily compressed `.csv.gz` files.

---

## Requirements

- Python 3.7+
- Libraries:
  - `requests`
  - `websocket-client`
  - `csv`
  - `gzip`
  - `shutil`

You can start the recorders via:

```bash
python3 main.py
