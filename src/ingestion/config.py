"""Central configuration for the ingestion layer."""
from datetime import datetime
from pathlib import Path

TICKERS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "V", "WMT"]

START_DATE = "2016-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

BASE_DIR = Path("data/raw")

# Used only when yfinance's .info call fails or returns no sector.
SECTOR_FALLBACK = {
    "AAPL": "Technology",
    "MSFT": "Technology",
    "NVDA": "Technology",
    "AMZN": "Consumer Cyclical",
    "GOOGL": "Communication Services",
    "META": "Communication Services",
    "TSLA": "Consumer Cyclical",
    "JPM": "Financial Services",
    "V": "Financial Services",
    "WMT": "Consumer Defensive",
}

PRICE_COLUMNS = [
    "ticker", "trade_date", "open", "high", "low", "close", "adjusted_close",
    "volume", "dividends", "stock_splits", "source", "ingested_at",
]

RETRIES = 3
RETRY_SLEEP_SECONDS = 5
