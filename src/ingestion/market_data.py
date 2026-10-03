"""Download prices, company metadata and financial statements from yfinance
and write them as Parquet files under data/raw/.

Run from the project root:  python src/ingestion/market_data.py
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

try:  # works both as `python src/ingestion/market_data.py` and `-m ingestion.market_data`
    from config import (BASE_DIR, END_DATE, PRICE_COLUMNS, RETRIES,
                        RETRY_SLEEP_SECONDS, SECTOR_FALLBACK, START_DATE, TICKERS)
except ImportError:  # pragma: no cover
    from ingestion.config import (BASE_DIR, END_DATE, PRICE_COLUMNS, RETRIES,
                                  RETRY_SLEEP_SECONDS, SECTOR_FALLBACK, START_DATE, TICKERS)


def utc_now() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc))


def with_retry(fn, *args, **kwargs):
    """Call fn, retrying on any exception (Yahoo throttles occasionally)."""
    last_exc = None
    for attempt in range(1, RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            print(f"  attempt {attempt}/{RETRIES} failed: {exc}")
            if attempt < RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS)
    raise last_exc


def clean_index(df: pd.DataFrame) -> pd.DataFrame:
    """Remove timezone information from a DatetimeIndex."""
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_localize(None)
    return df


def standardize_prices(df: pd.DataFrame, ticker_symbol: str) -> pd.DataFrame:
    """Rename yfinance columns to our schema and add lineage columns."""
    df = clean_index(df.copy())
    df.index.name = "Date"
    df = df.reset_index().rename(columns={
        "Date": "trade_date", "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Adj Close": "adjusted_close", "Volume": "volume",
        "Dividends": "dividends", "Stock Splits": "stock_splits",
    })
    if "adjusted_close" not in df.columns:  # fall back if source omits it
        df["adjusted_close"] = df["close"]
    for col in ("dividends", "stock_splits"):
        if col not in df.columns:
            df[col] = 0.0
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.tz_localize(None)
    df["ticker"] = ticker_symbol
    df["source"] = "yfinance"
    df["ingested_at"] = utc_now()
    return df[PRICE_COLUMNS]


def statements_to_long(statements: dict[str, pd.DataFrame], ticker_symbol: str) -> pd.DataFrame:
    """Convert yfinance statements (line items x report dates) to a long table:
    ticker | report_date | statement_type | line_item | value | source | ingested_at

    A long format avoids schema drift between companies and statement types.
    """
    frames = []
    for statement_type, df in statements.items():
        if df is None or df.empty:
            print(f"  no {statement_type} data for {ticker_symbol}")
            continue
        wide = df.copy()
        wide.columns = [str(c) for c in wide.columns]  # Timestamp labels break melt in some pandas versions
        long_df = (
            wide.rename_axis("line_item").reset_index()
            .melt(id_vars="line_item", var_name="report_date", value_name="value")
        )
        long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
        long_df = long_df.dropna(subset=["value"])
        long_df["statement_type"] = statement_type
        frames.append(long_df)

    columns = ["ticker", "report_date", "statement_type", "line_item", "value", "source", "ingested_at"]
    if not frames:
        return pd.DataFrame(columns=columns)

    out = pd.concat(frames, ignore_index=True)
    out["report_date"] = pd.to_datetime(out["report_date"]).dt.tz_localize(None)
    out["ticker"] = ticker_symbol
    out["source"] = "yfinance"
    out["ingested_at"] = utc_now()
    return out[columns]


def collect_price_data(ticker_symbol: str) -> bool:
    print(f"Downloading price data for {ticker_symbol}...")
    ticker = yf.Ticker(ticker_symbol)
    df = with_retry(ticker.history, start=START_DATE, end=END_DATE, auto_adjust=False, actions=True)
    if df is None or df.empty:
        print(f"  no data returned for {ticker_symbol}")
        return False

    df = standardize_prices(df, ticker_symbol)
    out_dir = BASE_DIR / "stock_prices"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{ticker_symbol}.parquet"
    df.to_parquet(out_file, index=False)
    print(f"  saved {out_file} ({len(df)} rows)")
    return True


def collect_company_metadata(ticker_symbol: str) -> dict:
    ticker = yf.Ticker(ticker_symbol)
    try:
        info = with_retry(lambda: ticker.info)
    except Exception as exc:  # noqa: BLE001
        print(f"  metadata request failed for {ticker_symbol}: {exc}")
        info = {}
    market_cap = info.get("marketCap")
    return {
        "ticker": ticker_symbol,
        "company_name": info.get("longName"),
        "sector": info.get("sector") or SECTOR_FALLBACK.get(ticker_symbol, "Unknown"),
        "industry": info.get("industry"),
        "country": info.get("country"),
        "currency": info.get("currency"),
        "market_cap": float(market_cap) if market_cap is not None else None,
        "source": "yfinance",
        "ingested_at": utc_now(),
    }


def collect_metadata() -> None:
    records = []
    for ticker_symbol in TICKERS:
        print(f"Collecting metadata for {ticker_symbol}...")
        records.append(collect_company_metadata(ticker_symbol))
    df = pd.DataFrame(records)
    out_dir = BASE_DIR / "company_metadata"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "company_metadata.parquet"
    df.to_parquet(out_file, index=False)
    print(f"  saved {out_file}")


def collect_financial_statements(ticker_symbol: str) -> bool:
    print(f"Collecting financial statements for {ticker_symbol}...")
    ticker = yf.Ticker(ticker_symbol)
    statements = {
        "income_statement": with_retry(lambda: ticker.income_stmt),
        "balance_sheet": with_retry(lambda: ticker.balance_sheet),
        "cash_flow": with_retry(lambda: ticker.cashflow),
    }
    df = statements_to_long(statements, ticker_symbol)
    if df.empty:
        return False
    out_dir = BASE_DIR / "financials"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{ticker_symbol}_financials.parquet"
    df.to_parquet(out_file, index=False)
    print(f"  saved {out_file} ({len(df)} rows)")
    return True


def main() -> int:
    print("Starting financial data ingestion...")
    failures = []
    for ticker_symbol in TICKERS:
        try:
            if not collect_price_data(ticker_symbol):
                failures.append(ticker_symbol)
            collect_financial_statements(ticker_symbol)
        except Exception as exc:  # noqa: BLE001 - one bad ticker must not stop the rest
            print(f"  FAILED {ticker_symbol}: {exc}")
            failures.append(ticker_symbol)
    collect_metadata()

    if len(failures) == len(TICKERS):
        print("All tickers failed - aborting with error.")
        return 1
    if failures:
        print(f"Completed with failures for: {sorted(set(failures))}")
    else:
        print("Ingestion completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
