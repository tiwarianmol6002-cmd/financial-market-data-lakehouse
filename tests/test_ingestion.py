import pandas as pd

from ingestion.config import PRICE_COLUMNS
from ingestion.market_data import clean_index, standardize_prices, statements_to_long


def _raw_prices(tz=True):
    idx = pd.date_range("2024-01-02", periods=3, freq="D", tz="America/New_York" if tz else None)
    return pd.DataFrame(
        {"Open": [1.0, 2.0, 3.0], "High": [2.0, 3.0, 4.0], "Low": [0.5, 1.5, 2.5],
         "Close": [1.5, 2.5, 3.5], "Adj Close": [1.4, 2.4, 3.4], "Volume": [10, 20, 30],
         "Dividends": [0.0, 0.0, 0.1], "Stock Splits": [0.0, 0.0, 0.0]},
        index=idx,
    )


def test_clean_index_removes_timezone():
    assert clean_index(_raw_prices()).index.tz is None


def test_standardize_prices_schema():
    df = standardize_prices(_raw_prices(), "AAPL")
    assert list(df.columns) == PRICE_COLUMNS
    assert (df["ticker"] == "AAPL").all()
    assert df["trade_date"].dt.tz is None
    assert df["adjusted_close"].iloc[0] == 1.4


def test_standardize_prices_falls_back_when_adj_close_missing():
    raw = _raw_prices().drop(columns=["Adj Close"])
    df = standardize_prices(raw, "MSFT")
    assert (df["adjusted_close"] == df["close"]).all()


def test_statements_to_long_shape_and_nan_removal():
    cols = pd.to_datetime(["2024-09-30", "2023-09-30"])
    income = pd.DataFrame({cols[0]: [100.0, 20.0], cols[1]: [90.0, None]},
                          index=["Total Revenue", "Net Income"])
    out = statements_to_long({"income_statement": income, "balance_sheet": pd.DataFrame()}, "AAPL")
    assert set(out.columns) == {"ticker", "report_date", "statement_type", "line_item",
                                "value", "source", "ingested_at"}
    assert len(out) == 3  # the NaN is dropped
    assert (out["statement_type"] == "income_statement").all()


def test_statements_to_long_empty():
    out = statements_to_long({"income_statement": pd.DataFrame()}, "AAPL")
    assert out.empty
