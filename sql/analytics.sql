-- ===== Analytics queries on the Gold layer =====

-- 1. Latest close per ticker
SELECT ticker, trade_date, close
FROM finance_lakehouse.gold.daily_returns
QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) = 1
ORDER BY ticker;

-- 2. Top-performing stocks, last 365 days (average daily return)
SELECT ticker, AVG(daily_return) AS avg_daily_return
FROM finance_lakehouse.gold.daily_returns
WHERE trade_date >= DATE_SUB(CURRENT_DATE(), 365)
GROUP BY ticker
ORDER BY avg_daily_return DESC;

-- 3. Most volatile stocks, last 365 days
SELECT ticker, AVG(annualized_volatility) AS avg_volatility
FROM finance_lakehouse.gold.daily_returns
WHERE trade_date >= DATE_SUB(CURRENT_DATE(), 365)
GROUP BY ticker
ORDER BY avg_volatility DESC;

-- 4. Sector performance, last 365 days (equal-weighted)
SELECT sector,
       AVG(average_daily_return) AS avg_return,
       AVG(average_volatility)   AS avg_volatility
FROM finance_lakehouse.gold.sector_performance
WHERE trade_date >= DATE_SUB(CURRENT_DATE(), 365)
GROUP BY sector
ORDER BY avg_return DESC;

-- 5. Stocks trading above their 50-day moving average on the latest date
SELECT ticker, trade_date, close, ma_50
FROM finance_lakehouse.gold.daily_returns
WHERE trade_date = (SELECT MAX(trade_date) FROM finance_lakehouse.gold.daily_returns)
  AND close > ma_50
ORDER BY ticker;

-- 6. High-volatility observations (annualized > 40%)
SELECT ticker, trade_date, annualized_volatility
FROM finance_lakehouse.gold.daily_returns
WHERE annualized_volatility > 0.40
ORDER BY annualized_volatility DESC
LIMIT 100;

-- 7. Trailing 1-year price return with explicit start/end dates
WITH bounds AS (
  SELECT MAX(trade_date) AS end_date, DATE_SUB(MAX(trade_date), 365) AS start_target
  FROM finance_lakehouse.gold.daily_returns
),
start_prices AS (   -- first trading day on/after the start target
  SELECT ticker, trade_date, adjusted_close,
         ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date) AS rn
  FROM finance_lakehouse.gold.daily_returns, bounds
  WHERE trade_date >= start_target
),
end_prices AS (
  SELECT d.ticker, d.trade_date, d.adjusted_close
  FROM finance_lakehouse.gold.daily_returns d, bounds b
  WHERE d.trade_date = b.end_date
)
SELECT s.ticker,
       s.trade_date AS start_date,
       e.trade_date AS end_date,
       ROUND(e.adjusted_close / s.adjusted_close - 1, 4) AS one_year_return
FROM start_prices s
JOIN end_prices e USING (ticker)
WHERE s.rn = 1
ORDER BY one_year_return DESC;

-- 8. Risk / return summary per ticker
SELECT ticker, ROUND(total_return, 3) AS total_return, ROUND(annualized_return, 3) AS ann_return,
       ROUND(annualized_volatility_full, 3) AS ann_vol, ROUND(max_drawdown, 3) AS max_drawdown,
       ROUND(return_to_risk, 2) AS return_to_risk
FROM finance_lakehouse.gold.volatility_metrics
ORDER BY return_to_risk DESC;

-- 9. Golden-cross style trend signal on the latest date
SELECT ticker, trend_signal, ROUND(ma_20, 2) AS ma_20, ROUND(ma_50, 2) AS ma_50
FROM finance_lakehouse.gold.daily_returns
WHERE trade_date = (SELECT MAX(trade_date) FROM finance_lakehouse.gold.daily_returns)
ORDER BY ticker;

-- 10. Latest fundamentals per company
SELECT ticker, company_name, report_date, revenue, net_income,
       ROUND(net_margin, 3) AS net_margin, ROUND(debt_to_equity, 2) AS debt_to_equity,
       ROUND(return_on_equity, 3) AS roe
FROM finance_lakehouse.gold.company_fundamentals
QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY report_date DESC) = 1
ORDER BY revenue DESC;

-- 11. Sector index (growth of 1 unit, equal-weighted)
SELECT trade_date, sector, sector_index
FROM finance_lakehouse.gold.sector_performance
ORDER BY trade_date, sector;

-- 12. Worst drawdown days per ticker
SELECT ticker, trade_date, ROUND(drawdown, 3) AS drawdown
FROM finance_lakehouse.gold.daily_returns
QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY drawdown ASC) = 1
ORDER BY drawdown;
