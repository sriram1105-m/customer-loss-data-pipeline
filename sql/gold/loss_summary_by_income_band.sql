CREATE OR REPLACE TABLE gold.loss_summary_by_income_band AS
WITH base AS (
    SELECT
      IncomeBand,
      COUNT(*) AS total_customers,
      SUM(CASE WHEN Loss = 1 THEN 1 ELSE 0 END) AS loss_customers,
      ROUND(100.0 * SUM(CASE WHEN Loss = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS loss_rate_percent,
      ROUND(AVG(OffsetValue), 2) AS avg_offset_value,
      ROUND(AVG(CASE WHEN Loss = 1 THEN OffsetValue ELSE NULL END), 2) AS avg_loss_offset_value,
      SUM(OffsetValue) AS total_offset_value,
      SUM(CASE WHEN Loss = 1 THEN OffsetValue ELSE 0 END) AS total_loss_offset_value
    FROM silver_validated.customer_loss_strict
    GROUP BY IncomeBand
)
SELECT
  IncomeBand,
  total_customers,
  loss_customers,
  loss_rate_percent,
  avg_offset_value,
  avg_loss_offset_value,
  total_offset_value,
  total_loss_offset_value,
  ROUND((loss_customers / total_customers) * avg_loss_offset_value, 2) AS loss_risk_score,
  CURRENT_DATE AS processed_date,
  'loss_summary_by_income_band' AS table_version,
  'v1.0' AS schema_version
FROM base 
ORDER BY loss_risk_score DESC;
