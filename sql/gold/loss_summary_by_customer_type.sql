CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.loss_summary_by_customer_type AS
WITH base AS (
    SELECT 
      CustomerType,
      COUNT(*) AS total_customers,
      SUM(CASE WHEN Loss = 1 THEN 1 ELSE 0 END) AS loss_customers,
      SUM(OffsetValue) AS total_offset_value,
      SUM(CASE WHEN Loss = 1 THEN OffsetValue ELSE 0 END) as total_loss_offset_value,
      ROUND(AVG(OffsetValue), 2) AS avg_offset_value,
      ROUND(AVG(CASE WHEN Loss = 1 THEN OffsetValue ELSE NULL END), 2) AS avg_loss_offset_value
    FROM silver_validated.customer_loss_strict
    GROUP BY CustomerType
)

SELECT
  CustomerType,
  total_customers,
  loss_customers,
  ROUND(100.0 * loss_customers / total_customers, 2) AS loss_rate_percent,
  total_offset_value,
  total_loss_offset_value,
  avg_offset_value,
  avg_loss_offset_value,
  ROUND((loss_customers / total_customers) * avg_loss_offset_value, 2) AS loss_risk_score,

-- Metadata
  CURRENT_DATE() AS processed_date,
  'loss_summary_by_customer_type' AS table_version,
  'v1.0' AS schema_version

FROM base
ORDER BY loss_risk_score DESC;gold.loss_summary_by_customer_type
