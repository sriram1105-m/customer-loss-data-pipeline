CREATE OR REPLACE TABLE gold.loss_summary_by_mosaic_group AS
WITH base AS (
  SELECT
    MosaicGroup,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN Loss = 1 THEN 1 ELSE 0 END) AS loss_customers,
    ROUND(100.0 * SUM(CASE WHEN Loss = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS loss_rate_percent,
    ROUND(AVG(CASE WHEN Loss = 1 THEN OffsetValue ELSE NULL END), 2) AS avg_loss_offset_value,
    SUM(CASE WHEN Loss = 1 THEN OffsetValue ELSE 0 END) AS total_loss_offset_value
  FROM silver_validated.customer_loss_strict
  WHERE MosaicGroup IS NOT NULL
  GROUP BY MosaicGroup
)

SELECT
  MosaicGroup,
  total_customers,
  loss_customers,
  loss_rate_percent,
  avg_loss_offset_value,
  total_loss_offset_value,

  -- Composite business risk index
  ROUND(
    (loss_customers / total_customers) * avg_loss_offset_value,
    2
  ) AS loss_risk_score,

  -- Metadata
  CURRENT_DATE() AS processed_date,
  'loss_summary_by_mosaic_group' AS table_version,
  'v1.0' AS schema_version

FROM base
ORDER BY loss_risk_score DESC;
