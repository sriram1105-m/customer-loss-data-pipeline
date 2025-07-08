CREATE SCHEMA IF NOT EXISTS silver_validated;

CREATE OR REPLACE TABLE silver_validated.customer_loss_strict AS
SELECT *,

-- Row Hash / Row Identifier
MD5(CONCAT(
  COALESCE(CAST(LossDate AS STRING), ''),
  COALESCE(CAST(AccountStartDate AS STRING), ''),
  COALESCE(CAST(OffsetValue AS STRING), ''),
  COALESCE(CustomerType, ''),
  COALESCE(MosaicGroup, '')
  )) AS row_hash,

-- Validation Status and Reason
CASE 
  WHEN is_invalid_date_sequence = 0
       AND is_invalid_offset = 0
       AND has_null_critical_field = 0
       AND has_null_demographic_info = 0
  THEN 'VALID'
  ELSE 'INVALID'
END AS validation_status,

CASE 
  WHEN is_invalid_date_sequence = 1 THEN 'Bad Date Sequence'
  WHEN is_invalid_offset = 1 THEN 'Invalid Offset Value'
  WHEN has_null_critical_field = 1 THEN 'Mising Critical Fields'
  WHEN has_null_demographic_info = 1 THEN 'Missing Demographics'
  ELSE 'None'
END AS rejection_reason,

CASE 
  WHEN is_invalid_date_sequence = 1 THEN 'R001'
  WHEN is_invalid_offset = 1 THEN 'R002'
  WHEN has_null_critical_field = 1 THEN 'R003'
  WHEN has_null_demographic_info = 1 THEN 'R004'
  ELSE NULL 
END AS rejection_code,

-- Row Confidence Score
ROUND(
  (1
    - (is_invalid_date_sequence * 0.25)
    - (is_invalid_offset * 0.25)
    - (has_null_critical_field * 0.25)
    - (has_null_demographic_info * 0.25)
    ), 2
) AS row_confidence_score,

-- Metadata / Audit Trail
CURRENT_DATE() AS processed_date,
'customer_loss_strict' AS table_version,
'v1.0' AS schema_version

FROM silver.customer_loss_clean
WHERE 
  is_invalid_date_sequence = 0
  AND is_invalid_offset = 0
  AND has_null_critical_field = 0
  AND has_null_demographic_info = 0;
