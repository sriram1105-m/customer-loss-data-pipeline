-- Creating a Schema for Silver Table
CREATE SCHEMA IF NOT EXISTS silver;

-- Creating Silver Table 
CREATE OR REPLACE TABLE silver.customer_loss_clean AS
SELECT 
  -- Type Casting and Fixing
  TRY_CAST(LossDate AS DATE) AS LossDate,
  TRY_CAST(AccountStartDate AS DATE) AS AccountStartDate,
  TRY_CAST(OffsetValue AS DOUBLE) AS OffsetValue,
  CAST(Loss AS INT) AS Loss,

-- Categorical Cleanup (Normalize casing, Default Handling)
  CASE 
    WHEN LOWER(InstallmentPlan) IN ('yes', 'y') THEN 'Yes'
    WHEN LOWER(InstallmentPlan) IN ('no', 'n') THEN 'No'
    ELSE 'Unknown'
  END AS InstallmentPlan,

  INITCAP(AccountDeterminationGroup) AS AccountDeterminationGroup,
  INITCAP(CustomerCategory) AS CustomerCategory,
  INITCAP(CustomerType) AS CustomerType,
  COALESCE(ComplaintType, 'None') AS ComplaintType,
  INITCAP(StatusGrouping) AS StatusGrouping,
  INITCAP(PreferredContactMethod) AS PreferredContactMethod,

  MosaicGroup,
  MosaicType,
  MosaicTypeDesc,
  MosaicDigitalGroup,
  MosaicDigitalGroupDesc,
  MosaicFSSGroup,
  MosaicFSSType,
  INITCAP(MosaicResidenceType) AS MosaicResidenceType,
  TRY_CAST(MosaicHouseholdIncValue AS INT) AS MosaicHouseholdIncValue,
  TRY_CAST(MosaicNoAdultsHousehold AS INT) AS MosaicNoAdultsHousehold,
  INITCAP(OfgemCodeRisk) AS OfgemCodeRisk,
  INITCAP(PaymentFrequency) AS PaymentFrequency,
  INITCAP(DebtPlanGroup) AS DebtPlanGroup,

-- Derived / Enriched Business Fields
  DATEDIFF(TRY_CAST(LossDate AS Date), TRY_CAST(AccountStartdate AS DATE)) AS AccountAgeDays,

  CASE 
    WHEN MosaicHouseholdIncValue >= 100000 THEN 'High Income'
    WHEN MosaicHouseholdIncValue >= 50000 THEN 'Mid Income'
    WHEN MosaicHouseholdIncValue IS NULL THEN 'Unknown'
    ELSE 'Low Income'
  END AS IncomeBand,

  CASE 
    WHEN MosaicFSSGroup IN ('F', 'E') AND MosaicHouseholdIncValue < 30000 THEN 'Yes'
    ELSE 'No'
  END AS IsHighRiskCustomer,

-- DQ Validation Flags
 CASE 
   WHEN LossDate IS NULL OR AccountStartdate IS NULL THEN 1
   WHEN TRY_CAST(LossDate AS DATE) < TRY_CAST(AccountStartdate AS DATE) THEN 1
   ELSE 0
 END AS is_invalid_date_sequence,

 CASE 
   WHEN OffsetValue IS NULL OR OffsetValue < 0 THEN 1
   ELSE 0
 END AS is_invalid_offset,

 CASE 
   WHEN ComplaintType IS NULL OR InstallmentPlan IS NULL THEN 1
   ELSE 0
  END AS has_null_critical_field,

 CASE 
   WHEN ComplaintType IS NULL OR MosaicFSSGroup IS NULL THEN 1
   ELSE 0
 END AS has_null_demographic_info

FROM bronze.customer_loss_raw
WHERE 
  Loss IN (0, 1);










