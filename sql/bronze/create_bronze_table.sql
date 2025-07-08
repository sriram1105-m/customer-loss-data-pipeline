CREATE OR REPLACE TABLE bronze.customer_loss_raw
USING CSV
OPTIONS (
  header = "true",
  inferSchema = "true",
  delimiter = ","
)
LOCATION 'dbfs:/FileStore/tables/SP_Loss_Data_450k.csv';
