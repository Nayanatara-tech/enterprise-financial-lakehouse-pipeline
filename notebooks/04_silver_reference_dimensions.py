# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC ###Silver accounts

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE finance.silver_accounts as
# MAGIC select distinct 
# MAGIC AccountID,
# MAGIC CustomerID,
# MAGIC AccountType,
# MAGIC current_timestamp() as Load_timestamp
# MAGIC from finance.bronze_accounts;
# MAGIC
# MAGIC select * from finance.silver_accounts;

# COMMAND ----------

# MAGIC %md
# MAGIC ###silver cost centers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE finance.silver_cost_centers as
# MAGIC select distinct 
# MAGIC CostCenter,
# MAGIC Department,
# MAGIC current_timestamp() as Load_timestamp
# MAGIC from finance.bronze_cost_centers;
# MAGIC     
# MAGIC SELECT * FROM finance.silver_cost_centers;

# COMMAND ----------

# MAGIC %md
# MAGIC ###silve exchange rates

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE finance.silver_exchange_rates as
# MAGIC select distinct 
# MAGIC Currency,
# MAGIC USD_Rate,
# MAGIC current_timestamp() as Load_timestamp
# MAGIC from finance.bronze_exchange_rates;
# MAGIC     
# MAGIC SELECT * FROM finance.silver_exchange_rates;

# COMMAND ----------

# MAGIC %md
# MAGIC ###enriched silver fact table for analytical queries
# MAGIC
# MAGIC - This project uses the current active customer version for enrichment.
# MAGIC - A production implementation could use a point-in-time SCD2 join
# MAGIC - based on TransactionDate and EffectiveFrom/EffectiveTo.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE finance.silver_transactions_enriched AS
# MAGIC SELECT
# MAGIC     t.TransactionID,
# MAGIC     t.TransactionDate,
# MAGIC     t.Amount,
# MAGIC     t.Currency,
# MAGIC     t.BusinessUnit,
# MAGIC     t.CostCenter,
# MAGIC
# MAGIC     cc.Department AS CostCenterDepartment,
# MAGIC
# MAGIC     a.AccountType,
# MAGIC
# MAGIC     r.USD_Rate AS ExchangeRate,
# MAGIC
# MAGIC     ROUND(t.Amount * r.USD_Rate, 2) AS AmountUSD,
# MAGIC
# MAGIC     c.CustomerSK,
# MAGIC     c.CustomerID,
# MAGIC     c.CustomerName,
# MAGIC     c.Region
# MAGIC
# MAGIC FROM finance.silver_transactions_staging t
# MAGIC
# MAGIC LEFT JOIN finance.silver_cost_centers cc
# MAGIC     ON t.CostCenter = cc.CostCenter
# MAGIC
# MAGIC LEFT JOIN finance.silver_accounts a
# MAGIC     ON t.AccountID = a.AccountID
# MAGIC     
# MAGIC LEFT JOIN finance.silver_customers_scd2 c
# MAGIC     ON a.CustomerID = c.CustomerID
# MAGIC    AND c.CurrentFlag = 'Y'
# MAGIC
# MAGIC LEFT JOIN finance.silver_exchange_rates r
# MAGIC     ON t.Currency = r.Currency
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finance.silver_transactions_enriched