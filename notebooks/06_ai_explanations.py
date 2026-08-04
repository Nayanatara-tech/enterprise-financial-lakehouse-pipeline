# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC ###Anamoly Detection

# COMMAND ----------

# MAGIC %md
# MAGIC ###z-score anomaly detection
# MAGIC The z-score formula
# MAGIC
# MAGIC (AmountUSD - mean_amt) / std_amt
# MAGIC
# MAGIC This is:
# MAGIC
# MAGIC z=x-u/o
# MAGIC
# MAGIC Where:
# MAGIC x = transaction amount
# MAGIC μ = mean
# MAGIC σ = standard deviation
# MAGIC
# MAGIC transactions whose amount is many standard deviations away from the average.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE finance.gold_transaction_anomalies AS
# MAGIC WITH stats AS (
# MAGIC   SELECT
# MAGIC       AVG(AmountUSD) AS mean_amt,
# MAGIC       STDDEV(AmountUSD) AS std_amt
# MAGIC   FROM finance.silver_transactions_enriched
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     t.*,
# MAGIC     ROUND((t.AmountUSD - s.mean_amt) / s.std_amt, 2) AS z_score
# MAGIC FROM finance.silver_transactions_enriched t
# MAGIC CROSS JOIN stats s
# MAGIC WHERE ABS((t.AmountUSD - s.mean_amt) / s.std_amt) > 2.1;
# MAGIC
# MAGIC
# MAGIC select * from finance.gold_transaction_anomalies

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE finance.gold_anomaly_explanations AS
# MAGIC SELECT
# MAGIC   TransactionID,
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     concat(
# MAGIC       'You are a senior finance risk analyst. ',
# MAGIC         'Provide ONE concise business insight (max 25 words). ',
# MAGIC         'Avoid repeating the z-score definition. ',
# MAGIC         'Focus on what makes the transaction operationally or financially unusual. ',
# MAGIC         'Focus on concentration, currency exposure, customer exposure, and review priorities.',
# MAGIC         'Data: ',
# MAGIC       to_json(named_struct(
# MAGIC         'TransactionID', TransactionID,
# MAGIC         'Customer', CustomerName,
# MAGIC         'AmountUSD', AmountUSD,
# MAGIC         'Currency', Currency,
# MAGIC         'ZScore', z_score,
# MAGIC         'TransactionDate',TransactionDate,
# MAGIC         'BusinessUnit',BusinessUnit,
# MAGIC         'CostCenter',CostCenter,
# MAGIC         'CostCenterDepartment',CostCenterDepartment,
# MAGIC         'AccountType',AccountType,
# MAGIC         'ExchangeRate',ExchangeRate,
# MAGIC         'Amount',Amount,
# MAGIC         'Region',Region,
# MAGIC         'CustomerName',CustomerName
# MAGIC       ))
# MAGIC     )
# MAGIC   ) AS explanation
# MAGIC FROM finance.gold_transaction_anomalies;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM finance.gold_anomaly_explanations

# COMMAND ----------

# MAGIC %md
# MAGIC ###Executive summary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE finance.gold_anomaly_summary AS
# MAGIC SELECT
# MAGIC     COUNT(*) AS anomaly_count,
# MAGIC     ROUND(SUM(AmountUSD),2) AS total_anomalous_amount,
# MAGIC     ROUND(AVG(AmountUSD),2) AS avg_anomalous_amount,
# MAGIC     ROUND(MAX(AmountUSD),2) AS max_anomalous_amount,
# MAGIC     COUNT(DISTINCT CustomerName) AS affected_customers,
# MAGIC     COUNT(DISTINCT Currency) AS affected_currencies,
# MAGIC     COUNT(DISTINCT BusinessUnit) AS affected_business_units
# MAGIC FROM finance.gold_transaction_anomalies;
# MAGIC
# MAGIC select * from finance.gold_anomaly_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW anomaly_top_customers AS
# MAGIC SELECT
# MAGIC     CustomerName,
# MAGIC     COUNT(*) AS anomaly_txns,
# MAGIC     ROUND(SUM(AmountUSD),2) AS anomaly_amount
# MAGIC FROM finance.gold_transaction_anomalies
# MAGIC GROUP BY CustomerName
# MAGIC ORDER BY anomaly_amount DESC;
# MAGIC
# MAGIC select * from anomaly_top_customers
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW anomaly_currency_mix AS
# MAGIC SELECT
# MAGIC     Currency,
# MAGIC     COUNT(*) AS txn_count,
# MAGIC     ROUND(SUM(AmountUSD),2) AS total_amount
# MAGIC FROM finance.gold_transaction_anomalies
# MAGIC GROUP BY Currency
# MAGIC ORDER BY total_amount DESC;
# MAGIC
# MAGIC select * from anomaly_currency_mix

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW anomaly_bu_mix AS
# MAGIC SELECT
# MAGIC     BusinessUnit,
# MAGIC     COUNT(*) AS txn_count,
# MAGIC     ROUND(SUM(AmountUSD),2) AS total_amount
# MAGIC FROM finance.gold_transaction_anomalies
# MAGIC GROUP BY BusinessUnit
# MAGIC ORDER BY total_amount DESC;
# MAGIC
# MAGIC select * from anomaly_bu_mix

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE finance.gold_anomaly_executive_summary AS
# MAGIC SELECT
# MAGIC   current_timestamp() AS GeneratedAt,
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     concat(
# MAGIC       'You are a senior finance risk analyst preparing a management summary. ',
# MAGIC       'Generate exactly 5 concise bullet points (each under 18 words). ',
# MAGIC       'Do not repeat wording. ',
# MAGIC       'Focus on customer concentration, currency exposure, business-unit concentration, largest anomaly, and review priorities. ',
# MAGIC       'Overall summary: ',
# MAGIC       (SELECT to_json(named_struct(
# MAGIC           'anomaly_count', anomaly_count,
# MAGIC           'total_anomalous_amount', total_anomalous_amount,
# MAGIC           'avg_anomalous_amount', avg_anomalous_amount,
# MAGIC           'max_anomalous_amount', max_anomalous_amount,
# MAGIC           'affected_customers', affected_customers,
# MAGIC           'affected_currencies', affected_currencies,
# MAGIC           'affected_business_units', affected_business_units
# MAGIC       )) FROM finance.gold_anomaly_summary),
# MAGIC       '. Top customers: ',
# MAGIC       (SELECT to_json(collect_list(named_struct(
# MAGIC           'customer', CustomerName,
# MAGIC           'transactions', anomaly_txns,
# MAGIC           'amount', anomaly_amount
# MAGIC       ))) FROM anomaly_top_customers),
# MAGIC       '. Currency mix: ',
# MAGIC       (SELECT to_json(collect_list(named_struct(
# MAGIC           'currency', Currency,
# MAGIC           'transactions', txn_count,
# MAGIC           'amount', total_amount
# MAGIC       ))) FROM anomaly_currency_mix),
# MAGIC       '. Business-unit mix: ',
# MAGIC       (SELECT to_json(collect_list(named_struct(
# MAGIC           'business_unit', BusinessUnit,
# MAGIC           'transactions', txn_count,
# MAGIC           'amount', total_amount
# MAGIC       ))) FROM anomaly_bu_mix)
# MAGIC     )
# MAGIC   ) AS ExecutiveInsights;
# MAGIC
# MAGIC select * from finance.gold_anomaly_executive_summary;