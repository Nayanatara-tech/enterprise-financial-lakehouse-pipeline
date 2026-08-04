# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC #### Validation & Quarantine Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create quarantine table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS finance.quarantine_transactions (
# MAGIC     TransactionID STRING,
# MAGIC     AccountID STRING,
# MAGIC     TransactionDate DATE,
# MAGIC     Amount DOUBLE,
# MAGIC     Currency STRING,
# MAGIC     TransactionType STRING,
# MAGIC     CostCenter STRING,
# MAGIC     BusinessUnit STRING,
# MAGIC     IngestionTime TIMESTAMP,
# MAGIC     SourceFile STRING,
# MAGIC     RunID STRING,
# MAGIC     ErrorReason STRING,
# MAGIC     QuarantinedAt TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read Bronze tables

# COMMAND ----------

from pyspark.sql.functions import *
tx=spark.table("finance.bronze_transactions")
accounts=spark.table("finance.bronze_accounts")
rates=spark.table("finance.bronze_exchange_rates")

display(tx.count())
display(accounts.count())
display(rates.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Validation 1: NULL Amount

# COMMAND ----------

null_amount=tx.filter(col("Amount").isNull()).withColumn("ErrorReason",lit("NULL Amount")).withColumn("QuarantinedAt",current_timestamp())
display(null_amount)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Validation 2: Invalid Currency - referential integrity

# COMMAND ----------

invalid_currency=tx.join(rates,rates.Currency==tx.Currency,"leftanti").withColumn("ErrorReason",lit("Invalid Currency")).withColumn("QuarantinedAt",current_timestamp())
display(invalid_currency)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Validation 3: Invalid accounts - Referential _Integrity

# COMMAND ----------

invalid_accounts=tx.join(accounts,accounts.AccountID==tx.AccountID,"leftanti").withColumn("ErrorReason",lit("Invalid Accounts")).withColumn("QuarantinedAt",current_timestamp())
display(invalid_accounts)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Validation 4: Future Dates

# COMMAND ----------

invalid_dates=tx.filter(col("TransactionDate")>current_date()).withColumn("ErrorReason",lit("Future Date")).withColumn("QuarantinedAt",current_timestamp())
display(invalid_dates)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Validation 5: Duplicate TransactionID

# COMMAND ----------

from pyspark.sql.window import Window

w=Window.partitionBy("TransactionID").orderBy(col("IngestionTime").desc())

Dup_transactions=tx.withColumn("rn",row_number().over(w))\
    .filter(col("rn")>1)\
    .drop("rn")\
    .withColumn("ErrorReason", lit("DUPLICATE_TRANSACTION_ID"))\
    .withColumn("QuarantinedAt", current_timestamp())\

display(Dup_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Combine all bad records

# COMMAND ----------

quarantine_df=null_amount.unionByName(invalid_currency).unionByName(invalid_accounts).unionByName(invalid_dates).unionByName(Dup_transactions)
display(quarantine_df)

# COMMAND ----------

(quarantine_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("finance.quarantine_transactions"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM finance.quarantine_transactions

# COMMAND ----------

# MAGIC %md
# MAGIC ###Save valid transaction records into silver layer

# COMMAND ----------

invalid_ids=quarantine_df.select("TransactionID").distinct()

valid_records=tx.join(invalid_ids,tx.TransactionID==invalid_ids.TransactionID,"leftanti")

print("Valid records:", valid_records.count())
print("Quarantined records:", quarantine_df.count())

(valid_records.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("finance.silver_transactions_staging"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###DQ Metrics

# COMMAND ----------

dq_metrics = spark.createDataFrame([
    ("TOTAL_RECORDS", tx.count()),
    ("NULL_AMOUNT", null_amount.count()),
    ("INVALID_CURRENCY", invalid_currency.count()),
    ("INVALID_ACCOUNT", invalid_accounts.count()),
    ("FUTURE_DATE", invalid_dates.count()),
    ("DUPLICATE_TRANSACTION_ID", Dup_transactions.count()),
    ("VALID_RECORDS", valid_records.count())
], ["Metric", "Count"])

display(dq_metrics)