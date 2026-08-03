# Databricks notebook source
# MAGIC %md
# MAGIC ###Delta History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY finance_dev.silver_customers_scd2;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Time Travel
# MAGIC
# MAGIC Delta Lake allows querying historical snapshots of a table using VERSION AS OF or TIMESTAMP AS OF. This is useful for audit, debugging, recovery, and reproducible analytics.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finance_dev.silver_customers_scd2 version as of 3

# COMMAND ----------

# MAGIC %md
# MAGIC ###Partition pruning
# MAGIC
# MAGIC The query scans only the partition for 2025-07-01 instead of the entire Bronze table, reducing file reads and improving performance.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL finance_dev.bronze_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN
# MAGIC SELECT *
# MAGIC FROM finance_dev.bronze_transactions
# MAGIC WHERE TransactionDate = '2025-07-01';

# COMMAND ----------

# MAGIC %md
# MAGIC ###OPTIMIZE + ZORDER
# MAGIC
# MAGIC OPTIMIZE compacts many small Delta files into larger files. ZORDER colocates related values (CustomerID, TransactionDate) to reduce file scanning for selective queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE finance_dev.silver_transactions_enriched
# MAGIC ZORDER BY (CustomerID, TransactionDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ###Broadcast join
# MAGIC
# MAGIC Small dimension tables are broadcast to executors, avoiding expensive shuffle joins and improving performance in star-schema enrichments.

# COMMAND ----------

from pyspark.sql.functions import broadcast

txn = spark.table("finance_dev.silver_transactions_staging")
acc = spark.table("finance_dev.silver_accounts")
fx = spark.table("finance_dev.silver_exchange_rates")

broadcast_demo = (
    txn.join(broadcast(acc), "AccountID", "left")
       .join(broadcast(fx), "Currency", "left")
)

display(broadcast_demo.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ###VACUUM
# MAGIC
# MAGIC VACUUM removes obsolete data files that are no longer referenced by the Delta transaction log. Retaining 168 hours (7 days) preserves recent time-travel capability.

# COMMAND ----------

# MAGIC %sql
# MAGIC --VACUUM finance_dev.silver_customers_scd2 RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Change Data Feed (no implementation)
# MAGIC
# MAGIC
# MAGIC ALTER TABLE finance_dev.silver_customers_scd2
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC
# MAGIC Explanation:
# MAGIC
# MAGIC exposes inserts/updates/deletes between versions
# MAGIC
# MAGIC useful for incremental downstream processing
# MAGIC
# MAGIC not implemented in this project