# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC ###Silver Customer Dimension (SCD Type 2)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS finance.silver_customers_scd2 (
# MAGIC     CustomerSK BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     CustomerID STRING,
# MAGIC     CustomerName STRING,
# MAGIC     Region STRING,
# MAGIC     EffectiveFrom TIMESTAMP,
# MAGIC     EffectiveTo TIMESTAMP,
# MAGIC     CurrentFlag STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finance.bronze_customers

# COMMAND ----------

from pyspark.sql.functions import *
src = spark.table("finance.bronze_customers").select(
    "CustomerID", "CustomerName", "Region"
)

current_target = (spark.table("finance.silver_customers_scd2")
                  .filter(col("CurrentFlag") == "Y"))

# New customers
new_rows = (
    src.alias("s")
    .join(current_target.select("CustomerID").alias("t"), "CustomerID", "left_anti")
)

# Changed existing customers
changed_rows = (
    src.alias("s")
    .join(current_target.alias("t"), "CustomerID", "inner")
    .filter(
        (col("s.CustomerName") != col("t.CustomerName")) |
        (col("s.Region") != col("t.Region"))
    )
    .select("s.*")
)

display(new_rows)
display(changed_rows)

# COMMAND ----------

# For changed rows: create UPDATE row
staged_for_update = changed_rows.withColumn("MergeKey", col("CustomerID"))

# For changed rows: create INSERT row
staged_for_changed_insert = changed_rows.withColumn("MergeKey", lit(None).cast("string"))

# For new rows: only INSERT row
staged_for_new_insert = new_rows.withColumn("MergeKey", lit(None).cast("string"))

staged_updates = (
    staged_for_update
    .unionByName(staged_for_changed_insert)
    .unionByName(staged_for_new_insert)
)

staged_updates.createOrReplaceTempView("staged_updates")
display(staged_updates)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO finance.silver_customers_scd2 AS target
# MAGIC USING staged_updates AS source
# MAGIC ON target.CustomerID = source.MergeKey
# MAGIC AND target.CurrentFlag = 'Y'
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.CurrentFlag = 'N',
# MAGIC     target.EffectiveTo = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     CustomerID,
# MAGIC     CustomerName,
# MAGIC     Region,
# MAGIC     EffectiveFrom,
# MAGIC     EffectiveTo,
# MAGIC     CurrentFlag
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.CustomerID,
# MAGIC     source.CustomerName,
# MAGIC     source.Region,
# MAGIC     current_timestamp(),
# MAGIC     NULL,
# MAGIC     'Y'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finance.silver_customers_scd2