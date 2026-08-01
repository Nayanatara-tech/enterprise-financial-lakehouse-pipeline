# Databricks notebook source
# MAGIC %md
# MAGIC ###Bronze Ingestion
# MAGIC
# MAGIC This notebook performs:
# MAGIC - processed_files table setup
# MAGIC - idempotent file checks
# MAGIC - Bronze ingestion for all source datasets
# MAGIC - audit column enrichment
# MAGIC - operational metadata logging
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS finance_dev.processed_files (
# MAGIC     FileName STRING,
# MAGIC     RunID STRING,
# MAGIC     Layer STRING,
# MAGIC     ProcessedTime TIMESTAMP,
# MAGIC     Status STRING,
# MAGIC     RowCount BIGINT
# MAGIC ) USING DELTA;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
from uuid import uuid4

base_path = "/Volumes/workspace/finance_dev/raw_files"

master_config = {
    "customers": "bronze_customers",
    "accounts": "bronze_accounts",
    "cost_centers": "bronze_cost_centers",
    "exchange_rates": "bronze_exchange_rates"
}

transaction_table = "bronze_transactions"

run_id = str(uuid4())

print(f"RunID: {run_id}")

def process_folder(folder_name, table_name, path):

    file_name = f"{folder_name}.csv"

    existing = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM finance_dev.processed_files
        WHERE FileName = '{file_name}'
          AND Layer = 'BRONZE'
          AND Status = 'SUCCESS'
    """).collect()[0][0]

    if existing > 0:
        print(f"Skipping already processed: {file_name}")
        return

    df = (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(path))

    row_count = df.count()

    bronze_df = (df
        .withColumn("IngestionTime", current_timestamp())
        .withColumn("SourceFile", lit(file_name))
        .withColumn("RunID", lit(run_id)))

    (bronze_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"finance_dev.{table_name}"))

    spark.sql(f"""
        INSERT INTO finance_dev.processed_files
        VALUES (
            '{file_name}',
            '{run_id}',
            'BRONZE',
            current_timestamp(),
            'SUCCESS',
            {row_count}
        )
    """)

    print(f"Loaded {file_name} -> finance_dev.{table_name} ({row_count} rows)")

for source_folder, table_name in master_config.items():

    parent_path = f"{base_path}/master/{source_folder}"

    snapshots = [
        f.name.rstrip("/")
        for f in dbutils.fs.ls(parent_path)
    ]

    for snapshot in snapshots:

        full_path = f"{parent_path}/{snapshot}"

        process_folder(snapshot, table_name, full_path)


transaction_parent = f"{base_path}/transactions"

transaction_folders = [
    f.name.rstrip("/")
    for f in dbutils.fs.ls(transaction_parent)
    if f.name.startswith("transactions_")
]

for txn_folder in transaction_folders:

    full_path = f"{transaction_parent}/{txn_folder}"

    process_folder(txn_folder, transaction_table, full_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM finance_dev.processed_files;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finance_dev.bronze_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from finance_dev.bronze_transactions