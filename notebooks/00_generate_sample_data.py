# Databricks notebook source
# MAGIC %md
# MAGIC ###Sample Data Generator

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.finance_dev.raw_files;

# COMMAND ----------

from pyspark.sql import SparkSession
import pandas as pd
import random
from datetime import datetime, timedelta

spark = SparkSession.builder.getOrCreate()

base_path = "/Volumes/workspace/finance_dev/raw_files"

# -----------------------------
# Customers (Dimension Source)
# -----------------------------
customers = pd.DataFrame([
    ["C001", "Qatar Energy", "MiddleEast"],
    ["C002", "Shell Trading", "Europe"],
    ["C003", "LNG Partners", "Asia"],
    ["C004", "PetroChem Global", "MiddleEast"],
    ["C005", "Energy Retail", "Europe"]
], columns=["CustomerID", "CustomerName", "Region"])


# -----------------------------
# Accounts (Dimension Source)
# -----------------------------
accounts = pd.DataFrame([
    ["A100", "C001", "Revenue"],
    ["A200", "C001", "Expense"],
    ["A300", "C002", "Revenue"],
    ["A400", "C003", "Revenue"],
    ["A500", "C004", "Expense"],
    ["A600", "C005", "Revenue"]
], columns=["AccountID", "CustomerID", "AccountType"])


# -----------------------------
# Cost Centers
# -----------------------------
cost_centers = pd.DataFrame([
    ["CC10", "Operations"],
    ["CC20", "Trading"],
    ["CC30", "Logistics"],
    ["CC40", "Finance"]
], columns=["CostCenter", "Department"])


# -----------------------------
# Exchange Rates
# -----------------------------
exchange_rates = pd.DataFrame([
    ["USD", 1.0],
    ["QAR", 0.2747],
    ["EUR", 1.09]
], columns=["Currency", "USD_Rate"])


# -----------------------------
# Transactions (Fact Source)
# -----------------------------
rows = []
start_date = datetime(2025, 7, 1)

for i in range(1, 1001):
    txn_date = start_date + timedelta(days=random.randint(0, 29))
    txn_type = random.choice(["Revenue", "Expense"])

    amount = round(random.uniform(1000, 100000), 2)

    rows.append({
        "TransactionID": f"TXN{i:05d}",
        "AccountID": random.choice(accounts["AccountID"].tolist()),
        "TransactionDate": txn_date.strftime("%Y-%m-%d"),
        "Amount": amount,
        "Currency": random.choice(["USD", "QAR", "EUR"]),
        "TransactionType": txn_type,
        "CostCenter": random.choice(cost_centers["CostCenter"].tolist()),
        "BusinessUnit": random.choice(["GTL_Qatar", "LNG_Qatar", "Trading", "Chemicals"])
    })

# Add bad records intentionally
rows.append({
    "TransactionID": "TXN99999",
    "AccountID": "A100",
    "TransactionDate": "2025-07-15",
    "Amount": None,
    "Currency": "USD",
    "TransactionType": "Revenue",
    "CostCenter": "CC10",
    "BusinessUnit": "GTL_Qatar"
})

rows.append({
    "TransactionID": "TXN99998",
    "AccountID": "A300",
    "TransactionDate": "2025-07-16",
    "Amount": 5000,
    "Currency": "ABC",
    "TransactionType": "Revenue",
    "CostCenter": "CC20",
    "BusinessUnit": "Trading"
})

transactions = pd.DataFrame(rows)

# -----------------------------
# Write helper
# -----------------------------
def write_csv(pdf, relative_path):
    full_path = f"{base_path}/{relative_path}"

    (spark.createDataFrame(pdf)
          .coalesce(1)
          .write
          .mode("overwrite")
          .option("header", True)
          .csv(full_path))

    print(f"Written: {full_path}")

# Write all files
write_csv(customers, "master/customers/customers_2025_07_01")
write_csv(accounts, "master/accounts/accounts_2025_07_01")
write_csv(cost_centers, "master/cost_centers/cost_centers_2025_07_01")
write_csv(exchange_rates, "master/exchange_rates/exchange_rates_2025_07_01")
write_csv(transactions, "transactions/transactions_2025_07_01")

print(f"Transactions rows: {len(transactions)}")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/finance_dev/raw_files/master/customers"))

# COMMAND ----------

tx=spark.read.csv("/Volumes/workspace/finance_dev/raw_files/transactions/transactions_2025_07_01",header=True,inferSchema=True)
display(tx)