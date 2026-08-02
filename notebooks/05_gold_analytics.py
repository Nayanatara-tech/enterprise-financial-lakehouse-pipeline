# Databricks notebook source
# MAGIC %md
# MAGIC ###Revenue by region

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE finance_dev.gold_revenue_by_region
# MAGIC AS
# MAGIC SELECT
# MAGIC Region,
# MAGIC round(sum(AmountUSD),2) as Revenue
# MAGIC from finance_dev.silver_transactions_enriched
# MAGIC group by Region
# MAGIC ORDER BY revenue desc;
# MAGIC
# MAGIC select * from finance_dev.gold_revenue_by_region;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Top Customers by region

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE finance_dev.gold_top_customers_by_region
# MAGIC AS
# MAGIC SELECT
# MAGIC Region,
# MAGIC CustomerName,
# MAGIC round(sum(AmountUSD),2) as Revenue
# MAGIC from finance_dev.silver_transactions_enriched
# MAGIC group by Region,CustomerName
# MAGIC ORDER BY revenue desc;
# MAGIC
# MAGIC select * from finance_dev.gold_top_customers_by_region;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Monthly Revenue Trend

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE finance_dev.gold_monthly_trend AS
# MAGIC SELECT
# MAGIC     date_format(to_date(TransactionDate), 'yyyy-MM') AS YearMonth,
# MAGIC     ROUND(SUM(AmountUSD), 2) AS RevenueUSD
# MAGIC FROM finance_dev.silver_transactions_enriched
# MAGIC GROUP BY YearMonth
# MAGIC ORDER BY YearMonth;
# MAGIC
# MAGIC select * from finance_dev.gold_monthly_trend

# COMMAND ----------

# MAGIC %md
# MAGIC ###Currency Distribution percentage

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE finance_dev.gold_currency_distribution as
# MAGIC (With currency_amt as
# MAGIC (
# MAGIC SELECT
# MAGIC Currency,
# MAGIC round(sum(Amount),2) as Revenue
# MAGIC from finance_dev.silver_transactions_enriched
# MAGIC group by Currency
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC Currency,
# MAGIC Revenue,
# MAGIC round(Revenue/sum(Revenue) over(),2)*100 as distribution_currency
# MAGIC from currency_amt);
# MAGIC
# MAGIC select * from finance_dev.gold_currency_distribution
# MAGIC