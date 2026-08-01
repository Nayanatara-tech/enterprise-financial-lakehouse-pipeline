# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS FINANCE_DEV;
# MAGIC CREATE SCHEMA IF NOT EXISTS FINANCE_TEST;
# MAGIC CREATE SCHEMA IF NOT EXISTS FINANCE_PROD;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS;