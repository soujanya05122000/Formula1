-- Databricks notebook source
DROP SCHEMA f1_processed CASCADE;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS f1_processed
MANAGED LOCATION 'abfss://formula1@adventureworksa1.dfs.core.windows.net/silver';