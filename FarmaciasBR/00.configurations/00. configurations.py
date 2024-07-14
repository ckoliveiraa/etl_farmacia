# Databricks notebook source
# MAGIC %sql
# MAGIC --Cria e utiliza o catálogo farmaciaBR
# MAGIC CREATE CATALOG IF NOT EXISTS farmaciaBR;
# MAGIC USE CATALOG farmaciaBR;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC MANAGED LOCATION "abfss://bronze@dbfarmacia.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC MANAGED LOCATION "abfss://silver@dbfarmacia.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC MANAGED LOCATION "abfss://gold@dbfarmacia.dfs.core.windows.net/"
