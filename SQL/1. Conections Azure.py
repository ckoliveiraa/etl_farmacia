# Databricks notebook source
# MAGIC %md
# MAGIC > #### Presets Azure
# MAGIC   1 . Create external conections
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE LOCAL EXTERNAL TRANSIENT
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS farmacia_ext_location_transient
# MAGIC     URL "abfss://transient@dbfarmacia.dfs.core.windows.net/"
# MAGIC     WITH (STORAGE CREDENTIAL dbfarmacia_ext_credential);

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE LOCAL EXTERNAL BRONZE
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS farmacia_ext_location_bronze
# MAGIC     URL "abfss://bronze@dbfarmacia.dfs.core.windows.net/"
# MAGIC     WITH (STORAGE CREDENTIAL dbfarmacia_ext_credential);

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE LOCAL EXTERNAL SILVER
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS farmacia_ext_location_silver
# MAGIC     URL "abfss://silver@dbfarmacia.dfs.core.windows.net/"
# MAGIC     WITH (STORAGE CREDENTIAL dbfarmacia_ext_credential);

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE LOCAL EXTERNAL GOLD
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS farmacia_ext_location_gold
# MAGIC     URL "abfss://gold@dbfarmacia.dfs.core.windows.net/"
# MAGIC     WITH (STORAGE CREDENTIAL dbfarmacia_ext_credential);

# COMMAND ----------

# MAGIC %md
# MAGIC #####2.Create Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS farmacias;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG farmacias;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.Create Schemas

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

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS;
