# Databricks notebook source
gold_RJ = "/mnt/gold/RJ/"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM db_farmacia.silver.farmacias
# MAGIC WHERE filial = 'Rio de Janeiro'

# COMMAND ----------

df_unidade_RJ = _sqldf

# COMMAND ----------

#Salva em formado parquet/delta 
df_unidade_RJ.write.format("delta").mode("overwrite").save(gold_RJ)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS db_farmacias.farmaciasRJ")
df_unidade_RJ.write.format("delta").saveAsTable("db_farmacia.gold.farmaciasRJ")
