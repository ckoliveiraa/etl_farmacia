# Databricks notebook source
# MAGIC %md
# MAGIC ETL compras camada Transient > Bronze

# COMMAND ----------

from pyspark.sql.functions import current_date, from_utc_timestamp, date_format,current_timestamp


# COMMAND ----------

# DBTITLE 1,path
transient_compras ="/mnt/transient/compras.csv"
bronze_compras = "/mnt/bronze/compras"


# COMMAND ----------

# DBTITLE 1,df_compras
df_compras = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .load(transient_compras)


# COMMAND ----------

# DBTITLE 1,Adiciona coluna de controle
df_compras = df_compras.withColumn("data_ingestao", current_date())
df_compras = df_compras.withColumn("hora_ingestao", date_format(from_utc_timestamp(current_timestamp(), "Etc/GMT+3"), "HH:mm:ss"))

# COMMAND ----------

#Salva em formado parquet/delta o df compras no meu ADSL bronze/compras
df_compras.write.format("delta").mode("append").save(bronze_compras)
