# Databricks notebook source
# MAGIC %md
# MAGIC ETL vendedores camada Transient > Bronze

# COMMAND ----------

from pyspark.sql.functions import current_date, from_utc_timestamp, date_format,current_timestamp


# COMMAND ----------

# DBTITLE 1,path
transient_vendedores ="/mnt/transient/vendedores.csv"
bronze_vendedores = "/mnt/bronze/vendedores"


# COMMAND ----------

# DBTITLE 1,df_unidades
df_vendedores = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .option("inferSchema", True) \
    .load(transient_vendedores)


# COMMAND ----------

# DBTITLE 1,Adiciona coluna de controle
df_vendedores = df_vendedores.withColumn("data_ingestao", current_date())
df_vendedores = df_vendedores.withColumn("hora_ingestao", date_format(from_utc_timestamp(current_timestamp(), "Etc/GMT+3"), "HH:mm:ss"))

# COMMAND ----------

#Salva em formado parquet/delta o df vendedores no meu ADSL bronze/clientes
df_vendedores.write.format("delta").mode("append").save(bronze_vendedores)
