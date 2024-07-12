# Databricks notebook source
# MAGIC %md
# MAGIC ETL produtos camada Transient > Bronze

# COMMAND ----------

from pyspark.sql.functions import current_date, from_utc_timestamp, date_format,current_timestamp


# COMMAND ----------

# DBTITLE 1,path
transient_produtos ="/mnt/transient/produtos.csv"
bronze_produtos = "/mnt/bronze/produtos"


# COMMAND ----------

# DBTITLE 1,df_produtos
df_produtos = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .load(transient_produtos)


# COMMAND ----------

# DBTITLE 1,Adiciona coluna de controle
df_produtos = df_produtos.withColumn("data_ingestao", current_date())
df_produtos = df_produtos.withColumn("hora_ingestao", date_format(from_utc_timestamp(current_timestamp(), "Etc/GMT+3"), "HH:mm:ss"))

# COMMAND ----------

#Salva em formado parquet/delta o df clientes no meu ADSL bronze/clientes
df_produtos.write.format("delta").mode("append").save(bronze_produtos)
