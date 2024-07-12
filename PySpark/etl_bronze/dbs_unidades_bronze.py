# Databricks notebook source
# MAGIC %md
# MAGIC ETL unidades camada Transient > Bronze

# COMMAND ----------

from pyspark.sql.functions import current_date, from_utc_timestamp, date_format,current_timestamp


# COMMAND ----------

# DBTITLE 1,path
transient_unidades ="/mnt/transient/unidades.csv"
bronze_unidades = "/mnt/bronze/unidades"


# COMMAND ----------

# DBTITLE 1,df_unidades
df_unidades = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .load(transient_unidades)


# COMMAND ----------

# DBTITLE 1,Adiciona coluna de controle
df_unidades = df_unidades.withColumn("data_ingestao", current_date())
df_unidades = df_unidades.withColumn("hora_ingestao", date_format(from_utc_timestamp(current_timestamp(), "Etc/GMT+3"), "HH:mm:ss"))

# COMMAND ----------

df_unidades.display()

# COMMAND ----------

#Salva em formado parquet/delta o df unidades no meu ADSL bronze/clientes
df_unidades.write.format("delta").mode("append").save(bronze_unidades)
