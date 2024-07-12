# Databricks notebook source
# MAGIC %md
# MAGIC ETL clientes camada Transient > Bronze

# COMMAND ----------

from pyspark.sql.functions import current_date, from_utc_timestamp, date_format,current_timestamp


# COMMAND ----------

# DBTITLE 1,path
transient_clientes ="/mnt/transient/clientes.csv"
bronze_clientes = "/mnt/bronze/clientes"

# COMMAND ----------

# DBTITLE 1,df_clientes
df_clientes = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .load(transient_clientes)


# COMMAND ----------

df_clientes = df_clientes.withColumn('data_ingestao', from_utc_timestamp(current_timestamp(), "Etc/GMT+3"))

# COMMAND ----------

#Salva em formado parquet/delta o df clientes no meu ADSL bronze/clientes
df_clientes.write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable("farmacias.bronze.clientes_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM farmacias.bronze.clientes_bronze
# MAGIC WHERE name = 'Ravy Pires'
# MAGIC
