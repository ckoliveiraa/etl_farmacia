# Databricks notebook source
# MAGIC %md
# MAGIC ####1. Clientes
# MAGIC
# MAGIC Movendo da camada transient para bronze e alterando para delta

# COMMAND ----------

df_clientes = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .load("abfss://transient@dbfarmacia.dfs.core.windows.net/clientes.csv")

    #Salva em formado parquet/delta o df clientes no meu ADSL bronze/clientes
df_clientes.write.format("delta").option("mergeSchema","true").mode("overwrite").save("abfss://bronze@dbfarmacia.dfs.core.windows.net/clientes/")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create schema compras_bronze
# MAGIC DROP TABLE IF EXISTS farmacias.bronze.clientes_bronze;
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS farmacias.bronze.clientes_bronze
# MAGIC (
# MAGIC   customer_id INT,
# MAGIC   unit_id INT,
# MAGIC   name STRING,
# MAGIC   address STRING,
# MAGIC   phone STRING,
# MAGIC   email STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@dbfarmacia.dfs.core.windows.net/clientes/"

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Produtos
# MAGIC Movendo da camada transient para bronze e alterando para delta

# COMMAND ----------

df_produtos = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .load("abfss://transient@dbfarmacia.dfs.core.windows.net/produtos.csv")

    #Salva em formado parquet/delta o df clientes no meu ADSL bronze/clientes
df_produtos.write.format("delta").option("mergeSchema","true").mode("overwrite").save("abfss://bronze@dbfarmacia.dfs.core.windows.net/produtos/")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create schema compras_bronze
# MAGIC DROP TABLE IF EXISTS farmacias.bronze.produtos_bronze;
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS farmacias.bronze.produtos_bronze
# MAGIC (
# MAGIC   product_id INT,
# MAGIC   name STRING,
# MAGIC   price DOUBLE,
# MAGIC   description STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@dbfarmacia.dfs.core.windows.net/produtos/"

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Unidades
# MAGIC Movendo da camada transient para bronze e alterando para delta

# COMMAND ----------

df_unidades = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .load("abfss://transient@dbfarmacia.dfs.core.windows.net/unidades.csv")

    #Salva em formado parquet/delta o df clientes no meu ADSL bronze/clientes
df_unidades.write.format("delta").option("mergeSchema","true").mode("overwrite").save("abfss://bronze@dbfarmacia.dfs.core.windows.net/unidades/")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create schema compras_bronze
# MAGIC DROP TABLE IF EXISTS farmacias.bronze.unidades_bronze;
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS farmacias.bronze.unidades_bronze
# MAGIC (
# MAGIC unit_id INT,
# MAGIC unit_name STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@dbfarmacia.dfs.core.windows.net/unidades/"

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Vendedores
# MAGIC Movendo da camada transient para bronze e alterando para delta

# COMMAND ----------

df_vendedores = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .load("abfss://transient@dbfarmacia.dfs.core.windows.net/vendedores.csv")

    #Salva em formado parquet/delta o df clientes no meu ADSL bronze/clientes
df_vendedores.write.format("delta").option("mergeSchema","true").mode("overwrite").save("abfss://bronze@dbfarmacia.dfs.core.windows.net/vendedores/")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create schema vendedores_bronze
# MAGIC DROP TABLE IF EXISTS farmacias.bronze.vendedores_bronze;
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS farmacias.bronze.vendedores_bronze
# MAGIC (
# MAGIC   vendor_id INT,
# MAGIC   name STRING,
# MAGIC   email STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@dbfarmacia.dfs.core.windows.net/vendedores"
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Compras
# MAGIC Movendo da camada transient para bronze e alterando para delta

# COMMAND ----------

df_compras = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .option("mergeSchema", "True")\
    .load("abfss://transient@dbfarmacia.dfs.core.windows.net/compras.csv")

    #Salva em formado parquet/delta o df clientes no meu ADSL bronze/clientes
df_compras.write.format("delta").option("mergeSchema","true").mode("overwrite").save("abfss://bronze@dbfarmacia.dfs.core.windows.net/compras/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE farmacias.bronze.compras_bronze
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@dbfarmacia.dfs.core.windows.net/compras/';
# MAGIC
