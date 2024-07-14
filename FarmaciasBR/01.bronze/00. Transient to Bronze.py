# Databricks notebook source
# MAGIC %md
# MAGIC ###Variáveis de ambiente bronze###
# MAGIC
# MAGIC

# COMMAND ----------

schema = "bronze"
catalog = "farmaciaBR"
container = "transient"
storage_account_name = "dbfarmacia"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Imports###

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp, expr
from delta.tables import DeltaTable


# COMMAND ----------

# MAGIC %md
# MAGIC ###Clientes###
# MAGIC 1. Definição das variáveis 
# MAGIC 2. Leitura do arquvivo CSV e criação do DataFrame
# MAGIC 3. Adição das colunas de controle (data_carga e hora_data)
# MAGIC 4. Salvando tabela em formato Delta na camada bronze

# COMMAND ----------

#1.Definição das variáveis
file = "clientes.csv"
table = "clientes"

clientes_path = (f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{file}")

# COMMAND ----------

#2.Leitura do arquivo CSV e criação do DataFrame
clientes_df = spark.read.csv(clientes_path, header=True, multiLine=True)

# COMMAND ----------

#3.Adição das colunas de controle (data_carga e data_hora_carga)
clientes_df = clientes_df.withColumn("data_carga", current_date())
clientes_df = clientes_df.withColumn("data_hora_carga", expr("current_timestamp() - INTERVAL 3 HOURS"))

# COMMAND ----------

#4.Salvando tabela em formato Delta na camada bronze
clientes_df.write \
    .format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f"{catalog}.{schema}.{table}")
print("Data saved successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Produtos###
# MAGIC 1. Definição das variáveis 
# MAGIC 2. Lendo o arquivo .csv na camada transient e transformando em df
# MAGIC 3. Adição das colunas de controle (data_carga e hora_data)
# MAGIC 4. Salvando tabela em formato Delta na camada bronze

# COMMAND ----------

###PRODUTOS###
file = "produtos.csv"
table = "produtos"

produtos_path = (f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{file}")

# COMMAND ----------

produtos_df = spark.read.csv(produtos_path, header=True, multiLine=True)

# COMMAND ----------

produtos_df = produtos_df.withColumn("data_carga", current_date())
produtos_df = produtos_df.withColumn("data_hora_carga", expr("current_timestamp() - INTERVAL 3 HOURS"))

# COMMAND ----------

produtos_df.write \
    .format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f"{catalog}.{schema}.{table}")
print("Data saved successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unidades###
# MAGIC 1. Definição das variáveis 
# MAGIC 2. Lendo o arquivo .csv na camada transient e transformando em df
# MAGIC 3. Adição das colunas de controle (data_carga e hora_data)
# MAGIC 4. Salvando tabela em formato Delta na camada bronze

# COMMAND ----------

###unidades###
file = "unidades.csv"
table = "unidades"

unidades_path = (f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{file}")
unidades_df = spark.read.csv(unidades_path, header=True, multiLine=True)

# COMMAND ----------

unidades_df = spark.read.csv(unidades_path, header=True, multiLine=True)

# COMMAND ----------

unidades_df = unidades_df.withColumn("data_carga", current_date())
unidades_df = unidades_df.withColumn("data_hora_carga", expr("current_timestamp() - INTERVAL 3 HOURS"))

# COMMAND ----------

unidades_df.write \
    .format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f"{catalog}.{schema}.{table}")
print("Data saved successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Vendedores###
# MAGIC 0. Full Load
# MAGIC 1. Definição das variáveis 
# MAGIC 2. Lendo o arquivo .csv na camada transient e transformando em df
# MAGIC 3. Adição das colunas de controle (data_carga e hora_data)
# MAGIC 4. Salvando tabela em formato Delta na camada bronze

# COMMAND ----------

###Vendedores###
file = "vendedores.csv"
table = "vendedores"

vendedores_path = (f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{file}")
vendedores_df = spark.read.csv(unidades_path, header=True, multiLine=True)

# COMMAND ----------

vendedores_df = spark.read.csv(vendedores_path, header=True, multiLine=True)

# COMMAND ----------

vendedores_df = vendedores_df.withColumn("data_carga", current_date())
vendedores_df = vendedores_df.withColumn("data_hora_carga", expr("current_timestamp() - INTERVAL 3 HOURS"))

# COMMAND ----------

vendedores_df.write \
    .format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f"{catalog}.{schema}.{table}")
print("Data saved successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Compras###
# MAGIC 0. Merge Load
# MAGIC 1. Definição das variáveis 
# MAGIC 2. Lendo o arquivo .csv na camada transient e transformando em df
# MAGIC 3. Adição das colunas de controle (data_carga e hora_data)
# MAGIC 4. Salvando tabela em formato Delta na camada bronze

# COMMAND ----------

###Compras###
file = "compras.csv"
table = "compras"
temp_table = "compras_temp"
delta_table = "famaciabr.bronze.compras"

compras_path = (
    f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{file}"
)
compras_df = spark.read.csv(unidades_path, header=True, multiLine=True)

# COMMAND ----------

compras_df = spark.read.csv(compras_path, header=True, multiLine=True)

# COMMAND ----------

compras_df = compras_df.withColumn("data_carga", current_date())
compras_df = compras_df.withColumn("data_hora_carga", expr("current_timestamp() - INTERVAL 3 HOURS"))


# COMMAND ----------

#Cria tabela temporária
compras_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{temp_table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO farmaciabr.bronze.compras AS tgt
# MAGIC USING farmaciabr.bronze.compras_temp AS scr
# MAGIC ON tgt.purchase_id = scr.purchase_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE farmaciabr.bronze.compras_temp;
