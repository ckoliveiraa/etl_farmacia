# Databricks notebook source
from pyspark.sql.functions import col, expr

# COMMAND ----------

# DBTITLE 1,Path
bronze_compras = "/mnt/bronze/compras/"
silver_farmacias = "/mnt/silver/farmacias/"

# COMMAND ----------

# DBTITLE 1,df_unidades
bronze_unidades = "/mnt/bronze/unidades"
df_unidades = spark.read.format("delta").load(bronze_unidades)

#Renomeando colunas
df_unidades = df_unidades.withColumnRenamed("unit_name", "filial") \
 
#Removendo colunas de controle da bronze
colunas_para_remover = ["data_ingestao", "hora_ingestao"]
df_unidades = df_unidades.drop(*colunas_para_remover)

# COMMAND ----------

# DBTITLE 1,df_clientes
bronze_clientes = "/mnt/bronze/clientes"
df_clientes = spark.read.format("delta").load(bronze_clientes)

#Renomeando colunas
df_clientes = df_clientes.withColumnRenamed("name", "nome_cliente") \
                         .withColumnRenamed("address", "endereço") \
                         .withColumnRenamed("phone", "telefone")\
                         .withColumnRenamed("email", "email_cliente")
#Removendo colunas de controle da bronze
colunas_para_remover = ["data_ingestao", "hora_ingestao"]
df_clientes = df_clientes.drop(*colunas_para_remover)

#Inner com df_unidades
df_clientes = df_clientes.join(df_unidades, 'unit_id', 'inner')
                         

# COMMAND ----------

# DBTITLE 1,df_produtos
bronze_produtos = "/mnt/bronze/produtos"
df_produtos = spark.read.format("delta").load(bronze_produtos)

#Renomeando colunas
df_produtos = df_produtos.withColumnRenamed("name", "nome_produto") \
                         .withColumnRenamed("price", "preço") \
                         .withColumnRenamed("description", "descrição_produto")

#Removendo colunas de controle da bronze
colunas_para_remover = ["data_ingestao", "hora_ingestao"]
df_produtos = df_produtos.drop(*colunas_para_remover)

# COMMAND ----------

# DBTITLE 1,df_vendedores
bronze_vendedores = "/mnt/bronze/vendedores"
df_vendedores = spark.read.format("delta").load(bronze_vendedores)

#Renomeando colunas
df_vendedores = df_vendedores.withColumnRenamed("name", "nome_vendedor") \
                             .withColumnRenamed("email", "email_vendedor") \
#Removendo colunas de controle da bronze
colunas_para_remover = ["data_ingestao", "hora_ingestao"]
df_vendedores = df_vendedores.drop(*colunas_para_remover)

# COMMAND ----------

# DBTITLE 1,df_compras_silver
df_compras_silver = spark.read.format("delta").load(bronze_compras)

#Renomeando colunas
df_compras_silver = df_compras_silver.withColumnRenamed("date", "data_compra")\
                                     .withColumnRenamed("quantity", "quantidade")

#Removendo colunas de controle da bronze
colunas_para_remover = ["data_ingestao", "hora_ingestao", "total"]
df_compras_silver = df_compras_silver.drop(*colunas_para_remover)
                                     

# COMMAND ----------

# DBTITLE 1,Inner compras_silver and clientes
df_compras_silver = df_compras_silver.join(df_clientes, 'customer_id', 'inner')

# COMMAND ----------

# DBTITLE 1,Inner compras_silver and produtos
df_compras_silver = df_compras_silver.join(df_produtos, 'product_id', 'inner')

# COMMAND ----------

# DBTITLE 1,Inner compras_silver and vendedores
df_compras_silver = df_compras_silver.join(df_vendedores, 'vendor_id', 'inner')

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col

df_compras_silver = df_compras_silver.withColumn("quantidade", col("quantidade").cast("float"))
df_compras_silver = df_compras_silver.withColumn("preço", col("preço").cast("float"))

df_compras_silver = df_compras_silver.withColumn("total_compra", col("quantidade") * col("preço"))

# COMMAND ----------

#Salva em formado parquet/delta o df compras_silver no meu ADSL silver/farmacias
df_compras_silver.write.format("delta").mode("overwrite").save(silver_farmacias)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS db_farmacia.silver.farmacias")
df_compras_silver.write.format("delta").saveAsTable("db_farmacia.silver.farmacias")
