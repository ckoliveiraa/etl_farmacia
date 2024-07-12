# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####1. Clientes
# MAGIC
# MAGIC Movendo da camada bronze para silver e fazendo ajustes

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS farmacias.silver.clientes_unidades_silver;
# MAGIC CREATE TABLE IF NOT EXISTS farmacias.silver.clientes_unidades_silver
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   cl.customer_id as `id_cliente`,
# MAGIC   cl.unit_id as `id_unidade`,
# MAGIC   un.unit_name as `nome_unidade`,
# MAGIC   cl.name as `nome_cliente`,
# MAGIC   cl. address as `endereco_cliente`,
# MAGIC   cl.phone as `telefone`,
# MAGIC   cl.email as `email_cliente`
# MAGIC FROM farmacias.bronze.clientes_bronze cl
# MAGIC INNER JOIN farmacias.bronze.unidades_bronze un
# MAGIC ON
# MAGIC cl.unit_id = un.unit_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE farmacias.silver.clientes_unidades_silver
# MAGIC SET nome_unidade = 'São Paulo'
# MAGIC WHERE nome_unidade LIKE 'S%o Paulo';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS farmacias.silver.farmacias;
# MAGIC CREATE TABLE IF NOT EXISTS farmacias.silver.farmacias
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   cp.purchase_id AS id_compra,
# MAGIC   cp.customer_id AS id_cliente,
# MAGIC   cl.nome_cliente AS nome_cliente,
# MAGIC   cl.endereco_cliente AS endereco_cliente,
# MAGIC   cl.telefone AS telefone,
# MAGIC   cl.email_cliente AS email_cliente,
# MAGIC   cl.nome_unidade AS nome_unidade,
# MAGIC   cp.vendor_id AS id_vendedor,
# MAGIC   vd.name AS nome_vendedor,
# MAGIC   vd.email AS email_vendedor,
# MAGIC   cp.product_id AS id_produto,
# MAGIC   pr.name AS nome_produto,
# MAGIC   pr.price AS preco_produto,
# MAGIC   pr.description AS descricao_produto,
# MAGIC   cp.date AS data_compra,
# MAGIC   cp.quantity AS quantidade_comprada,
# MAGIC   (cp.quantity * pr.price) AS total,
# MAGIC   from_utc_timestamp(current_timestamp(), "-03:00") AS data_ingestao
# MAGIC FROM farmacias.bronze.compras_bronze cp
# MAGIC INNER JOIN farmacias.bronze.vendedores_bronze vd ON cp.vendor_id = vd.vendor_id
# MAGIC INNER JOIN farmacias.bronze.produtos_bronze pr ON cp.product_id = pr.product_id
# MAGIC INNER JOIN farmacias.silver.clientes_unidades_silver cl ON cp.customer_id = cl.id_cliente;
