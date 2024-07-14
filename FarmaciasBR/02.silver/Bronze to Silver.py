# Databricks notebook source
# MAGIC %md
# MAGIC ###Clientes###

# COMMAND ----------

# MAGIC %sql
# MAGIC --Ajustes do schema e renomenando colunas da tabela clientes
# MAGIC
# MAGIC DROP TABLE IF EXISTS farmaciabr.silver.clientes;
# MAGIC CREATE TABLE IF NOT EXISTS farmaciabr.silver.clientes
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC CAST(customer_id AS INT) AS id_cliente,
# MAGIC CAST(unit_id AS INT) AS id_unidade,
# MAGIC CAST(name AS STRING) AS nome_cliente,
# MAGIC CAST(address AS STRING) AS endereco_cliente,
# MAGIC CAST(phone AS STRING) AS telefone_cliente,
# MAGIC CAST(email AS STRING) AS email_cliente,
# MAGIC current_timestamp() - INTERVAL 3 HOURS AS data_carga,
# MAGIC CAST(date_format(current_timestamp() - INTERVAL 3 HOURS, 'yyyy-MM-dd') AS date) AS data_hora_carga
# MAGIC FROM farmaciabr.bronze.clientes

# COMMAND ----------

# MAGIC %md
# MAGIC ###Produtos###

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS farmaciabr.silver.produtos;
# MAGIC CREATE TABLE IF NOT EXISTS farmaciabr.silver.produtos
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC CAST(product_id AS INT) AS id_produto,
# MAGIC CAST(name AS STRING) AS nome_produto,
# MAGIC CAST(price AS FLOAT) AS preco_produto,
# MAGIC CAST(description AS STRING) AS descricao_produto,
# MAGIC current_timestamp() - INTERVAL 3 HOURS AS data_carga,
# MAGIC CAST(date_format(current_timestamp() - INTERVAL 3 HOURS, 'yyyy-MM-dd') AS date) AS data_hora_carga
# MAGIC FROM farmaciabr.bronze.produtos

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unidades###

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS farmaciabr.silver.unidades;
# MAGIC CREATE TABLE IF NOT EXISTS farmaciabr.silver.unidades
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC CAST(unit_id AS INT) AS id_unidade,
# MAGIC CAST(unit_name AS STRING) AS nome_unidade,
# MAGIC current_timestamp() - INTERVAL 3 HOURS AS data_carga,
# MAGIC CAST(date_format(current_timestamp() - INTERVAL 3 HOURS, 'yyyy-MM-dd') AS date) AS data_hora_carga
# MAGIC FROM farmaciabr.bronze.unidades

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE farmaciabr.silver.unidades
# MAGIC SET nome_unidade = 'São Paulo'
# MAGIC WHERE nome_unidade LIKE 'S%o Paulo';

# COMMAND ----------

# MAGIC %md
# MAGIC ###Vendedores###

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS farmaciabr.silver.vendedores;
# MAGIC CREATE TABLE IF NOT EXISTS farmaciabr.silver.vendedores
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC CAST(vendor_id AS INT) AS id_vendedor,
# MAGIC CAST(name AS STRING) AS nome_vendedor,
# MAGIC CAST(email AS STRING) AS email_vendedor,
# MAGIC current_timestamp() - INTERVAL 3 HOURS AS data_carga,
# MAGIC CAST(date_format(current_timestamp() - INTERVAL 3 HOURS, 'yyyy-MM-dd') AS date) AS data_hora_carga
# MAGIC FROM farmaciabr.bronze.vendedores

# COMMAND ----------

# MAGIC %md
# MAGIC ###Compras###
# MAGIC
# MAGIC
# MAGIC ####Dúvida, precisa fazer merge na silver tbm??? Qual a melhor maneira de se fazer?? Quando faço a view não estou processando 100% da tabela tbm?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS farmaciabr.silver.compras
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   CAST(purchase_id AS INT) AS id_compra,
# MAGIC   CAST(customer_id AS INT) AS id_cliente,
# MAGIC   CAST(vendor_id AS INT) AS id_vendedor,
# MAGIC   CAST(product_id AS INT) AS id_produto,
# MAGIC   CAST(date AS DATE) AS data_compra,
# MAGIC   CAST(quantity AS INT) AS quantidade,
# MAGIC   current_timestamp() - INTERVAL 3 HOURS AS data_carga,
# MAGIC   CAST(date_format(current_timestamp() - INTERVAL 3 HOURS, 'yyyy-MM-dd') AS date) AS data_hora_carga
# MAGIC FROM farmaciabr.bronze.compras;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW silver_compras_updates AS
# MAGIC SELECT
# MAGIC   CAST(purchase_id AS INT) AS id_compra,
# MAGIC   CAST(customer_id AS INT) AS id_cliente,
# MAGIC   CAST(vendor_id AS INT) AS id_vendedor,
# MAGIC   CAST(product_id AS INT) AS id_produto,
# MAGIC   CAST(date AS DATE) AS data_compra,
# MAGIC   CAST(quantity AS INT) AS quantidade,
# MAGIC   current_timestamp() - INTERVAL 3 HOURS AS data_carga,
# MAGIC   CAST(date_format(current_timestamp() - INTERVAL 3 HOURS, 'yyyy-MM-dd') AS date) AS data_hora_carga
# MAGIC FROM farmaciabr.bronze.compras
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO farmaciabr.silver.compras AS tgt
# MAGIC USING silver_compras_updates AS scr
# MAGIC ON 
# MAGIC tgt.id_compra = scr.id_compra AND
# MAGIC tgt.id_cliente = scr.id_cliente
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET
# MAGIC     tgt.id_vendedor = scr.id_vendedor,
# MAGIC     tgt.id_produto = scr.id_produto,
# MAGIC     tgt.data_compra = scr.data_compra,
# MAGIC     tgt.quantidade = scr.quantidade,
# MAGIC     tgt.data_carga = scr.data_carga,
# MAGIC     tgt.data_hora_carga = scr.data_hora_carga
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (id_compra, id_cliente, id_vendedor, id_produto, data_compra, quantidade, data_carga, data_hora_carga)
# MAGIC   VALUES (scr.id_compra, scr.id_cliente, scr.id_vendedor, scr.id_produto, scr.data_compra, scr.quantidade, scr.data_carga, scr.data_hora_carga);
# MAGIC
# MAGIC
