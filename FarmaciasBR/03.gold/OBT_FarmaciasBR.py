# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW clientes_unidades
# MAGIC AS SELECT 
# MAGIC cl.id_cliente,
# MAGIC cl.id_unidade,
# MAGIC un.unit_name,
# MAGIC cl.nome_cliente,
# MAGIC cl.endereco_cliente,
# MAGIC cl.telefone_cliente,
# MAGIC cl.email_cliente
# MAGIC FROM farmaciabr.silver.clientes cl
# MAGIC INNER JOIN farmaciabr.bronze.unidades un
# MAGIC ON
# MAGIC cl.id_unidade = un.unit_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS farmaciabr.gold.farmaciasBR
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   cp.id_compra,
# MAGIC   cp.id_cliente,
# MAGIC   cl.nome_cliente,
# MAGIC   cl.endereco_cliente,
# MAGIC   cl.telefone_cliente,
# MAGIC   cl.email_cliente,
# MAGIC   vd.nome_vendedor,
# MAGIC   vd.email_vendedor,
# MAGIC   cp.id_produto,
# MAGIC   pr.nome_produto,
# MAGIC   pr.preco_produto,
# MAGIC   pr.descricao_produto,
# MAGIC   cp.data_compra,
# MAGIC   cp.quantidade,
# MAGIC   (cp.quantidade * pr.preco_produto) AS valor_compra,
# MAGIC   current_timestamp() - INTERVAL 3 HOURS AS data_atualizacao,
# MAGIC   CAST(date_format(current_timestamp() - INTERVAL 3 HOURS, 'yyyy-MM-dd') AS date) AS data_hora_atualizacao
# MAGIC FROM farmaciabr.silver.compras cp
# MAGIC INNER JOIN clientes_unidades cl ON cp.id_cliente = cl.id_cliente
# MAGIC INNER JOIN farmaciabr.silver.vendedores vd ON cp.id_vendedor = vd.id_vendedor
# MAGIC INNER JOIN farmaciabr.silver.produtos pr ON cp.id_produto = pr.id_produto

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW temp_farmaciasBR AS
# MAGIC SELECT
# MAGIC   cp.id_compra,
# MAGIC   cp.id_cliente,
# MAGIC   cl.nome_cliente,
# MAGIC   cl.endereco_cliente,
# MAGIC   cl.telefone_cliente,
# MAGIC   cl.email_cliente,
# MAGIC   vd.nome_vendedor,
# MAGIC   vd.email_vendedor,
# MAGIC   cp.id_produto,
# MAGIC   pr.nome_produto,
# MAGIC   pr.preco_produto,
# MAGIC   pr.descricao_produto,
# MAGIC   cp.data_compra,
# MAGIC   cp.quantidade,
# MAGIC   (cp.quantidade * pr.preco_produto) AS valor_compra,
# MAGIC   current_timestamp() - INTERVAL 3 HOURS AS data_atualizacao,
# MAGIC   CAST(date_format(current_timestamp() - INTERVAL 3 HOURS, 'yyyy-MM-dd') AS date) AS data_hora_atualizacao
# MAGIC FROM farmaciabr.silver.compras cp
# MAGIC INNER JOIN clientes_unidades cl ON cp.id_cliente = cl.id_cliente
# MAGIC INNER JOIN farmaciabr.silver.vendedores vd ON cp.id_vendedor = vd.id_vendedor
# MAGIC INNER JOIN farmaciabr.silver.produtos pr ON cp.id_produto = pr.id_produto
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO farmaciabr.gold.farmaciasBR AS tgt
# MAGIC USING (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY id_compra ORDER BY data_hora_atualizacao DESC) AS rn
# MAGIC     FROM temp_farmaciasBR
# MAGIC   ) tmp
# MAGIC   WHERE rn = 1
# MAGIC ) AS src
# MAGIC ON tgt.id_compra = src.id_compra
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET
# MAGIC     tgt.id_cliente = src.id_cliente,
# MAGIC     tgt.nome_cliente = src.nome_cliente,
# MAGIC     tgt.endereco_cliente = src.endereco_cliente,
# MAGIC     tgt.telefone_cliente = src.telefone_cliente,
# MAGIC     tgt.email_cliente = src.email_cliente,
# MAGIC     tgt.nome_vendedor = src.nome_vendedor,
# MAGIC     tgt.email_vendedor = src.email_vendedor,
# MAGIC     tgt.id_produto = src.id_produto,
# MAGIC     tgt.nome_produto = src.nome_produto,
# MAGIC     tgt.preco_produto = src.preco_produto,
# MAGIC     tgt.descricao_produto = src.descricao_produto,
# MAGIC     tgt.data_compra = src.data_compra,
# MAGIC     tgt.quantidade = src.quantidade,
# MAGIC     tgt.valor_compra = src.valor_compra,
# MAGIC     tgt.data_atualizacao = src.data_atualizacao,
# MAGIC     tgt.data_hora_atualizacao = src.data_hora_atualizacao
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (id_compra, id_cliente, nome_cliente, endereco_cliente, telefone_cliente, email_cliente,
# MAGIC           nome_vendedor, email_vendedor, id_produto, nome_produto, preco_produto, descricao_produto,
# MAGIC           data_compra, quantidade, valor_compra, data_atualizacao, data_hora_atualizacao)
# MAGIC   VALUES (src.id_compra, src.id_cliente, src.nome_cliente, src.endereco_cliente, src.telefone_cliente, src.email_cliente,
# MAGIC           src.nome_vendedor, src.email_vendedor, src.id_produto, src.nome_produto, src.preco_produto, src.descricao_produto,
# MAGIC           src.data_compra, src.quantidade, src.valor_compra, src.data_atualizacao, src.data_hora_atualizacao);
# MAGIC
