# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS farmacias.gold.farmacias_brasil;
# MAGIC CREATE TABLE IF NOT EXISTS farmacias.gold.farmacias_brasil
# MAGIC AS
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM farmacias.silver.farmacias

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS farmacias.gold.farmacias_rj;
# MAGIC CREATE TABLE IF NOT EXISTS farmacias.gold.farmacias_rj
# MAGIC AS
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM farmacias.silver.farmacias
# MAGIC WHERE nome_unidade = 'Rio de Janeiro'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS farmacias.gold.farmacias_sp;
# MAGIC CREATE TABLE IF NOT EXISTS farmacias.gold.farmacias_sp
# MAGIC AS
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM farmacias.silver.farmacias
# MAGIC WHERE nome_unidade = 'São Paulo'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS farmacias.gold.farmacias_sc;
# MAGIC CREATE TABLE IF NOT EXISTS farmacias.gold.farmacias_sc
# MAGIC AS
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM farmacias.silver.farmacias
# MAGIC WHERE nome_unidade = 'Santa Catarina'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS farmacias.gold.farmacias_mg;
# MAGIC CREATE TABLE IF NOT EXISTS farmacias.gold.farmacias_mg
# MAGIC AS
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM farmacias.silver.farmacias
# MAGIC WHERE nome_unidade = 'Minas Gerais'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS farmacias.gold.farmacias_bsb;
# MAGIC CREATE TABLE IF NOT EXISTS farmacias.gold.farmacias_bsb
# MAGIC AS
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM farmacias.silver.farmacias
# MAGIC WHERE nome_unidade = 'Brasilia'
