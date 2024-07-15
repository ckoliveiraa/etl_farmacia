
<h1>Farmácias Brasil 🏪 </h1> 

> Status: Completo


<h2>1. Introdução</h2>

A Rede Farmácias Brasil é uma rede fictícia composta por cinco unidades distribuídas pelo país, que utilizam um sistema ERP para a gestão de suas operações diárias. Este sistema gera relatórios em formato .csv, que são essenciais para a análise e tomada de decisões estratégicas.

<h2>2. Objetivo</h2>

Implementar uma solução de ETL (Extração, Transformação e Carga) para consolidar os dados provenientes dos relatórios .csv das unidades da Rede Farmácias Brasil, garantindo integridade, consistência e facilidade de acesso às informações e uma visão em dashborad de cada unidade.


<h2>3. Primeiros passos</h2>


Foi criado uma função em python para geração dos datasets e utilizado do google colab, para execução da mesma.



1. Acessar o site do google colab,
https://colab.research.google.com/

2. Criar um novo notebook

3. Instalar o faker
```
pip install faker
```
4. Copiar, colar o código abaixo e executar o código
```python
import pandas as pd
import numpy as np
import random
from faker import Faker

fake = Faker('pt_BR')

# Número de linhas: unidades, clientes, vendedores, compras e produtos
num_units = 5
num_customers_per_unit = 50
num_vendors = 20
num_purchases = 100000
num_products = 50

# Gerar dados de unidades
units = [f'Farmacia_{i}' for i in range(1, num_units + 1)]
unit_df = pd.DataFrame({'unit_id': range(1, num_units + 1), 'unit_name': units})

# Gerar dados de clientes
customer_data = []
for unit_id in range(1, num_units + 1):
    for _ in range(num_customers_per_unit):
        customer_data.append({
            'customer_id': len(customer_data) + 1,
            'unit_id': unit_id,
            'name': fake.name(),
            'address': fake.address(),
            'phone': fake.phone_number(),
            'email': fake.email()
        })
customer_df = pd.DataFrame(customer_data)

# Gerar dados de vendedores
vendor_data = []
for _ in range(num_vendors):
    vendor_data.append({
        'vendor_id': len(vendor_data) + 1,
        'name': fake.name(),
        'email': fake.email()
    })
vendor_df = pd.DataFrame(vendor_data)

# Lista de produtos relacionados a farmácia
produtos_farmacia = [
    "Analgésico",
    "Anti-inflamatório",
    "Antibiótico",
    "Antiácido",
    "Antialérgico",
    "Anti-histamínico",
    "Antisséptico",
    "Anti-hipertensivo",
    "Anti-asmático",
    "Anticoncepcional",
    "Anti-hemorrágico",
    "Antiemético",
    "Broncodilatador",
    "Colírio",
    "Descongestionante nasal",
    "Digestivo",
    "Diurético",
    "Expectorante",
    "Hidratante labial",
    "Imunizante",
    "Laxante",
    "Loção capilar",
    "Multivitamínico",
    "Pasta de dente",
    "Pomada cicatrizante",
    "Protetor solar",
    "Repelente",
    "Soro fisiológico",
    "Suplemento alimentar",
    "Vitamina C",
    "Xarope para tosse",
    "Acetaminofeno",
    "Ácido fólico",
    "Água oxigenada",
    "Algodão",
    "Anti-rugas",
    "Bálsamo para lábios",
    "Calêndula",
    "Creme dermatológico",
    "Escova dental",
    "Fio dental",
    "Gel para acne",
    "Lenço umedecido",
    "Máscara facial",
    "Óleo de amêndoas",
    "Pasta antisséptica",
    "Shampoo anticaspa",
    "Spray nasal",
    "Tintura de iodo"
]

# Gerar dados de produtos usando a lista de produtos de farmácia
product_data = []
for idx, produto in enumerate(produtos_farmacia):
    product_data.append({
        'product_id': idx + 1,
        'name': produto,
        'price': round(random.uniform(1, 100), 2),
        'description': fake.sentence()
    })
product_df = pd.DataFrame(product_data)

# Gerar dados de compras
purchase_data = []
for i in range(num_purchases):
    purchase_data.append({
        'purchase_id': i + 1,  # Start purchase_id from 1
        'customer_id': random.randint(1, len(customer_data)),
        'vendor_id': random.randint(1, len(vendor_data)),
        'product_id': random.randint(1, len(product_data)),
        'date': fake.date_this_year(),
        'quantity': random.randint(1, 10),
        'total': round(random.uniform(10, 1000), 2)
    })
purchase_df = pd.DataFrame(purchase_data)

# Salvar os DataFrames em arquivos CSV
unit_df.to_csv('unidades.csv', index=False)
customer_df.to_csv('clientes.csv', index=False)
vendor_df.to_csv('vendedores.csv', index=False)
product_df.to_csv('produtos.csv', index=False)
purchase_df.to_csv('compras.csv', index=False)

print("Arquivos CSV gerados com sucesso!")
```

5. Fazer donwload dos arquivos ".csv"

<h2>4. Modelagem de Dados</h2>

Para este exemplo, utilizamos a modelagem Star Schema, que é uma das mais comuns em bancos de dados de ERP. Abaixo está o diagrama do modelo.

![Modelagem BD relacional](https://github.com/user-attachments/assets/efc1cc20-d97b-4eea-aa36-6e8918de1b6e)

<h2>5. Processo ETL</h2>

<h3>5.1 Extração:</h3>

Utilização do Azure Data Factory para extrair os arquivos .csv das unidades, de uma pasta on-premise para a camada Transient.

Conversão dos arquivos .csv para o formato .parquet.

<h3>5.2 Transformação:</h3>

***Configuração:*** Rodar arquivo configuração para criação do Unity Catalog e os bancos de dados externos.
```SQL
# Databricks notebook source
# MAGIC %sql
# MAGIC --Cria e utiliza o catálogo farmaciaBR
# MAGIC CREATE CATALOG IF NOT EXISTS farmaciaBR;
# MAGIC USE CATALOG farmaciaBR;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC MANAGED LOCATION "abfss://bronze@dbfarmacia.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC MANAGED LOCATION "abfss://silver@dbfarmacia.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC MANAGED LOCATION "abfss://gold@dbfarmacia.dfs.core.windows.net/"
```

***Bronze:*** Conversão dos arquivos .parquet para o formato Delta.
```python
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
```


***Silver:*** Limpeza de dados: remoção de caracteres especiais, tratamento de valores nulos.
```sql
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
```

***Gold:*** Agregações, cálculos e criação de uma tabela otimizada (OBT) contendo todos os dados.
```SQL
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
```

<h3>5.3 Carga:</h3>

Armazenamento dos dados transformados nas respectivas camadas.

Criação de tabelas otimizadas para consultas e análises.

<h3>5.4 Arquitetura de Dados</h3>

(![Arquitetura de Dados](https://github.com/user-attachments/assets/09e8a09d-e275-499a-9c17-2804844da78c)


)


<h2>6. Integração com Power BI</h2>

Conexão da tabela OBT da camada Gold com o Power BI.

Criação de dashboards e relatórios interativos para análise avançada dos dados.

<h2>7. Ferramentas Utilizadas</h2>

***Databricks:*** Plataforma unificada para engenharia e ciência de dados.

***Python:*** Linguagem de programação para scripts de ETL.

***Azure Data Factory:*** Serviço para orquestração de ETL.

***Azure Data Lake Storage:*** Armazenamento escalável para dados brutos e transformados.

***Power BI:*** Ferramenta de visualização de dados.

<h2>8. Conclusão</h2>

A solução de ETL proposta garante que a Rede Farmácias Brasil tenha acesso a dados precisos e atualizados, proporcionando uma base sólida para a tomada de decisões estratégicas e operacionais. A integração com o Power BI oferece uma camada adicional de interatividade e insights através de dashboards e relatórios dinâmicos.
