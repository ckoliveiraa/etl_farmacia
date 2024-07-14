import pandas as pd
import numpy as np
import random
from faker import Faker

fake = Faker('pt_BR')

# Número de unidades, clientes, vendedores, compras e produtos
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
