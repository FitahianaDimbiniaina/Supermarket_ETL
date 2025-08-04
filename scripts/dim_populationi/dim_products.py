import pandas as pd
from sqlalchemy import create_engine
import os

user = "postgres"
password = "4499405"
host = "localhost"
port = 5432
database = "supermarket_etl"

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
RAW_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw', 'Sample_ Superstore.csv')

df_raw = pd.read_csv(RAW_PATH, encoding='latin1')

# Select relevant columns for dim_produit
dim_produit = df_raw[['Product ID', 'Product Name', 'Category']].drop_duplicates().reset_index(drop=True)

dim_produit.rename(columns={
    'Product ID': 'code',
    'Product Name': 'libelle',
    'Category': 'categorie'
}, inplace=True)

dim_produit.to_sql('dim_produit', con=engine, if_exists='replace', index=False)

print(f"dim_produit populated with {len(dim_produit)} rows in PostgreSQL.")
