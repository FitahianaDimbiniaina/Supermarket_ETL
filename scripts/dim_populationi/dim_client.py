import pandas as pd
import numpy as np
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

dim_client = pd.DataFrame()
dim_client['client_id'] = df_raw['Customer ID'].astype(str)
dim_client['ville'] = df_raw['City']

dim_client = dim_client.drop_duplicates(subset=['client_id']).reset_index(drop=True)

np.random.seed(42)
dim_client['age'] = np.random.randint(18, 71, size=len(dim_client))
dim_client['sexe'] = np.random.choice(['M', 'F'], size=len(dim_client))

fidelite_levels = ['low', 'medium', 'high']
dim_client['fidelite'] = np.random.choice(fidelite_levels, size=len(dim_client))

dim_client.to_sql('dim_client', con=engine, if_exists='replace', index=False)

print(f"dim_client populated with {len(dim_client)} rows in PostgreSQL.")
