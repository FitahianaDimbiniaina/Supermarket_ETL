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
ZIP_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw', 'uszips.csv')

df_raw = pd.read_csv(RAW_PATH, encoding='latin1')
df_zip = pd.read_csv(ZIP_PATH, encoding='latin1')
# After renaming columns and dropping duplicates:
dim_magasin = df_raw[['City', 'State', 'Region']].drop_duplicates().reset_index(drop=True)
dim_magasin.rename(columns={'City': 'ville', 'State': 'state'}, inplace=True)

# Map full state names to abbreviations for matching
state_abbrev = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
    'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
    'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
    'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY', 'District of Columbia': 'DC', 'Puerto Rico': 'PR'
}

dim_magasin['state'] = dim_magasin['state'].map(state_abbrev)

df_zip.rename(columns={'city': 'ville', 'state_id': 'state'}, inplace=True)

# Normalize for merge
for df in [dim_magasin, df_zip]:
    df['ville'] = df['ville'].str.strip().str.lower()
    df['state'] = df['state'].str.strip().str.upper()

dim_magasin = dim_magasin.merge(df_zip[['ville', 'state', 'zip']], on=['ville', 'state'], how='left')

dim_magasin.insert(0, 'magasin_id', range(1, len(dim_magasin) + 1))
dim_magasin['zip'] = dim_magasin['zip'].fillna('Unknown')

dim_magasin.to_sql('dim_magasin', con=engine, if_exists='replace', index=False)

print(f"dim_magasin populated with {len(dim_magasin)} rows in PostgreSQL.")
