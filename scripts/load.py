import pandas as pd
from sqlalchemy import create_engine
import os

user = "postgres"
password = "4499405"
host = "localhost"
port = 5432
database = "supermarket_etl"

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
RAW_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw', 'Sample_ Superstore.csv')

def main():
    df_raw = pd.read_csv(RAW_PATH, encoding='latin1')
    df_raw.columns = df_raw.columns.str.strip()

    # Map full state names to abbreviations in raw data for join with dim_magasin
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
    df_raw['State'] = df_raw['State'].map(state_abbrev)
    df_raw['State'] = df_raw['State'].str.upper()

    # Normalize city and state for matching
    df_raw['City'] = df_raw['City'].str.strip().str.lower()

    with engine.connect() as conn:
        # Load dimension tables
        dim_client = pd.read_sql("SELECT client_id FROM dim_client", conn)
        dim_produit = pd.read_sql("SELECT code FROM dim_produit", conn)
        dim_magasin = pd.read_sql("SELECT magasin_id, ville, state FROM dim_magasin", conn)
        dim_temps = pd.read_sql("SELECT date_id, date FROM dim_temps", conn)

    # Normalize dim_magasin city/state for matching
    dim_magasin['ville'] = dim_magasin['ville'].str.strip().str.lower()
    dim_magasin['state'] = dim_magasin['state'].str.strip().str.upper()

    # Merge raw data with dim_produit on product code
    df_fact = df_raw.merge(dim_produit, left_on='Product ID', right_on='code', how='inner')
    print(f"Rows after joining dim_produit: {len(df_fact)}")

    # Merge with dim_client on client_id (Customer ID in raw == client_id in dim_client)
    df_fact = df_fact.merge(dim_client, left_on='Customer ID', right_on='client_id', how='inner')
    print(f"Rows after joining dim_client: {len(df_fact)}")

    # Merge with dim_magasin on city and state
    df_fact = df_fact.merge(dim_magasin, left_on=['City', 'State'], right_on=['ville', 'state'], how='left', indicator=True)
    unmatched_magasin = df_fact[df_fact['_merge'] == 'left_only']
    if len(unmatched_magasin) > 0:
        print(f"Warning: {len(unmatched_magasin)} unmatched city/state pairs. Example:")
        print(unmatched_magasin[['City', 'State']].drop_duplicates().head())
    df_fact = df_fact[df_fact['_merge'] == 'both'].copy()
    print(f"Rows after joining dim_magasin: {len(df_fact)}")

    if df_fact.empty:
        print("No rows left after matching dim_magasin. Aborting insert.")
        return

    # Convert Order Date to date for join with dim_temps
    df_fact['Order Date'] = pd.to_datetime(df_fact['Order Date'], errors='coerce').dt.date
    dim_temps['date'] = pd.to_datetime(dim_temps['date']).dt.date

    # Merge with dim_temps on date
    df_fact = df_fact.merge(dim_temps, left_on='Order Date', right_on='date', how='inner')
    print(f"Rows after joining dim_temps: {len(df_fact)}")

    if df_fact.empty:
        print("No rows left after matching dim_temps. Aborting insert.")
        return

    # Prepare final fact table dataframe with correct column names
    fact_ventes_df = df_fact[['date_id', 'code', 'client_id', 'magasin_id', 'Sales', 'Quantity']].copy()
    fact_ventes_df.rename(columns={
        'code': 'produit_id',
        'Sales': 'montant',
        'Quantity': 'quantite'
    }, inplace=True)

    # Remove duplicates to respect uniqueness constraints
    fact_ventes_df.drop_duplicates(subset=['date_id', 'produit_id', 'client_id', 'magasin_id'], inplace=True)

    # Insert into fact_ventes table
    try:
        fact_ventes_df.to_sql('fact_ventes', con=engine, if_exists='append', index=False)
        print(f"fact_ventes populated with {len(fact_ventes_df)} rows.")
    except Exception as e:
        print("Error inserting fact_ventes:", e)

if __name__ == "__main__":
    main()
