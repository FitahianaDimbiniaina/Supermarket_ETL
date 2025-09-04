import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:4499405@localhost:5432/supermarket_etl")

POINT_CONVERSION_RATE = 0.1

with engine.connect() as conn:
    df_fact = pd.read_sql("SELECT * FROM fact_ventes", conn)

df_fact['points_earned'] = df_fact['montant'] * POINT_CONVERSION_RATE
df_fact['points_redeemed'] = 0 

df_fidelite = df_fact[['date_id', 'client_id', 'magasin_id', 'points_earned', 'points_redeemed', 'montant', 'quantite']].copy()

df_fidelite = df_fidelite.groupby(['date_id', 'client_id', 'magasin_id'], as_index=False).sum()

df_fidelite.to_sql('fact_fidelite', engine, if_exists='replace', index=False)
