import pandas as pd
from sqlalchemy import create_engine

user = "fitahiana"
password = "4499405"
host = "localhost"
port = 5432
database = "supermarket_etl"


engine = create_engine('postgresql://postgres:4499405@localhost:5432/supermarket_etl')


schemas = {
    "dim_produit": {
        "code": 'str',
        "libelle": 'str',
        "categorie": 'str',
    },
    "dim_client": {
        "client_id": 'int',
        "age": 'int',
        "sexe": 'str',
        "fidelite": 'bool',
        "ville": 'str'
    },
    "dim_temps": {
        "date_id": 'int',
        "date": 'datetime64[ns]',
        "jour": 'int',
        "semaine": 'int',
        "mois": 'int',
        "annee": 'int',
        "is_ferie": 'bool',
        "is_weekend": 'bool',
        "jour_semaine": 'str'
    },
    "dim_magasin": {
        "magasin_id": 'int',
        "ville": 'str',
        "region": 'str',
        "surface": 'int'
    }
}

for table_name, columns in schemas.items():
    df = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in columns.items()})
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"Created empty table '{table_name}'")

print("All tables created successfully.")
