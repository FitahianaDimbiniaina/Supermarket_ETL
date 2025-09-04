from utils.common import create_db_engine, logger
import pandas as pd
from sqlalchemy import text
from faker import Faker

engine = create_db_engine()
num_magasin = 3947
fake = Faker()

with engine.begin() as conn:  # ⬅ use begin() for automatic commit
    # Fetch magasin_ids
    df_magasin = pd.read_sql("SELECT magasin_id FROM dim_magasin ORDER BY magasin_id", conn)

    # Generate random names
    df_magasin['magasin_name'] = [fake.company() for _ in range(len(df_magasin))]

    # Update each row
    for _, row in df_magasin.iterrows():
        update_sql = text(
            "UPDATE dim_magasin SET magasin_name = :name WHERE magasin_id = :mid"
        )
        conn.execute(update_sql, {"name": row['magasin_name'], "mid": row['magasin_id']})

    logger.info("Populated 'magasin_name' for all magasins.")
