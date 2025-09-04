
from utils.common import create_db_engine, logger
import pandas as pd
from sqlalchemy import text
from faker import Faker

engine = create_db_engine()
fake = Faker()

with engine.begin() as conn:  # auto-commit transaction
    # Add client_name column if not exists
    conn.execute(text("""
        ALTER TABLE dim_client
        ADD COLUMN IF NOT EXISTS client_name TEXT
    """))

    # Fetch client_ids
    df_client = pd.read_sql("SELECT client_id FROM dim_client ORDER BY client_id", conn)

    # Generate random names
    df_client['client_name'] = [fake.name() for _ in range(len(df_client))]

    # Update each row
    for _, row in df_client.iterrows():
        update_sql = text(
            "UPDATE dim_client SET client_name = :name WHERE client_id = :cid"
        )
        conn.execute(update_sql, {"name": row['client_name'], "cid": row['client_id']})

    logger.info("Populated 'client_name' for all clients.")
