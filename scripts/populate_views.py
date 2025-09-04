import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import datetime
import sys
from utils.common import extract_sql_block, create_db_engine, SQL_DIR, logger

# Define the views in order: base first, dependent second
VIEWS_ORDER = [
   "Top 3 products by sales in the Eastern region for January 2017", "Total loyalty points per client in March 2017", "Top 5 regions by sales in Q2 2017", "Most sold product categories in June 2017", "Top loyal clients in the Western region for 2017 H2 (Jul-Dec)"
]


def deploy_views():
    start_time = datetime.now()
    try:
        engine = create_db_engine()
        with engine.begin() as conn:
            for view_name in VIEWS_ORDER:
                logger.info(f"Deploying view: {view_name}")
                sql = extract_sql_block(SQL_DIR / "views.sql", view_name)
                conn.execute(text(sql))
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"All views deployed successfully in {duration:.2f} seconds")
    except Exception as e:
        logger.error(f"Failed to deploy views: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if 'engine' in locals():
            engine.dispose()

if __name__ == "__main__":
    deploy_views()
