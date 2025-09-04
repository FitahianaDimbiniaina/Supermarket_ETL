import logging
import pandas as pd
from scripts.utils.common import create_db_engine, export_data, extract_sql_block, SQL_DIR, logger
from datetime import datetime
import sys

logging.basicConfig(level=logging.INFO)

def run():
    start_time = datetime.now()
    try:
        logger.info("Running: top_10_loyal_magasins")

        engine = create_db_engine()
        query = extract_sql_block(SQL_DIR / 'olap_queries.sql', 'top_10_magasins_by_loyal_customers')
        df = pd.read_sql(query, engine)

        if df.empty:
            logger.info("No data found for loyal clients.")
            return

        csv_path = export_data(df, __name__, 'csv')
        logger.info(f"CSV exported to: {csv_path}")

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Analysis completed in {duration:.2f} seconds")

    except Exception as e:
        logger.error(f"Error in top_10_loyal_magasins: {str(e)}", exc_info=True)
        sys.exit(1)

    finally:
        if 'engine' in locals():
            engine.dispose()
            
if __name__ == '__main__':
    run()