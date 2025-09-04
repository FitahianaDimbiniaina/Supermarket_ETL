import logging
import pandas as pd
from scripts.utils.common import extract_sql_block, create_db_engine, export_data, SQL_DIR, logger
from datetime import datetime
import sys

logging.basicConfig(level=logging.INFO)

def run():
    start_time = datetime.now()
    try:
        logger.info("Running: top_loyal_clients_for_magasin_246")

        engine = create_db_engine()
        query = extract_sql_block(SQL_DIR / 'olap_queries.sql', 'top_loyal_clients_for_magasin_246')
        # logger.info(f"Extracted SQL query preview:\n{query[:200]}")

        df = pd.read_sql(query, engine)

        csv_path = export_data(df, __name__, 'csv')

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Analysis completed in {duration:.2f} seconds")
        logger.info(f"CSV exported to: {csv_path}")

    except Exception as e:
        logger.error(f"Error in top_loyal_clients_for_magasin_246 analysis: {str(e)}", exc_info=True)
        sys.exit(1)

    finally:
        if 'engine' in locals():
            engine.dispose()

if __name__ == '__main__':
    main()