import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scripts.utils.common import (
    extract_sql_block, 
    create_db_engine, 
    export_data, 
    SQL_DIR, 
    EXPORTS_DIR, 
    logger
)
from datetime import datetime
import sys

logging.basicConfig(level=logging.INFO)

def run():
    start_time = datetime.now()
    try:
        logger.info("Running: top_10_products_by_quantity")

        engine = create_db_engine()
        query = extract_sql_block(SQL_DIR / 'olap_queries.sql', 'top_10_products_by_quantity')
        logger.info(f"Extracted SQL query preview:\n{query[:200]}")

        df = pd.read_sql(query, engine)

        # Export CSV only (no parquet)
        csv_path = export_data(df, 'top_10_products_by_quantity', 'csv')

        sns.set_style("darkgrid")

        plt.figure(figsize=(12, 7))
        bars = plt.bar(df['product_name'], df['total_sales'], color='mediumseagreen', edgecolor='darkgreen', linewidth=0.7)
        plt.title("Top 10 Products by Quantity", fontsize=16, pad=20)
        plt.xlabel("Product Name", labelpad=10)
        plt.ylabel("Total Quantity", labelpad=10)
        plt.xticks(rotation=45, ha='right')

        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval, f"{int(yval):,}", ha='center', va='bottom', fontsize=9)

        plt.tight_layout()

        img_path = EXPORTS_DIR / 'charts' / 'top_10_products_by_quantity.png'
        img_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(img_path, dpi=300)
        plt.close()

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Analysis completed in {duration:.2f} seconds")
        logger.info(f"CSV exported to: {csv_path}")
        logger.info(f"Chart saved to: {img_path}")

    except Exception as e:
        logger.error(f"Error in top_10_products_by_quantity analysis: {str(e)}", exc_info=True)
        sys.exit(1)

    finally:
        if 'engine' in locals():
            engine.dispose()
