import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scripts.utils.common import extract_sql_block, create_db_engine, export_data, SQL_DIR, EXPORTS_DIR, logger
from datetime import datetime

def run():
    logger.info("Running: top_10_products_by_region_limited")
    engine = create_db_engine()
    query = extract_sql_block(SQL_DIR / 'olap_queries.sql', 'top_10_products_by_region_limited')
    df = pd.read_sql(query, engine)
    export_data(df, 'top_10_products_by_region_limited', 'parquet')

    # Horizontal grouped barplot
    plt.figure(figsize=(14, 10))
    sns.barplot(
        data=df,
        y='product_name',
        x='total_quantity',
        hue='region',
        dodge=True,
        palette='tab10'
    )
    plt.title("Top 10 Products by Region")
    plt.xlabel("Total Quantity")
    plt.ylabel("Product Name")
    plt.legend(title='Region')
    plt.tight_layout()
    plt.savefig(EXPORTS_DIR / 'top_10_products_by_region_limited.png', dpi=300)
    plt.close()
    logger.info("Exported top_10_products_by_region_limited.png")
    engine.dispose()
