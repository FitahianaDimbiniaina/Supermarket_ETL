import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scripts.utils.common import extract_sql_block, create_db_engine, export_data, SQL_DIR, EXPORTS_DIR, logger
from datetime import datetime

def run():
    logger.info("Running: correlation_category_recurrence")
    engine = create_db_engine()
    query = extract_sql_block(SQL_DIR / 'olap_queries.sql', 'correlation_category_recurrence')
    df = pd.read_sql(query, engine)
    
    pivot_df = df.pivot_table(
        index='client_id',
        columns='categorie',
        values='purchase_days',
        aggfunc='mean',
        fill_value=0
    )

    corr_matrix = pivot_df.corr(method='spearman')
    export_data(corr_matrix, 'correlation_category_recurrence', 'csv')

    plt.figure(figsize=(12, 10))
    sns.heatmap(
        corr_matrix,
        annot=True,
        cmap='coolwarm',
        center=0,
        fmt=".2f",
        linewidths=.5
    )
    plt.title("Correlation Between Product Categories and Purchase Recurrence", pad=20)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(EXPORTS_DIR / 'correlation_category_recurrence.png', dpi=300)
    plt.close()
    logger.info("Exported correlation_category_recurrence.png")
    engine.dispose()
