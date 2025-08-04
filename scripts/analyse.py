import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.io as pio
from sqlalchemy import create_engine
from pathlib import Path
from typing import List, Optional
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
PROJECT_ROOT = Path(__file__).parent.parent
SQL_DIR = PROJECT_ROOT / 'sql'
EXPORTS_DIR = PROJECT_ROOT / 'exports'
DB_URI = 'postgresql://postgres:4499405@localhost:5432/supermarket_etl'

# Configure Plotly
pio.templates.default = "plotly_white"


def extract_sql_block(filename: Path, block_name: str) -> str:
    """Extracts SQL query from marked block in file with error handling."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()

        blocks = content.split('-- ')
        for block in blocks:
            if block.startswith(block_name):
                return block[len(block_name):].strip()
        raise ValueError(f"Query block '{block_name}' not found.")
    except Exception as e:
        logger.error(f"Error extracting SQL block: {e}")
        raise


def setup_directories() -> None:
    """Ensure required directories exist."""
    try:
        EXPORTS_DIR.mkdir(exist_ok=True, parents=True)
        SQL_DIR.mkdir(exist_ok=True, parents=True)
    except Exception as e:
        logger.error(f"Directory setup failed: {e}")
        raise


def create_db_engine() -> create_engine:
    """Create and return a database engine with connection pooling."""
    try:
        return create_engine(
            DB_URI,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600
        )
    except Exception as e:
        logger.error(f"Database engine creation failed: {e}")
        raise


def export_data(df: pd.DataFrame, filename: str, format_type: str = 'csv') -> None:
    """Export DataFrame to specified format."""
    try:
        export_path = EXPORTS_DIR / f"{filename}.{format_type}"
        if format_type == 'csv':
            df.to_csv(export_path, index=False)
        elif format_type == 'parquet':
            df.to_parquet(export_path)
        logger.info(f"Data exported to {export_path}")
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise


def create_sales_by_region_plot(df: pd.DataFrame) -> px.bar:
    """Create interactive bar plot for sales by region."""
    fig = px.bar(
        df,
        x='region',
        y='total_sales',
        title='Top 10 Sales by Region',
        labels={'total_sales': 'Total Sales (€)', 'region': 'Region'},
        text='total_sales',
        color='region',
        hover_data=['region', 'total_sales']
    )
    fig.update_traces(
        texttemplate='%{text:,.2f}',
        textposition='outside'
    )
    fig.update_layout(
        uniformtext_minsize=8,
        uniformtext_mode='hide',
        xaxis_tickangle=-45
    )
    return fig


def create_products_by_quantity_plot(df: pd.DataFrame) -> px.bar:
    """Create interactive bar plot for products by quantity."""
    fig = px.bar(
        df,
        x='code',
        y='total_qty',
        title='Top 10 Products by Quantity',
        labels={'total_qty': 'Total Quantity', 'code': 'Product Code'},
        text='total_qty',
        color='code',
        hover_data=['code', 'total_qty', 'product_name']
    )
    fig.update_traces(
        texttemplate='%{text:,}',
        textposition='outside'
    )
    return fig


def create_products_by_region_plot(df: pd.DataFrame) -> px.bar:
    """Create grouped bar plot for products by region."""
    fig = px.bar(
        df,
        x='total_quantity',
        y='product_name',
        color='region',
        title='Top 10 Products by Region',
        labels={'total_quantity': 'Total Quantity', 'product_name': 'Product Name'},
        hover_data=['product_name', 'region', 'total_quantity'],
        barmode='group',
        orientation='h'
    )
    fig.update_layout(
        legend_title_text='Region',
        yaxis={'categoryorder': 'total ascending'}
    )
    return fig


def run_analysis(query_name: str, engine: create_engine) -> None:
    """Run analysis for a specific query."""
    try:
        logger.info(f"Starting analysis: {query_name}")
        start_time = datetime.now()
        
        query = extract_sql_block(SQL_DIR / 'olap_queries.sql', query_name)
        df = pd.read_sql(query, engine)
        
        # Export raw data
        export_data(df, query_name, 'parquet')
        
        # Create visualization
        if query_name == 'top_10_sales_by_region':
            fig = create_sales_by_region_plot(df)
        elif query_name == 'top_10_products_by_quantity':
            fig = create_products_by_quantity_plot(df)
        elif query_name == 'top_10_products_by_region_limited':
            fig = create_products_by_region_plot(df)
        else:
            logger.warning(f"No plotting logic defined for: {query_name}")
            return

        # Export interactive visualization
        html_path = EXPORTS_DIR / f"{query_name}.html"
        fig.write_html(
            html_path,
            full_html=False,
            include_plotlyjs='cdn',
            auto_open=False
        )
        
        # Export static image
        img_path = EXPORTS_DIR / f"{query_name}.png"
        fig.write_image(img_path, scale=2)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Completed {query_name} in {duration:.2f} seconds. Outputs saved to {html_path} and {img_path}")
        
    except Exception as e:
        logger.error(f"Error processing {query_name}: {e}", exc_info=True)

def correlation_category_recurrence(engine: create_engine) -> None:
    """Perform correlation analysis between product categories and purchase recurrence."""
    try:
        logger.info("Starting correlation analysis")
        start_time = datetime.now()
        
        query = extract_sql_block(SQL_DIR / 'olap_queries.sql', 'correlation_category_recurrence')
        df = pd.read_sql(query, engine)
        
        # Data processing
        pivot_df = df.pivot_table(
            index='client_id',
            columns='categorie',
            values='purchase_days',
            aggfunc='mean',
            fill_value=0
        )

        # Correlation analysis
        corr_matrix = pivot_df.corr(method='spearman')
        export_data(corr_matrix, 'correlation_category_recurrence', 'csv')
        
        # Visualization
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
        
        # Export visualization
        fig_path = EXPORTS_DIR / 'correlation_category_recurrence.png'
        plt.savefig(fig_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Correlation analysis completed in {duration:.2f} seconds. Output saved to {fig_path}")
        
    except Exception as e:
        logger.error(f"Error in correlation analysis: {e}", exc_info=True)


def main():
    """Main execution function."""
    try:
        setup_directories()
        engine = create_db_engine()
        
        analyses = [
            'top_10_sales_by_region',
            'top_10_products_by_quantity',
            'top_10_products_by_region_limited'
        ]
        
        for analysis in analyses:
            run_analysis(analysis, engine)
            
        correlation_category_recurrence(engine)
        
    except Exception as e:
        logger.critical(f"Fatal error in main execution: {e}", exc_info=True)
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()


if __name__ == "__main__":
    main()