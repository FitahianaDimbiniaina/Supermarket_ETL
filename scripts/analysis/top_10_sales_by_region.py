import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio
from sqlalchemy import text
from scripts.utils.common import extract_sql_block, create_db_engine, export_data, SQL_DIR, EXPORTS_DIR, logger

def run():
    logger.info("Running: top_10_products_per_top_regions")

    engine = create_db_engine()

    # Extract SQL blocks from olap_queries.sql
    top_regions_query = extract_sql_block(SQL_DIR / 'olap_queries.sql', 'top_3_regions_by_sales')
    top_products_query_str = extract_sql_block(SQL_DIR / 'olap_queries.sql', 'top_10_products_by_quantity_in_region')

    # Step 1: get top 3 regions by sales
    top_regions_df = pd.read_sql(top_regions_query, engine)
    top_regions = top_regions_df['Region'].tolist()

    charts_html = []

    # Step 2: For each top region, get top 10 products by quantity and export CSV
    for region in top_regions:
        top_products_query = text(top_products_query_str)
        df = pd.read_sql(top_products_query, engine, params={"region": region})

        # Export CSV for this region
        csv_filename = f"top_10_products_{region.replace(' ', '_')}"
        export_data(df, csv_filename, format_type='csv')

        fig = go.Figure(go.Bar(
            x=df['product_name'],
            y=df['total_sales'],
            marker_color='mediumseagreen'
        ))
        fig.update_layout(
            title=f"Top 10 Products by Quantity in {region}",
            xaxis_title="Product Name",
            yaxis_title="Total Quantity",
            xaxis_tickangle=-45,
            template='plotly_white',
            height=400,
            margin=dict(l=40, r=40, t=60, b=100)
        )

        chart_html = pio.to_html(fig, full_html=False, include_plotlyjs='cdn')
        charts_html.append(chart_html)

    # Step 3: Render all charts in one HTML file
    html_content = f"""
    <html>
    <head>
        <title>Top 10 Products by Region</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .chart-section {{ margin-bottom: 50px; }}
            h2 {{ color: #2c3e50; }}
        </style>
    </head>
    <body>
        <h1>Top 10 Products by Quantity for Top Regions</h1>
        {"".join(f'<div class="chart-section"><h2>{region}</h2>{chart}</div>' for region, chart in zip(top_regions, charts_html))}
    </body>
    </html>
    """

    output_path = EXPORTS_DIR / 'top_products_by_region.html'
    output_path.write_text(html_content, encoding='utf-8')

    logger.info(f"HTML report generated: {output_path}")

    engine.dispose()

if __name__ == "__main__":
    run()
