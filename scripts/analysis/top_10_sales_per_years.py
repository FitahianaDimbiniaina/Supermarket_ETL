import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio
from sqlalchemy import text
from scripts.utils.common import extract_sql_block, create_db_engine, export_data, SQL_DIR, EXPORTS_DIR, logger

def run():
    logger.info("Running: top_10_products_per_year")

    engine = create_db_engine()

    # Step 1: Get the SQL block
    sql_query_str = extract_sql_block(SQL_DIR / 'olap_queries.sql', 'top_10_products_by_year')
    query = text(sql_query_str)
    df = pd.read_sql(query, engine)

    # Step 2: Loop through each year and export CSV and plot
    charts_html = []
    years = df['annee'].unique()

    for year in years:
        year_df = df[df['annee'] == year].sort_values(by='total_sales', ascending=False)

        # Export CSV
        csv_filename = f"top_10_products_{year}"
        export_data(year_df, csv_filename, format_type='csv')

        # Generate Plotly chart
        fig = go.Figure(go.Bar(
            x=year_df['product_name'],
            y=year_df['total_sales'],
            marker_color='darkorchid'
        ))

        fig.update_layout(
            title=f"Top 10 Products by Sales in {year}",
            xaxis_title="Product Name",
            yaxis_title="Total Sales",
            xaxis_tickangle=-45,
            template='plotly_white',
            height=400,
            margin=dict(l=40, r=40, t=60, b=100)
        )

        chart_html = pio.to_html(fig, full_html=False, include_plotlyjs='cdn')
        charts_html.append((year, chart_html))

    # Step 3: Compile all into an HTML file
    html_content = f"""
    <html>
    <head>
        <title>Top 10 Products by Year</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .chart-section {{ margin-bottom: 50px; }}
            h2 {{ color: #2c3e50; }}
        </style>
    </head>
    <body>
        <h1>Top 10 Products by Sales per Year</h1>
        {"".join(f'<div class="chart-section"><h2>{year}</h2>{chart}</div>' for year, chart in charts_html)}
    </body>
    </html>
    """

    output_path = EXPORTS_DIR / 'top_products_by_year.html'
    output_path.write_text(html_content, encoding='utf-8')

    logger.info(f"HTML report generated: {output_path}")
    engine.dispose()

if __name__ == "__main__":
    run()
