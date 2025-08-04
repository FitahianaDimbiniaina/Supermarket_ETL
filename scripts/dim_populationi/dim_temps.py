import pandas as pd
import holidays
from sqlalchemy import create_engine
from pathlib import Path

# Database connection config
DB_URI = 'postgresql://postgres:4499405@localhost:5432/supermarket_etl'
engine = create_engine(DB_URI)

# Generate date range
start_date = '2010-01-01'
end_date = '2025-12-31'
dates = pd.date_range(start=start_date, end=end_date, freq='D')

# US holidays for 2010-2025
us_holidays = holidays.US(years=range(2010, 2026))

# Create dim_temps dataframe
dim_temps = pd.DataFrame({
    'date': dates,
    'jour': dates.day,
    'semaine': dates.isocalendar().week,
    'mois': dates.month,
    'annee': dates.year,
    'jour_semaine': dates.day_name(),
    'is_weekend': dates.weekday >= 5,
    'is_ferie': [date in us_holidays for date in dates]
})

# Add date_id as an integer sequence starting at 1
dim_temps.insert(0, 'date_id', range(1, len(dim_temps) + 1))

# Load into PostgreSQL
dim_temps.to_sql('dim_temps', engine, if_exists='replace', index=False)

print(f"dim_temps populated with {len(dim_temps)} records (including US holidays).")
