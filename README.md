# 🧠 Loyalty & Sales ETL Pipeline

A modular ETL pipeline for processing, analyzing, and exporting loyalty and sales data across multiple marts and regions. Built for clarity, schema alignment, and frontend-driven control over backend analytics.

Requirements
PostgreSQL must be installed to run this project.

---folder structure---
```
+---data
| +---processed # Cleaned or transformed datasets ready for analysis
| ---raw # Raw, original datasets
+---exports
| +---fact_fidelite_mart # Processed output tables for loyalty analysis
| ---fact_vente_mart # Processed output tables for sales analysis
+---scripts
| +---analysis
| | +---fact_fidelite_mart # Analysis scripts for loyalty mart
| | +---fact_vente_mart # Analysis scripts for sales mart
| +---dim_population # Scripts for population/dimension tables
| +---utils # Helper functions and utilities
```

### Populating the `dim_population` table

To populate the `dim_population` table, follow these steps:
1. install the necessary library
```bash
pip install -r requirements.txt
```
2. create the database
  a. navigate to scripts folder if not already in
```bash
cs scripts
```
  b.Create the database and load the table
```python
python load_to_postgre.py
```
3. Run:
```bash
python dim_population
```
4. Populate the views sql
```python
python populate_views.py
```
5. Run your first analysis
  a. navigate to the root folder and run
```python
python main.py
```
