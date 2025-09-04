# 🧠 Loyalty & Sales ETL Pipeline

A modular ETL pipeline for processing, analyzing, and exporting loyalty and sales data across multiple marts and regions. Built for clarity, schema alignment, and frontend-driven control over backend analytics.

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

1. Open a terminal and navigate to the `scripts` folder:
```
cd scripts
```
```
python dim_population
```
