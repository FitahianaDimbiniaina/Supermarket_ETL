# 🧠 Loyalty & Sales ETL Pipeline

A modular ETL pipeline designed to process, analyze, and export loyalty and sales data across multiple marts and regions. The project emphasizes clarity, schema alignment, and modular scripts, enabling easy integration with frontend tools for analytics.

## Requirements

- PostgreSQL must be installed to run this project.
- Python 3.10+ recommended.
- All Python dependencies can be installed from `requirements.txt`.

## Project Folder Structure

```
+---data
|   +---processed      # Cleaned or transformed datasets ready for analysis
|   ---raw             # Raw, original datasets
+---exports
|   +---fact_fidelite_mart  # Processed output tables for loyalty mart analysis
|   ---fact_vente_mart      # Processed output tables for sales mart analysis
+---scripts
|   +---analysis
|   |   +---fact_fidelite_mart  # Analysis scripts for loyalty mart
|   |   +---fact_vente_mart     # Analysis scripts for sales mart
|   +---dim_population          # Scripts to populate dimension tables such as `dim_population`
|   +---utils                   # Helper functions and utilities used across scripts
```

## Setup & Populating the `dim_population` Table

Follow these steps to set up the project and populate the population dimension table:

1. **Install Python dependencies**  
```
pip install -r requirements.txt
```

2. **Create the PostgreSQL database and load initial tables**  
a. Navigate to the `scripts` folder:
```
cd scripts
```
b. Run the script to create the database and empty tables:
```
python load_to_postgre.py
```

3. **Populate the `dim_population` table**  
```
python dim_population/your_script_name.py
```

4. **Populate views for analysis**  
```
python populate_views.py
```

5. **Run your first analysis**  
Navigate back to the project root folder and execute:
```
python main.py
```

### Notes & Recommendations

- Ensure PostgreSQL is running and accessible before executing scripts.  
- All scripts are modular; you can run only the ones you need without executing the entire pipeline.  
- Raw datasets should be placed in `data/raw`. Processed outputs will appear in `data/processed` or `exports` depending on the script.  
- The `utils` folder contains reusable functions to avoid code duplication.
