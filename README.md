🧠 Loyalty & Sales ETL Pipeline
A modular ETL pipeline designed to process, analyze, and export loyalty and sales data across multiple marts and regions. The project emphasizes clarity, schema alignment, and reusable scripts, enabling seamless integration with frontend tools and dashboards.

🎯 Purpose
This pipeline transforms raw transactional data into structured insights to support:
- Customer loyalty segmentation
- Sales performance analysis
- Regional and temporal trend tracking
It enables data-driven decisions by generating clean, queryable outputs for dashboards, reporting, and further analytics.

📦 Requirements
- PostgreSQL (must be installed and running)
- Python 3.10+ recommended
- Dependencies listed in requirements.txt

📁 Project Structure
data/
├── raw/                  # Raw input datasets
└── processed/            # Cleaned and transformed outputs

exports/
├── fact_fidelite_mart/   # Loyalty analysis exports
└── fact_vente_mart/      # Sales analysis export
scripts/
├── analysis/
│   ├── fact_fidelite_mart/   # Loyalty analysis scripts
│   ├── fact_vente_mart/      # Sales analysis scripts
│   └── __pycache__/
├── dim_population/           # Dimension table population scripts
├── utils/                    # Shared helper functions
└── __pycache__/

sql/                          # SQL queries for transformation and export

⚙️ Setup & Execution
1. Install Dependencies
```python
pip install -r requirements.txt
```

2. Create Database & Load Tables
```cd scripts```
```python
python load_to_postgre.py
```


3. Populate Dimension Table
```python
python dim_population.py
```


4. Populate Views
```python
python populate_views.py
```

5. Run Analysis
```
cd ..
```
```python
python main.py
```



📊 Key Outputs
- Loyalty segmentation by store, region, and brand
- Top-performing products by quantity and category
- Sales breakdowns by geography and time
- Customer affinity analysis for targeted marketing
All exports are organized by date for reproducibility and version control.

🧠 Design Principles
- Modular scripts for flexible execution
- Clear folder structure for maintainability
- Hybrid SQL + Python logic for transformation
- Reusable utilities to reduce code duplication

🛣️ Roadmap
- [ ] Add unit tests for transformation logic
- [ ] Integrate Airflow for scheduling
- [ ] Build interactive dashboard in dashboard/

🤝 Contributing
Feel free to fork, open issues, or submit pull requests. For questions, reach out via GitHub profile
