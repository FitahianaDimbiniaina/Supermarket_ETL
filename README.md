# 🧠 Loyalty & Sales ETL Pipeline

A modular ETL pipeline designed to process, analyze, and export loyalty and sales data across multiple marts and regions. The project emphasizes clarity, schema alignment, and reusable scripts, enabling seamless integration with frontend tools and dashboards.

---

## 🎯 Purpose

This pipeline transforms raw transactional data into structured insights to support:

- Customer loyalty segmentation
- Sales performance analysis
- Regional and temporal trend tracking

It enables **data-driven decisions** by generating clean, queryable outputs for dashboards, reporting, and further analytics. By creating structured dimension and fact tables, the project ensures consistency and maintainability of downstream analytics.

---

## 📦 Requirements

- PostgreSQL (must be installed and running)  
- Python 3.10+ recommended  
- All dependencies listed in `requirements.txt`  

Install dependencies with:

```bash
pip install -r requirements.txt
```

---

## 📁 Project Structure

```folder
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
├── dim_population/           # Scripts to populate dimension tables (e.g., dim_population)
├── utils/                    # Shared helper functions
└── __pycache__/
sql/
├── olap_queries.sql          # Predefined queries for reporting and analysis
```

---

## ⚙️ Setup & Execution

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Create Database & Load Tables

Navigate to the `scripts` folder and initialize the database with empty tables:

```bash
cd scripts
python load_to_postgre.py
```

### 3. Populate Dimension Table

The `dim_population` script loads population data into the database:

```bash
python dim_population.py
```

### 4. Populate Views

Views are **predefined SQL queries stored in the database** that simplify analysis, reduce repeated query writing, and optimize performance. Populate them with:

```bash
python populate_views.py
```

This reads queries from `sql/olap_queries.sql` and creates corresponding views in the database.

### 5. Run Analysis

Return to the root folder and run the main analysis script:

```bash
cd ..
python main.py
```

This generates processed outputs, aggregates, and metrics for reporting and dashboards.

---

## 📊 Key Outputs

- Loyalty segmentation by store, region, and brand  
- Top-performing products by quantity and category  
- Sales breakdowns by geography and time  
- Customer affinity analysis for targeted marketing  

All exports are organized **by date** for reproducibility, version control, and easy integration with downstream systems.

---

## 🧠 Design Principles

- **Modular scripts** for flexible execution  
- **Clear folder structure** for maintainability  
- **Hybrid SQL + Python logic** for robust transformation pipelines  
- **Reusable utilities** in `utils/` to reduce code duplication  
- **Views** centralize queries, improve performance, and simplify reporting  

---

## 🛣️ Roadmap

- [ ] Add unit tests for transformation logic  
- [ ] Integrate Airflow for scheduling  
- [ ] Build interactive dashboard in `dashboard/`  

---

## 🤝 Contributing

Feel free to fork, open issues, or submit pull requests.  
For questions, reach out via my GitHub profile.  

---

### 🔎 Additional Context

- **Queries**: All analytical queries are stored in `sql/olap_queries.sql`.  
- **Views**: Simplify repeated analysis, provide optimized pre-aggregated tables, and allow analysts to query clean results without touching raw tables.  
- **Dimension tables**: Ensure consistency across multiple marts (e.g., clients, products, stores, time).  
- **Fact tables**: Store transactional metrics like sales, loyalty points, and purchase counts.  
- **Processed data**: Outputs in `data/processed` and `exports/` are ready for visualization or further downstream analytics.  
- **Reproducibility**: Scripts are deterministic; running them with the same raw data always produces the same processed outputs.  
- **Scalability**: Modular design allows easy addition of new marts, dimensions, or analyses without modifying existing logic.

