# dbt + DuckDB + Dagster Project

This project uses:

- **dbt** for data transformations
- **DuckDB** as the analytical data store
- **Dagster** for orchestration and scheduled runs

---

## 🗂 Project Structure




---

## 🚀 Setup Instructions

### 1. Create & activate virtual environment
```bash
python -m venv .venv
source .venv/bin/activate      # macOS/Linux
.venv\Scripts\activate         # Windows

2. Install dependencies
pip install -r requirements.txt

# 🦆 Working with DuckDB
duckdb data/warehouse.duckdb

# 🔧 Running dbt
cd epl_dbt
dbt debug
dbt run
dbt test

# ⚙️ Running Dagster UI (Dagit)

dagster dev


