---
description: Instructions to reproduce the EPL (English Premier League) data project

---

# EPL Project Reproduction Guide

This project is an EPL (English Premier League) data pipeline using dbt, DuckDB, Dagster, and Streamlit.

## Prerequisites

- Python 3.11 or higher
- Git

## Step 1: Clone the Repository

```bash
git clone <repository-url>
cd EPL_New
```

## Step 2: Create Virtual Environment

```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
# or
source .venv/bin/activate  # macOS/Linux
```

## Step 3: Install Dependencies

Using pip:
```bash
pip install -r requirements.txt
```

Or using uv (if available):
```bash
uv sync
```

## Step 4: Environment Configuration

Create a `.env` file in the project root with any necessary environment variables (check with project maintainers for required variables).

## Step 5: Setup dbt

Navigate to the dbt project directory and install dbt packages:

```bash
cd epl_dbt
dbt deps
```

Verify dbt configuration:
```bash
dbt debug
```

## Step 6: Initialize DuckDB Database

The project uses DuckDB as the data store. The database file will be created automatically when you run the pipeline.

## Step 7: Run Dagster

Start the Dagster UI from the project root:

```bash
cd epl
dagster dev
```

This launches the Dagster UI at: http://localhost:3000

From the Dagster UI, you can:
- View and materialize assets
- Run scheduled jobs
- Monitor pipeline execution

## Step 8: Run dbt Models

From the epl_dbt directory:

```bash
cd epl_dbt
dbt run
dbt test
```

## Step 9: Run Streamlit App

From the project root:

```bash
streamlit run streamlit_app/main_app.py
```

Or for the alternative app:

```bash
streamlit run streamlit_app/app_4.py
```

## Project Structure

```
EPL_New/
├── epl_dbt/          # dbt models, seeds, macros
├── epl/epl/          # Dagster jobs, assets & ops
├── streamlit_app/    # Streamlit visualization apps
├── data/             # Data storage directory
└── requirements.txt  # Python dependencies
```

## Key Components

- **dbt**: Data transformation models in `epl_dbt/models/`
- **Dagster**: Orchestration assets in `epl/epl/assets.py` and `epl/epl/assets_new.py`
- **dlt**: Data loading pipeline for EPL API data
- **Streamlit**: Interactive dashboards in `streamlit_app/`
- **DuckDB**: Analytical database for storing transformed data

## Troubleshooting

If you encounter issues:
1. Ensure all dependencies are installed correctly
2. Check that the virtual environment is activated
3. Verify dbt configuration in `epl_dbt/dbt_project.yml`
4. Check Dagster logs for asset execution errors
5. Ensure DuckDB has write permissions in the data directory
