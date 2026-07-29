from datetime import datetime
from pathlib import Path
import dlt
import requests
from dagster import asset

DUCKDB_PATH = str(Path(__file__).resolve().parents[2] / "epl_duckdb.duckdb")


@dlt.resource(name="epl_api")
def epl_data():
    data = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/").json()
    for row in data["elements"]:
        yield row


@dlt.resource(name="epl_api_fixtures")
def epl_fixtures_data():
    data = requests.get("https://fantasy.premierleague.com/api/fixtures/").json()
    for row in data:
        yield row


@dlt.transformer(data_from=epl_data)
def add_date(row):
    row["load_date"] = datetime.utcnow()
    yield row


@dlt.transformer(data_from=epl_fixtures_data)
def add_date_fixtures(row):
    row["load_date"] = datetime.utcnow()
    yield row


pipeline = dlt.pipeline(
    pipeline_name="epl_pipeline",
    destination=dlt.destinations.duckdb(DUCKDB_PATH),
    dataset_name="epl_data",
)


# @dlt.transformer(data_from=epl_data)
# def add_date(row):
#     ...

@asset(compute_kind="python", group_name="Ingest_epl_data")
def load_epl_data():
    pipeline.run(add_date(), table_name="epl_raw_table", write_disposition="append")


@asset(deps=[load_epl_data], compute_kind="python", group_name="Ingest_epl_data")
def load_epl_fixtures_date():
    pipeline.run(
        add_date_fixtures(), table_name="Matches", write_disposition="append"
    )
