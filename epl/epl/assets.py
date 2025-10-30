import json
from datetime import datetime
from typing import Mapping, Any, Optional

import dagster as dg
import dlt
import requests
from dagster import AssetKey, asset
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

from .resources import dbt_project


class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        resource_type = dbt_resource_props['resource_type']
        name = dbt_resource_props['name']
        if resource_type == "source":
            return AssetKey(f"{name}")
        else:
            return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return dbt_resource_props['fqn'][1]


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=CustomizedDagsterDbtTranslator())
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@asset(compute_kind="python", group_name='ingested')
def dlt_run() -> None:
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'

    def stream_download_jsonl(url1):
        response = requests.get(url1, stream=True)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        for line in response.iter_lines():
            if line:
                yield json.loads(line)

    pipeline = dlt.pipeline(
        pipeline_name="epl_duckdb",
        destination=dlt.destinations.duckdb("../../EPL_New/epl_duckdb.duckdb"),
        dataset_name="epl_data",
    )
    # The response contains a list of issues
    pipeline.run(stream_download_jsonl(url), table_name="epl_raw_table", write_disposition="append")


@asset(deps=[dlt_run], compute_kind="python", group_name='ingested')
def fixtures() -> None:
    """Getting information related to EPL fixtures and Match Results through API."""

    url = "https://fantasy.premierleague.com/api/fixtures/"

    # function to download the json data
    def stream_download_jsonl(link):
        response = requests.get(url)
        response.raise_for_status()
        load_date = datetime.now().isoformat()  # or datetime.now()
        for item in response.json():
            item["load_date"] = load_date
            yield item

    # dlt pipeline to load data to duckdb. Pipeline name refers to database name and dataset_name to schema in db.
    pipeline = dlt.pipeline(
        pipeline_name="epl_duckdb",
        destination=dlt.destinations.duckdb("../../EPL_New/epl_duckdb.duckdb"),
        dataset_name="epl_data",
    )

    # Running pipeline
    # The response contains a list of issues
    pipeline.run(stream_download_jsonl(url), table_name="Matches", write_disposition="append")
