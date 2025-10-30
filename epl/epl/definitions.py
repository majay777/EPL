import os

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from .assets import dbt_models, dlt_run, fixtures


from .resources import dbt_project_directory

# noqa: TID252

# all_assets = load_assets_from_modules()

defs = Definitions(
    assets=[dbt_models, dlt_run, fixtures],
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_directory))},
)
