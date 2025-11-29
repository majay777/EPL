import os

from dagster import Definitions
#
#
from dagster_dbt import DbtCliResource, DbtProject

from .assets import dbt_models, dlt_run, fixtures
from .resources import dbt_project_directory
from .schedules import daily_schedule, dbt_schedule

# noqa: TID252
# all_assets = load_assets_from_modules()
# from pathlib import Path
# from dagster import Definitions
# from dagster_dbt import DbtCliResource
#
dbt_project = DbtProject(project_dir=dbt_project_directory)
#
# dbt_assets = load_assets_from_dbt_project(
#     project_dir=dbt_project,
#     profiles_dir=dbt_project,
# )


# dbt_models = dbt_models.
#
# dbt_assets = dbt_assets.with_dependencies(
#     upstream_assets=[AssetKey("load_date")]
# )


defs = Definitions(
    assets=[dbt_models, dlt_run, fixtures],
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_directory))},
    schedules=[daily_schedule, dbt_schedule],
)
