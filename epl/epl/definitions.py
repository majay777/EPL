import os

from dagster import Definitions
#
#
from dagster_dbt import DbtCliResource, DbtProject

from .assets import dbt_models, dlt_run, fixtures
from .resources import dbt_project_directory
from .schedules import daily_schedule, dbt_schedule, asset_new

dbt_project = DbtProject(project_dir=dbt_project_directory)



# defs = Definitions(
#     assets=[dbt_models, dlt_run, fixtures],
#     resources={"dbt": DbtCliResource(project_dir=os.fspath(dbt_project_directory))},
#     schedules=[daily_schedule, dbt_schedule, asset_new],
# )
from .assets_new import load_epl_data, load_epl_fixtures_date

defs = Definitions(
    assets=[dbt_models, dlt_run, fixtures, load_epl_data, load_epl_fixtures_date],
    resources={"dbt": DbtCliResource(project_dir=os.fspath(dbt_project_directory))},
    schedules=[daily_schedule, dbt_schedule, asset_new],
)