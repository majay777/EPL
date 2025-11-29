from dagster import define_asset_job, AssetSelection, ScheduleDefinition
from dagster_dbt import build_schedule_from_dbt_selection
import dagster as dg
from .assets import dbt_models, dlt_run, fixtures

api_load = define_asset_job("api_load", selection=AssetSelection.keys('dbt_models'))

# Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
load_schedule = ScheduleDefinition(
    job=api_load,
    cron_schedule="0 6 * * 3,5",
)

schedules = [
    build_schedule_from_dbt_selection(
        [dbt_models],
        job_name="materialize_dbt_models",
        cron_schedule="30 6 * * 3,5",
        dbt_select="fqn:*",
    ), load_schedule
]

daily_schedule = dg.ScheduleDefinition(
    name="daily_refresh",
    cron_schedule="0 0 * * *",  # Runs at midnight daily
    target=[dlt_run, fixtures],
)

dbt_schedule = dg.ScheduleDefinition(
    name="dbt_run",
    cron_schedule="0 0 * * 5",  # Runs at midnight daily
    target=[dbt_models],
)

