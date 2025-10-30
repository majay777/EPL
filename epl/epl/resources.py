from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject

dbt_project_directory = Path(__file__).absolute().parent / "../../epl_dbt"
dbt_project = DbtProject(project_dir=dbt_project_directory)

dbt_resource = DbtCliResource(project_dir=dbt_project)

dbt_manifest_path = dbt_project_directory.joinpath("target", "manifest.json")


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "dbt": dbt_resource,
        }
    )


from dagster import resource
import duckdb


@resource
def duckdb_resource(_):
    # Connect to DuckDB in memory or specify a database file
    connection = duckdb.connect(database=Path(__file__).absolute().parent / "../../epl_duckdb.duckdb",
                                read_only=False)  # Replace with your database path if needed.
    yield connection

    # Close the connection when done
    connection.close()




# # jobs.py
# from dagster import job, op, get_dagster_logger
# from .resources import duckdb_resource
#
# @op(required_resource_keys={'duckdb'})
# def my_op(context):
#     logger = get_dagster_logger()
#     # Use the resource
#     connection = context.resources.duckdb
#
#     # Example query
#     result = connection.execute("SELECT 1 as number").fetchall()
#     logger.info(f"Result: {result}")
#
# @job(resource_defs={"duckdb": duckdb_resource})
# def my_job():
#     my_op()
