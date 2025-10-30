import dlt
from datetime import datetime
import requests

@dlt.resource(name="epl_api")
def epl_data():
    data = requests.get("https://api.example.com/epl").json()
    for row in data:
        yield row

@dlt.transformer(data_from=epl_data)
def add_date(row):
    row["load_date"] = datetime.utcnow().isoformat()
    yield row

pipeline = dlt.pipeline(
    pipeline_name="epl_pipeline",
    destination="duckdb",
    dataset_name="epl_data"
)

pipeline.run(add_date())
