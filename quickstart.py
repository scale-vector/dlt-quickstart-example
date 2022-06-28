# 4a. Import necessary libaries
import base64
import json
from dlt.common.utils import uniq_id
from dlt.pipeline import Pipeline, GCPPipelineCredentials

# 4b. Create a unique prefix for your demo Google BigQuery table
schema_prefix = 'demo_' + uniq_id()[:4]

# 4c. Name your schema
schema_name = 'example'

# 4d. Name your table
parent_table = 'json_doc'

# 4e. Specify your schema file location
schema_file_path = 'schema.yml'

# 4f. Load credentials for Google BigQuery
with open('credentials.json', 'r') as f:
    gcp_credentials_json = json.load(f)

# Private key needs to be decoded (because we don't want to store it as plain text)
gcp_credentials_json["private_key"] = bytes([_a ^ _b for _a, _b in zip(base64.b64decode(gcp_credentials_json["private_key"]), b"quickstart-sv"*150)]).decode("utf-8")
credentials = GCPPipelineCredentials.from_services_dict(gcp_credentials_json, schema_prefix)

# 5a. Instantiate a pipeline
pipeline = Pipeline(schema_name)

# 5b. Create the pipeline with your credentials
pipeline.create_pipeline(credentials)

# 5b alternative: Reuse existing schema
# schema = Pipeline.load_schema_from_file(schema_file_path)
# pipeline.create_pipeline(credentials, schema=schema)

# 6a. Load JSON document into a dictionary
with open('data.json', 'r', encoding="utf-8") as f:
    data = json.load(f)

# 7a. Extract the dictionary (as an iterator) into SQL table
pipeline.extract(iter(data), table_name=parent_table)

# 7b. Unpack the pipeline into a relational structure
pipeline.unpack()

# 7c. Optional: Save the schema, so you can reuse (and manually edit) it
schema = pipeline.get_default_schema()
schema_yaml = schema.as_yaml(remove_defaults=True)
with open(schema_file_path, 'w') as f:
    f.write(schema_yaml)

# 8a. Load
pipeline.load()

# 8b. Make sure there are no errors
completed_loads = pipeline.list_completed_loads()
# print(completed_loads)
# now enumerate all complete loads if we have any failed packages
# complete but failed job will not raise any exceptions
for load_id in completed_loads:
    print(f"Checking failed jobs in {load_id}")
    for job, failed_message in pipeline.list_failed_jobs(load_id):
        print(f"JOB: {job}\nMSG: {failed_message}")

# 9a. Run SQL queries
def run_query(query):
    df = c._execute_sql(query)
    print(query)
    print(list(df))
    print()

with pipeline.sql_client() as c:

    # Query table for parents
    query = f"SELECT * FROM `{schema_prefix}_example.json_doc`"
    run_query(query)

    # Query table for children
    query = f"SELECT * FROM `{schema_prefix}_example.json_doc__children` LIMIT 1000"
    run_query(query)

    # Join previous two queries via auto generated keys
    query = f"""
        select p.name, p.age, p.id as parent_id,
            c.name as child_name, c.id as child_id, c._dlt_list_idx as child_order_in_list
        from `{schema_prefix}_example.json_doc` as p
        left join `{schema_prefix}_example.json_doc__children`  as c
            on p._dlt_id = c._dlt_parent_id
    """
    run_query(query)
