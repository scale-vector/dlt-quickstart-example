import base64
import json
from dlt.common.utils import uniq_id
from dlt.pipeline import Pipeline, GCPPipelineCredentials

# Creates a unique prefix for your demo table
schema_prefix = 'demo_' + uniq_id()[:4]

# 5a. Name your schema
schema_name = 'example'

# 5b. Name your table
parent_table = 'example_table'

# 5c. Load credentials
f = open('credentials.json')
gcp_credentials_json = json.load(f)
f.close()

# Private key needs to be decoded (because we don't want to store it as plain text)
gcp_credentials_json["private_key"] = bytes([_a ^ _b for _a, _b in zip(base64.b64decode(gcp_credentials_json["private_key"]), b"quickstart-sv"*150)]).decode("utf-8")

# This the file where the schema will be added
schema_file_path = "schema.yml"

credentials = GCPPipelineCredentials.from_services_dict(gcp_credentials_json, schema_prefix)

# 6a. Instantiate a pipeline
pipeline = Pipeline(schema_name)
pipeline.create_pipeline(credentials)

# 6b. Optional: Reuse existing schema
schema = Pipeline.load_schema_from_file(schema_file_path)

# 6c. Create the pipeline with your credentials
pipeline.create_pipeline(credentials, schema=schema)

# 7a. Load JSON document into a dictionary
f = open('data.json')
data = json.load(f)
f.close()

# 8a. Extract the dictionary into a SQL table
pipeline.extract(iter(data), table_name=parent_table)

# 8b. Unpack the pipeline into a relational structure
pipeline.unpack()

# 8c. Optional: Save the schema, so you can reuse (and manually edit) it
schema = pipeline.get_default_schema()
schema_yaml = schema.as_yaml()
f = open(data_schema_file_path, "a")
f.write(schema_yaml)
f.close()

# Load
pipeline.load()

# 5. Optional error handling - print, raise or handle.

# now enumerate all complete loads if we have any failed packages
# complete but failed job will not raise any exceptions
completed_loads = pipeline.list_completed_loads()
# print(completed_loads)
for load_id in completed_loads:
    print(f"Checking failed jobs in {load_id}")
    for job, failed_message in pipeline.list_failed_jobs(load_id):
        print(f"JOB: {job}\nMSG: {failed_message}")

# now you can use your data

# Tables created:

with pipeline.sql_client() as c:
    query = f"SELECT * FROM `{schema_prefix}_example.my_json_doc`"
    df = c._execute_sql(query)
    print(f"SELECT * FROM `{schema_prefix}_example.my_json_doc`")
    print(list(df))
    print()
    # {  "name": "Ana",  "age": "30",  "id": "456",  "_load_id": "1654787700.406905",  "_record_hash": "5b018c1ba3364279a0ca1a231fbd8d90"}
    # {  "name": "Bob",  "age": "30",  "id": "455",  "_load_id": "1654787700.406905",  "_record_hash": "afc8506472a14a529bf3e6ebba3e0a9e"}

    query = f"SELECT * FROM `{schema_prefix}_example.my_json_doc__children` LIMIT 1000"
    df = c._execute_sql(query)
    print(query)
    print(list(df))
    print()
    # {"name": "Bill", "id": "625", "_parent_hash": "5b018c1ba3364279a0ca1a231fbd8d90", "_pos": "0", "_root_hash": "5b018c1ba3364279a0ca1a231fbd8d90",
    #   "_record_hash": "7993452627a98814cc7091f2c51faf5c"}
    # {"name": "Bill", "id": "625", "_parent_hash": "afc8506472a14a529bf3e6ebba3e0a9e", "_pos": "0", "_root_hash": "afc8506472a14a529bf3e6ebba3e0a9e",
    #   "_record_hash": "9a2fd144227e70e3aa09467e2358f934"}
    # {"name": "Dave", "id": "621", "_parent_hash": "afc8506472a14a529bf3e6ebba3e0a9e", "_pos": "1", "_root_hash": "afc8506472a14a529bf3e6ebba3e0a9e",
    #   "_record_hash": "28002ed6792470ea8caf2d6b6393b4f9"}
    # {"name": "Elli", "id": "591", "_parent_hash": "5b018c1ba3364279a0ca1a231fbd8d90", "_pos": "1", "_root_hash": "5b018c1ba3364279a0ca1a231fbd8d90",
    #   "_record_hash": "d18172353fba1a492c739a7789a786cf"}

    # and we can join them via auto generated keys
    query = f"""
        select p.name, p.age, p.id as parent_id,
            c.name as child_name, c.id as child_id, c._pos as child_order_in_list
        from `{schema_prefix}_example.my_json_doc` as p
        left join `{schema_prefix}_example.my_json_doc__children`  as c
            on p._record_hash = c._parent_hash
    """
    df = c._execute_sql(query)
    print(list(df))
    # {  "name": "Ana",  "age": "30",  "parent_id": "456",  "child_name": "Bill",  "child_id": "625",  "child_order_in_list": "0"}
    # {  "name": "Ana",  "age": "30",  "parent_id": "456",  "child_name": "Elli",  "child_id": "591",  "child_order_in_list": "1"}
    # {  "name": "Bob",  "age": "30",  "parent_id": "455",  "child_name": "Bill",  "child_id": "625",  "child_order_in_list": "0"}
    # {  "name": "Bob",  "age": "30",  "parent_id": "455",  "child_name": "Dave",  "child_id": "621",  "child_order_in_list": "1"}