import json, requests
import os
import base64
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)

#set all prerequisit for Azure databricks
databricks_url = os.environ.get("databricks_url")
create_notebook_endpoint = os.environ.get("create_notebook_endpoint")
run_endpoint = os.environ.get("run_endpoint")
token = os.environ.get("token")


# Example JSON object with SQL code
json_data = '{"sql_code": "select * from democatalog.demoschema.test"}'
data = json.loads(json_data)

# Get the SQL code
sql_code = data.get("sql_code", "")

# Construct the notebook content with SQL code
notebook_content = f"""{sql_code}"""


# Payload to create a new notebook
create_notebook_payload = {
    "language": "sql",
    "content": notebook_content,
    "path": "/Workspace/Shared/test_1/sample" , # Specify the desired path
    "overwrite": "false"  # Do not overwrite if the notebook already exists
}

# Headers for authentication
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Make the API call to create a new notebook
response = requests.post(f"{databricks_url}{create_notebook_endpoint}", json=create_notebook_payload, headers=headers)

