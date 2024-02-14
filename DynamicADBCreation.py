import json
import requests
import base64
# Databricks workspace URL
databricks_url = "https://adb-6975897661226333.13.azuredatabricks.net"

# Databricks REST API endpoint to create a new notebook
create_notebook_endpoint = "/api/2.0/workspace/import"

# Databricks REST API endpoint to run a notebook
run_endpoint = "/api/2.0/jobs/runs/submit"

# Databricks access token
token = "dapi55107f4150f7d6c0580e5bbbff900e27-3"

# Example JSON object with SQL code
json_data = '{"sql_code": "INSERT INTO democatalog.demoschema.test values (3,'')"}'
data = json.loads(json_data)

# Get the SQL code
sql_code = data.get("sql_code", "")

# Construct the notebook content with SQL code
notebook_content = f"""
{sql_code}
"""

# Encode the notebook content in base64
encoded_content = base64.b64encode(notebook_content.encode()).decode()

# Payload to create a new notebook
create_notebook_payload = {
    "language": "sql",
    "content": encoded_content,
    "path": "/Workspace/Shared/test_1/sample",  # Specify the desired path
    "overwrite": "true"  # Do not overwrite if the notebook already exists
}


# Headers for authentication
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Make the API call to create a new notebook
response = requests.post(f"{databricks_url}{create_notebook_endpoint}", json=create_notebook_payload, headers=headers)

# Check the response
if response.status_code == 200:
    print("Notebook created successfully.")
else:
    print(f"Failed to create notebook. Response code: {response.status_code}, Response text: {response.text}")


notebook_path ='/Workspace/Shared/test_1/sample'
# Payload to run the notebook
run_notebook_payload = {
    "run_name": "MyRun",
    "existing_cluster_id": "0101-165609-yxj3in2s",  # Use an existing cluster or create one dynamically
    "notebook_task": {"notebook_path": notebook_path}
    
}

print(notebook_path)
# Make the API call to run the notebook
response = requests.post(f"{databricks_url}{run_endpoint}", json=run_notebook_payload, headers=headers)

# Check the response
if response.status_code == 200:
    print("Notebook run successfully initiated.")
else:
    print(f"Failed to run notebook. Response code: {response.status_code}, Response text: {response.text}")

# print(sql_code)