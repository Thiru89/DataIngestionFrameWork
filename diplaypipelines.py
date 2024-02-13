from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)

# tnt = os.environ.get("client_id")
# print(tnt)

tenant_id = os.environ.get("tenantID")
client_id = os.environ.get("client_id")
client_secret = os.environ.get("client_secret")
subscription_id = os.environ.get("subscription_id")
resource_group_name = os.environ.get("resource_group_name")
data_factory_name = os.environ.get("data_factory_name")


# Create a Service Principal credential
credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret
)

# Create a Data Factory Management Client
adf_client = DataFactoryManagementClient(credential, subscription_id)

# Now you can use adf_client to interact with your Azure Data Factory
# List all pipelines in the Data Factory
pipelines = adf_client.pipelines.list_by_factory(resource_group_name, data_factory_name)

for pipeline in pipelines:
    print(f"Pipeline Name: {pipeline.name}")
