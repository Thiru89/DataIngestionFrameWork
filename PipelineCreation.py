from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *

import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)
tenant_id = os.environ.get("tenantID")
client_id = os.environ.get("client_id")
client_secret = os.environ.get("client_secret")
subscription_id = os.environ.get("subscription_id")
resource_group_name = os.environ.get("resource_group_name")
data_factory_name = os.environ.get("data_factory_name")


def create_pipeline(data_factory_name, resource_group_name, pipeline_name, activities):

    # Create a Service Principal credential  to authenticate
    credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret
    )
    #credentials = ManagedIdentityCredential()

    adf_client = DataFactoryManagementClient(credential, subscription_id)

    # Define the pipeline
    pipeline = PipelineResource(
        activities=activities
    )

    # Create or update the pipeline
    adf_client.pipelines.create_or_update(
        resource_group_name,
        data_factory_name,
        pipeline_name,
        pipeline
    )

    print(f"Pipeline '{pipeline_name}' created or updated successfully.")

def main():
    # Replace with your Azure subscription ID, resource group, data factory name, and pipeline details
    subscription_id = '20e32f76-73a6-48d3-8647-674bf4c33a2a'
    resource_group_name = 'adf-rg'
    data_factory_name = 'demo-adf-east-us-001'
    pipeline_name = 'TEST'

    # Specify the activities for the pipeline (replace with your actual activities)
    activities= [
        {
            "name": "Wait1",
            "type": "Wait",
            "dependsOn": [],
            "userProperties": [],
            "typeProperties": {
            "waitTimeInSeconds": 5
                }
        }
        ] # type: ignore

    # Create the pipeline
    create_pipeline(data_factory_name, resource_group_name, pipeline_name, activities)
    

if __name__ == "__main__":
    main()
    