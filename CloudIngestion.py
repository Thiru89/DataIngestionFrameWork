from azure.storage.blob import BlobServiceClient,ContainerClient, BlobClient
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)

connection_string =  os.environ.get("connection_string")
storage_account_key = os.environ.get("storage_account_key")
directory = ".\landing"

def uploadBlobIntoContainer(file_path, file_name):
    blob_client = BlobClient.from_connection_string(conn_str=connection_string, container_name="newcontainer", blob_name=file_names)
    with open(file_path, "rb") as data:
        try:
            blob_client.upload_blob(data)
            print("uploaded file", file_names)
        except:
            print("Already uploaded",file_names )


files = os.listdir(directory)
for file_names in files:
    file_path = os.path.join(directory, file_names)
    uploadBlobIntoContainer(file_path, file_names)




# try:
#     container_client = ContainerClient.from_connection_string(connection_string, container_name="newcontainer")
#     container_client.create_container()
# except: 
#     print("Container already exists")

