from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import re

class AzureBlobStorageManager:
    def __init__(self, connection_str:str, container_name:str, download_dir="."):
        
        self.container_name = container_name
        
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_str)
        self.container_client = self.blob_service_client.get_container_client(container_name)


        # The default directory to which to download a blob.
        self.download_dir = download_dir


    def upload_blob(self, local_path:str,  blob_name=None, overwrite=False) -> None:
        """Upload a local file to blob storage in Azure"""

        # Default blob_name = local filename 
        if blob_name is None:
            blob_name = os.path.basename(local_path)
        blob_client = self.container_client.get_blob_client(blob_name)
        
        try:
            # Upload the blob
            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=overwrite)
            print(f"Blob {blob_name} uploaded successfully.")
        except Exception as e: # Do something with this exception block (e.g. add logging)
            print(f"An error occurred: {str(e)}")

    def list_blobs(self, name_only=True) -> list: 
        """List blobs in the container
        
        Args:

        name_only (bool): "True" to only list the names of blobs, "False" to list entire blob objects.  

        """
        blob_list = self.container_client.list_blobs()
        if name_only: 
            return [blob.name for blob in blob_list]
        else: 
            return list(blob_list)

    def download_blob(self, blob_name:str, local_path=None): 
        """Download a blob from the container. Local path defaults to blob_name"""

        blob_client = self.container_client.get_blob_client(blob_name)

        if local_path is None:
            local_path = os.path.join(self.download_dir, os.path.basename(blob_name)) 
        
        with open(local_path, "wb") as file:
            download_bytes = blob_client.download_blob().readall()
            file.write(download_bytes)