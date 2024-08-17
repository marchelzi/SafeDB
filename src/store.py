import os
import subprocess
from abc import ABC, abstractmethod
from datetime import datetime
import logging
from azure.storage.blob import BlobServiceClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class BackupStorage(ABC):
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def upload(self, local_file):
        pass

    @abstractmethod
    def apply_retention_policy(self, cutoff_date):
        pass


class LocalStorage(BackupStorage):
    def upload(self, local_file):
        local_path = self.config["Local"]["backup_path"]
        os.makedirs(local_path, exist_ok=True)
        dest_file = os.path.join(local_path, os.path.basename(local_file))
        os.rename(local_file, dest_file)
        logger.info(f"Backup saved locally: {dest_file}")
        return True

    def apply_retention_policy(self, cutoff_date):
        local_path = self.config["Local"]["backup_path"]
        for filename in os.listdir(local_path):
            file_path = os.path.join(local_path, filename)
            if os.path.isfile(file_path):
                file_date = datetime.fromtimestamp(os.path.getmtime(file_path))
                if file_date < cutoff_date:
                    os.remove(file_path)
                    logger.info(f"Deleted old backup: {filename}")


class AzureStorage(BackupStorage):
    def upload(self, local_file):
        connection_string = self.config["AzureBlob"]["connection_string"]
        container_name = self.config["AzureBlob"]["container_name"]

        try:
            blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
            blob_client = blob_service_client.get_blob_client(
                container=container_name, blob=os.path.basename(local_file)
            )

            with open(local_file, "rb") as data:
                blob_client.upload_blob(data)

            logger.info(f"Uploaded {local_file} to Azure Blob Storage")
            return True
        except Exception as e:
            logger.error(f"Failed to upload to Azure Blob Storage: {e}")
            return False

    def apply_retention_policy(self, cutoff_date):
        connection_string = self.config["AzureBlob"]["connection_string"]
        container_name = self.config["AzureBlob"]["container_name"]

        try:
            blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
            container_client = blob_service_client.get_container_client(container_name)

            blobs = container_client.list_blobs()
            for blob in blobs:
                if blob.last_modified < cutoff_date:
                    container_client.delete_blob(blob.name)
                    logger.info(
                        f"Deleted old backup from Azure Blob Storage: {blob.name}"
                    )
        except Exception as e:
            logger.error(f"Failed to apply retention policy on Azure Blob Storage: {e}")
