import os
import subprocess
from abc import ABC, abstractmethod
from datetime import datetime
import logging
from azure.storage.blob import BlobServiceClient
import datetime as dt
from tqdm import tqdm
import shutil
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class BackupStorage(ABC):
    def __init__(self, config):
        self.config = config

    def generate_folder_path(self, db_name, db_type, local_file):
        host = (
            self.config[db_name]["host"]
            if db_name in self.config and "host" in self.config[db_name]
            else self.config[db_type]["host"]
        )
        return os.path.join(host, db_name, os.path.basename(local_file))

    @abstractmethod
    def upload(self, local_file):
        pass

    @abstractmethod
    def apply_retention_policy(self, cutoff_date, db_name=None, db_type=None):
        pass


class LocalStorage(BackupStorage):
    def upload(self, local_file, db_name, db_type):
        local_path = self.config["Local"]["backup_path"]
        os.makedirs(local_path, exist_ok=True)
        os.makedirs(
            self.generate_folder_path(db_name, db_type, local_file).rsplit("/", 1)[0],
            exist_ok=True,
        )
        dest_file = os.path.join(
            local_path, self.generate_folder_path(db_name, db_type, local_file)
        )
        shutil.move(local_file, dest_file)
        logger.info(f"Backup saved locally: {dest_file}")
        return True

    def apply_retention_policy(self, cutoff_date, db_name=None, db_type=None):
        local_path = self.config["Local"]["backup_path"]
        blob_folder = self.generate_folder_path(db_name, db_type, "")
        for root, dirs, files in os.walk(os.path.join(local_path, blob_folder)):
            for filename in files:
                file_path = os.path.join(root, filename)
                file_date = datetime.fromtimestamp(
                    os.path.getmtime(file_path)
                ).astimezone(dt.timezone.utc)
                if file_date < cutoff_date:
                    os.remove(file_path)
                    logger.info(f"Deleted old backup: {filename}")


class AzureStorage(BackupStorage):
    def upload(self, local_file, db_name, db_type):
        connection_string = self.config["AzureBlob"]["connection_string"]
        container_name = self.config["AzureBlob"]["container_name"]

        try:
            blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
            blob_client = blob_service_client.get_blob_client(
                container=container_name,
                blob=self.generate_folder_path(db_name, db_type, local_file),
            )

            file_size = os.path.getsize(local_file)
            with open(local_file, "rb") as data:
                with tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Uploading {local_file}") as pbar:
                    blob_client.upload_blob(data, progress_hook=lambda current, total: pbar.update(current - pbar.n))

            logger.info(f"Uploaded {local_file} to Azure Blob Storage")
            return True
        except Exception as e:
            logger.error(f"Failed to upload to Azure Blob Storage: {e}")
            return False

    def apply_retention_policy(self, cutoff_date, db_name=None, db_type=None):
        connection_string = self.config["AzureBlob"]["connection_string"]
        container_name = self.config["AzureBlob"]["container_name"]
        try:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client(container_name)

            prefix = self.generate_folder_path(db_name, db_type, "") if db_name and db_type else None
            blobs = container_client.list_blobs(name_starts_with=prefix)

            for blob in blobs:
                if blob.last_modified < cutoff_date:
                    container_client.delete_blob(blob.name)
                    logger.info(f"Deleted old backup from Azure Blob Storage: {blob.name}")
        except Exception as e:
            logger.error(f"Failed to apply retention policy on Azure Blob Storage: {e}")

    def generate_folder_path(self, db_name, db_type, file_name):
        return f"{db_type.lower()}/{db_name}/{os.path.basename(file_name)}"
