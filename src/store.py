import os
import subprocess
from abc import ABC, abstractmethod
from datetime import datetime
import logging
from azure.storage.blob import BlobServiceClient
import datetime as dt
from tqdm import tqdm
import shutil
from typing import Optional

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
    def upload(self, local_file: str, db_name: str, db_type: str) -> bool:
        """
        Upload a local file to the local storage.

        This method moves a specified local file to a designated backup path
        in the local storage system.

        Args:
            local_file (str): The path to the local file to be uploaded.
            db_name (str): The name of the database associated with the file.
            db_type (str): The type of the database associated with the file.

        Returns:
            bool: True if the upload was successful.

        Note:
            This method creates necessary directories if they don't exist.
        """
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

    def apply_retention_policy(
        self,
        cutoff_date: datetime,
        db_name: Optional[str] = None,
        db_type: Optional[str] = None,
    ) -> None:
        """
        Apply the retention policy to local backups.

        This method deletes backup files older than the specified cutoff date.

        Args:
            cutoff_date (datetime): The date before which backups should be deleted.
            db_name (Optional[str]): The name of the database. If None, applies to all databases.
            db_type (Optional[str]): The type of the database. If None, applies to all types.

        Note:
            This method walks through the backup directory and removes files
            that are older than the cutoff date.
        """
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

    def __str__(self):
        return f"LocalStorage(backup_path={self.config['Local']['backup_path']})"

    def __repr__(self):
        return self.__str__()


class AzureStorage(BackupStorage):
    def upload(self, local_file: str, db_name: str, db_type: str) -> bool:
        """
        Upload a local file to Azure Blob Storage.

        This method uploads a specified local file to Azure Blob Storage using the
        configuration provided in the class initialization. It shows a progress bar
        during the upload process.

        Args:
            local_file (str): The path to the local file to be uploaded.
            db_name (str): The name of the database associated with the file.
            db_type (str): The type of the database associated with the file.

        Returns:
            bool: True if the upload was successful, False otherwise.

        Raises:
            Exception: If there's an error during the upload process. The exception
                       is caught and logged, and the method returns False.

        Note:
            This method uses the tqdm library to display a progress bar during upload.
        """
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
                with tqdm(
                    total=file_size,
                    unit="B",
                    unit_scale=True,
                    desc=f"Uploading {local_file}",
                ) as pbar:
                    blob_client.upload_blob(
                        data,
                        progress_hook=lambda current, total: pbar.update(
                            current - pbar.n
                        ),
                    )

            logger.info(f"Uploaded {local_file} to Azure Blob Storage")
            return True
        except Exception as e:
            logger.error(f"Failed to upload to Azure Blob Storage: {e}")
            return False

    def apply_retention_policy(
        self,
        cutoff_date: datetime,
        db_name: Optional[str] = None,
        db_type: Optional[str] = None,
    ) -> None:
        """
        Apply retention policy to Azure Blob Storage backups.

        This method deletes blobs (backup files) from Azure Blob Storage that are older
        than the specified cutoff date. It can optionally filter by database name and type.

        Args:
            cutoff_date (datetime): The date before which backups should be deleted.
            db_name (Optional[str], optional): The name of the database to filter backups. Defaults to None.
            db_type (Optional[str], optional): The type of the database to filter backups. Defaults to None.

        Raises:
            Exception: If there's an error while applying the retention policy.

        Note:
            This method logs information about deleted backups and any errors encountered.
        """
        connection_string = self.config["AzureBlob"]["connection_string"]
        container_name = self.config["AzureBlob"]["container_name"]
        try:
            blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
            container_client = blob_service_client.get_container_client(container_name)

            prefix = (
                self.generate_folder_path(db_name, db_type, "")
                if db_name and db_type
                else None
            )
            blobs = container_client.list_blobs(name_starts_with=prefix)

            for blob in blobs:
                if blob.properties.last_modified < cutoff_date:
                    container_client.delete_blob(blob.name)
                    logger.info(
                        f"Deleted old backup from Azure Blob Storage: {blob.name}"
                    )
        except Exception as e:
            logger.error(f"Failed to apply retention policy on Azure Blob Storage: {e}")

    def generate_folder_path(self, db_name, db_type, file_name):
        """
        Generate a folder path for storing backups in Azure Blob Storage.

        Args:
            db_name (str): The name of the database.
            db_type (str): The type of the database.
            file_name (str): The name of the backup file.

        Returns:
            str: A formatted string representing the folder path.
        """
        return f"{db_type.lower()}/{db_name}/{os.path.basename(file_name)}"

    def __str__(self):
        """
        Return a string representation of the AzureStorage instance.

        Returns:
            str: A string describing the AzureStorage configuration.
        """
        return f"AzureStorage(container={self.config['AzureBlob']['container_name']})"

    def __repr__(self):
        """
        Return a string representation of the AzureStorage instance.

        Returns:
            str: A string representation of the AzureStorage object.
        """
        return self.__str__()
