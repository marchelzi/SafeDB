import os
import configparser
from datetime import timedelta
from db import MSSQLBackup, MariaDBBackup, PostgreSQLBackup
from store import LocalStorage, AzureStorage
import logging
import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BackupManager:
    def __init__(self, config_file):
        self.config = self.read_config(config_file)
        self.databases = self.config["General"]["databases"].split(",")
        self.backup_destination = self.config["General"]["backup_destination"]
        self.retention_days = int(self.config["General"]["retention_days"])

    def read_config(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        return config

    def validate_config(self):
        required_sections = [
            "General",
            "MariaDB",
            "PostgreSQL",
            "MSSQL",
            "Local",
            "AzureBlob",
        ]
        for section in required_sections:
            if section not in self.config.sections():
                raise ValueError(f"Missing required section: {section}")

    def get_database_backup(self, db_name, db_type):
        db_creds = (
            self.config[db_name] if db_name in self.config else self.config[db_type]
        )
        if db_type.lower() == "mariadb":
            return MariaDBBackup(self.config, db_creds)
        elif db_type.lower() == "postgresql":
            return PostgreSQLBackup(self.config, db_creds)
        elif db_type.lower() == "mssql":
            return MSSQLBackup(self.config, db_creds)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    def get_storage(self):
        if self.backup_destination == "Local":
            return LocalStorage(self.config)
        elif self.backup_destination == "AzureBlob":
            return AzureStorage(self.config)
        else:
            raise ValueError(
                f"Unsupported backup destination: {self.backup_destination}"
            )

    def db_type(self, db_name):
        return (
            self.config[db_name]["type"]
            if db_name in self.config
            else self.config["General"]["default_db_type"]
        )

    def run_backup(self):
        storage = self.get_storage()

        for db_name in self.databases:
            backup_handler = self.get_database_backup(db_name, self.db_type(db_name))

            backup_file = backup_handler.backup(db_name)
            if backup_file:
                file_hash = backup_handler.compute_file_hash(backup_file)
                logger.info(f"Backup hash for {db_name}: {file_hash}")

                storage.upload(backup_file, db_name, self.db_type(db_name))
                if self.backup_destination == "AzureBlob":
                    os.remove(backup_file)
                    logger.info(f"Deleted local backup: {backup_file}")

        cutoff_date = datetime.datetime.now(datetime.UTC) - timedelta(
            days=self.retention_days
        )
        if self.backup_destination == "Local":
            for db_name in self.databases:
                storage.apply_retention_policy(
                    cutoff_date, db_name, self.db_type(db_name)
                )
        elif self.backup_destination == "AzureBlob":
            storage.apply_retention_policy(cutoff_date)

    def run_restore(self, db_name, db_type):
        backup_handler = self.get_database_backup(db_name, db_type)
        restore = backup_handler.restore(db_name)
        if restore:
            logger.info(f"Restored {db_name} from {restore}")
        else:
            logger.error(f"Failed to restore {db_name}")
