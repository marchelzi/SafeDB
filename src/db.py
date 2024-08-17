import os
import gzip
import hashlib
import subprocess
from abc import ABC, abstractmethod
import logging
import mysql.connector as mariadb
import psycopg2
import datetime

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DatabaseBackup(ABC):
    def __init__(self, config, db_config):
        self.config = config
        self.db_config = db_config
        self.db_type = None

    @abstractmethod
    def backup(self, db_name):
        pass

    @abstractmethod
    def get_db_list(self):
        pass

    def generate_backup_filename(self, db_name):
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{self.db_type}_{db_name}_{timestamp}.sql"

    @classmethod
    def verify_integrity(self, filename, expected_hash):
        return self.compute_file_hash(filename) == expected_hash

    def compress_file(self, input_file, output_file):
        with open(input_file, "rb") as f_in:
            with gzip.open(output_file, "wb") as f_out:
                f_out.writelines(f_in)
        os.remove(input_file)

    def compute_file_hash(self, filename):
        sha256_hash = hashlib.sha256()
        with open(filename, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()


class MariaDBBackup(DatabaseBackup):

    def __init__(self, config, db_config):
        super().__init__(config, db_config)
        self.db_type = "MariaDB"

    def get_db_list(self):
        """
        *CAUTION*: This method is not used in the current implementation.

        Retrieves a list of databases from the MariaDB server.

        Returns:
            list: A list of database names.

        Raises:
            mariadb.Error: If there is an error retrieving the database list.
        """
        host = self.config["MariaDB"]["host"]
        port = self.config["MariaDB"]["port"]
        user = self.config["MariaDB"]["user"]
        password = self.config["MariaDB"]["password"]

        try:
            conn = mariadb.connect(
                host=host, port=port, user=user, password=password, autocommit=True
            )
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            db_list = [db[0] for db in cursor.fetchall()]
            cursor.close()
            conn.close()
            return db_list
        except mariadb.Error as e:
            logger.error(f"Failed to get database list: {e}")
            return []

    def backup(self, db_name):
        # Use database-specific config if available, otherwise use general MariaDB config
        db_creds = (
            self.db_config[db_name]
            if db_name in self.db_config
            else self.config["MariaDB"]
        )

        host = db_creds.get("host", self.config["MariaDB"]["host"])
        port = db_creds.get("port", self.config["MariaDB"]["port"])
        user = db_creds.get("user", self.config["MariaDB"]["user"])
        password = db_creds.get("password", self.config["MariaDB"]["password"])

        backup_file = self.generate_backup_filename(db_name)
        compressed_file = f"{backup_file}.gz"

        mysqldump_cmd = [
            "mariadb-dump",  # Changed from mysqldump to mariadb-dump for consistency with MariaDB
            f"--host={host}",
            f"--port={port}",
            f"--user={user}",
            f"--password={password}",
            "--databases",
            db_name,
        ]

        try:
            subprocess.run(
                ["mariadb-dump", "--version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            with open(backup_file, "w") as f:
                subprocess.run(mysqldump_cmd, stdout=f, check=True)
            self.compress_file(backup_file, compressed_file)
            return compressed_file
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to backup MariaDB database {db_name}: {e}")
            return None


class PostgreSQLBackup(DatabaseBackup):

    def __init__(self, config, db_config):
        super().__init__(config, db_config)
        self.db_type = "PostgreSQL"

    def get_db_list(self):
        """
        *CAUTION*: This method is not used in the current implementation.
        Retrieves a list of databases from the PostgreSQL server.

        Returns:
            list: A list of database names.

        Raises:
            psycopg2.Error: If there is an error connecting to the PostgreSQL server.
        """
        host = self.config["PostgreSQL"]["host"]
        port = self.config["PostgreSQL"]["port"]
        user = self.config["PostgreSQL"]["user"]
        password = self.config["PostgreSQL"]["password"]

        try:
            conn = psycopg2.connect(host=host, port=port, user=user, password=password)
            cursor = conn.cursor()
            cursor.execute("SELECT datname FROM pg_database")
            db_list = [db[0] for db in cursor.fetchall()]
            cursor.close()
            conn.close()
            return db_list
        except psycopg2.Error as e:
            logger.error(f"Failed to get database list: {e}")
            return []

    def backup(self, db_name):
        # Use database-specific config if available, otherwise use general PostgreSQL config
        db_creds = (
            self.db_config[db_name]
            if db_name in self.db_config
            else self.config["PostgreSQL"]
        )

        host = db_creds.get("host", self.config["PostgreSQL"]["host"])
        port = db_creds.get("port", self.config["PostgreSQL"]["port"])
        user = db_creds.get("user", self.config["PostgreSQL"]["user"])
        password = db_creds.get("password", self.config["PostgreSQL"]["password"])

        backup_file = self.generate_backup_filename(db_name)
        compressed_file = f"{backup_file}.gz"

        pg_dump_cmd = [
            "pg_dump",
            f"--host={host}",
            f"--port={port}",
            f"--username={user}",
            "--format=plain",
            "--no-owner",
            f"--file={backup_file}",
            db_name,
        ]

        try:
            subprocess.run(
                ["pg_dump", "--version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            env = os.environ.copy()
            env["PGPASSWORD"] = password
            subprocess.run(pg_dump_cmd, env=env, check=True)
            self.compress_file(backup_file, compressed_file)
            return compressed_file
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to backup PostgreSQL database {db_name}: {e}")
            return None
