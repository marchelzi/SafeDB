import os
import gzip
import hashlib
import subprocess
from abc import ABC, abstractmethod
import logging
import mysql.connector as mariadb
import psycopg2
import datetime
import pymssql

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DatabaseBackup(ABC):
    """
    Abstract base class for database backup operations.

    This class defines the interface for database backup operations and provides
    some common utility methods.

    Attributes:
        config (dict): Configuration settings for the database connection.
        db_config (dict): Database-specific configuration settings.
        db_type (str): The type of database (set by subclasses).
    """

    def __init__(self, config, db_config):
        """
        Initialize the DatabaseBackup instance.

        Args:
            config (dict): General configuration settings.
            db_config (dict): Database-specific configuration settings.
        """
        self.config = config
        self.db_config = db_config
        self.db_type = None

    @abstractmethod
    def backup(self, db_name):
        """
        Perform a backup of the specified database.

        Args:
            db_name (str): The name of the database to backup.

        Returns:
            str: The name of the backup file if successful, None otherwise.
        """
        pass

    @abstractmethod
    def get_db_list(self):
        """
        Retrieve a list of databases from the server.

        Returns:
            list: A list of database names.
        """
        pass

    def generate_backup_filename(self, db_name):
        """
        Generate a filename for the backup file.

        Args:
            db_name (str): The name of the database.

        Returns:
            str: The generated filename.
        """
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{self.db_type}_{db_name}_{timestamp}.sql"

    def restore(self, db_name):
        """
        Restore a database from a backup file.

        Args:
            db_name (str): The name of the database to restore.

        Returns:
            str: The name of the backup file used for restoration if successful, None otherwise.
        """
        pass

    @classmethod
    def verify_integrity(cls, filename, expected_hash):
        """
        Verify the integrity of a file using its hash.

        Args:
            filename (str): The name of the file to verify.
            expected_hash (str): The expected hash of the file.

        Returns:
            bool: True if the file's hash matches the expected hash, False otherwise.
        """
        return cls.compute_file_hash(filename) == expected_hash

    def compress_file(self, input_file, output_file):
        """
        Compress a file using gzip compression.

        Args:
            input_file (str): The name of the input file to compress.
            output_file (str): The name of the output compressed file.
        """
        with open(input_file, "rb") as f_in:
            with gzip.open(output_file, "wb") as f_out:
                f_out.writelines(f_in)
        os.remove(input_file)

    def compute_file_hash(self, filename):
        """
        Compute the SHA256 hash of a file.

        Args:
            filename (str): The name of the file to hash.

        Returns:
            str: The hexadecimal representation of the file's SHA256 hash.
        """
        sha256_hash = hashlib.sha256()
        with open(filename, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()


class MariaDBBackup(DatabaseBackup):
    """
    Class for handling MariaDB database backups and restorations.

    This class extends the DatabaseBackup base class and provides specific
    implementations for MariaDB databases.

    Attributes:
        config (dict): Configuration settings for the database connection.
        db_config (dict): Database-specific configuration settings.
        db_type (str): The type of database, set to "MariaDB".
    """

    def __init__(self, config, db_config):
        """
        Initialize the MariaDBBackup instance.

        Args:
            config (dict): General configuration settings.
            db_config (dict): Database-specific configuration settings.
        """
        super().__init__(config, db_config)
        self.db_type = "MariaDB"

    def get_db_list(self):
        """
        Retrieve a list of databases from the MariaDB server.

        *CAUTION*: This method is not used in the current implementation.

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
        """
        Perform a backup of the specified MariaDB database.

        Args:
            db_name (str): The name of the database to backup.

        Returns:
            str: The name of the compressed backup file if successful, None otherwise.
        """
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

    def restore(self, db_name):
        """
        Restore the specified MariaDB database from the most recent backup.

        Args:
            db_name (str): The name of the database to restore.

        Returns:
            str: The name of the backup file used for restoration if successful, None otherwise.
        """
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

        # Find the most recent backup file
        backup_files = [
            f for f in os.listdir() if f.startswith(f"{db_name}_") and f.endswith(".gz")
        ]
        if not backup_files:
            logger.error(f"No backup files found for {db_name}")
            return None

        latest_backup = max(backup_files, key=os.path.getctime)
        uncompressed_file = latest_backup[:-3]  # Remove .gz extension

        try:
            # Decompress the backup file
            self.decompress_file(latest_backup, uncompressed_file)

            # Restore the database
            restore_cmd = [
                "mariadb",
                f"--host={host}",
                f"--port={port}",
                f"--user={user}",
                f"--password={password}",
                db_name,
                "<",
                uncompressed_file,
            ]

            subprocess.run(" ".join(restore_cmd), shell=True, check=True)

            logger.info(f"Successfully restored {db_name} from {latest_backup}")
            return latest_backup
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to restore MariaDB database {db_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"An error occurred while restoring {db_name}: {e}")
            return None
        finally:
            if os.path.exists(uncompressed_file):
                os.remove(uncompressed_file)  # Ensure cleanup


class PostgreSQLBackup(DatabaseBackup):
    """
    A class for handling PostgreSQL database backups and restorations.

    This class extends the DatabaseBackup base class and provides specific
    implementations for PostgreSQL databases.

    Attributes:
        config (dict): Configuration settings for the database connection.
        db_config (dict): Database-specific configuration settings.
        db_type (str): The type of database, set to "PostgreSQL".
    """

    def __init__(self, config, db_config):
        """
        Initialize the PostgreSQLBackup instance.

        Args:
            config (dict): General configuration settings.
            db_config (dict): Database-specific configuration settings.
        """
        super().__init__(config, db_config)
        self.db_type = "PostgreSQL"

    def get_db_list(self):
        """
        Retrieve a list of databases from the PostgreSQL server.

        *CAUTION*: This method is not used in the current implementation.

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
        """
        Perform a backup of the specified PostgreSQL database.

        Args:
            db_name (str): The name of the database to backup.

        Returns:
            str: The name of the compressed backup file if successful, None otherwise.
        """
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

    def restore(self, db_name):
        """
        Restore the specified PostgreSQL database from the most recent backup.

        Args:
            db_name (str): The name of the database to restore.

        Returns:
            str: The name of the backup file used for restoration if successful, None otherwise.
        """
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

        # Find the most recent backup file
        backup_files = [
            f for f in os.listdir() if f.startswith(f"{db_name}_") and f.endswith(".gz")
        ]
        if not backup_files:
            logger.error(f"No backup files found for {db_name}")
            return None

        latest_backup = max(backup_files, key=os.path.getctime)
        uncompressed_file = latest_backup[:-3]  # Remove .gz extension

        try:
            # Decompress the file
            self.decompress_file(latest_backup, uncompressed_file)

            # Restore the database
            psql_cmd = [
                "psql",
                f"--host={host}",
                f"--port={port}",
                f"--username={user}",
                "--set ON_ERROR_STOP=on",
                f"--dbname={db_name}",
                f"--file={uncompressed_file}",
            ]

            env = os.environ.copy()
            env["PGPASSWORD"] = password

            subprocess.run(psql_cmd, env=env, check=True)

            # Clean up the uncompressed file
            os.remove(uncompressed_file)

            logger.info(f"Successfully restored {db_name} from {latest_backup}")
            return latest_backup
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to restore PostgreSQL database {db_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"An error occurred while restoring {db_name}: {e}")
            return None


class MSSQLBackup(DatabaseBackup):
    def __init__(self, config, db_config):
        super().__init__(config, db_config)
        self.db_type = "MSSQL"

    def get_db_list(self):
        try:
            conn = pymssql.connect(
                server=self.config["MSSQL"]["host"],
                user=self.config["MSSQL"]["user"],
                password=self.config["MSSQL"]["password"],
                database="master",
            )
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")
            db_list = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return db_list
        except pymssql.Error as e:
            logger.error(f"Failed to get database list: {e}")
            return []

    def backup(self, db_name):
        # Use database-specific config if available, otherwise use general MSSQL config
        db_creds = (
            self.db_config[db_name]
            if db_name in self.db_config
            else self.config["MSSQL"]
        )

        server = db_creds.get("host", self.config["MSSQL"]["host"])
        user = db_creds.get("user", self.config["MSSQL"]["user"])
        password = db_creds.get("password", self.config["MSSQL"]["password"])

        backup_file = self.generate_backup_filename(db_name)
        compressed_file = f"{backup_file}.gz"

        try:
            conn = pymssql.connect(
                server=server, user=user, password=password, database=db_name
            )
            cursor = conn.cursor()

            backup_query = f"BACKUP DATABASE [{db_name}] TO DISK = N'{backup_file}' WITH NOFORMAT, NOINIT, NAME = N'{db_name}-Full Database Backup', SKIP, NOREWIND, NOUNLOAD, STATS = 10"
            cursor.execute(backup_query)

            while cursor.nextset():
                pass

            cursor.close()
            conn.close()

            self.compress_file(backup_file, compressed_file)
            return compressed_file
        except pymssql.Error as e:
            logger.error(f"Failed to backup MSSQL database {db_name}: {e}")
            return None

    def restore(self, db_name):
        """
        Restore the specified MSSQL database from the most recent backup.

        Args:
            db_name (str): The name of the database to restore.

        Returns:
            str: The name of the backup file used for restoration if successful, None otherwise.
        """
        # Use database-specific config if available, otherwise use general MSSQL config
        db_creds = (
            self.db_config[db_name]
            if db_name in self.db_config
            else self.config["MSSQL"]
        )

        server = db_creds.get("host", self.config["MSSQL"]["host"])
        user = db_creds.get("user", self.config["MSSQL"]["user"])
        password = db_creds.get("password", self.config["MSSQL"]["password"])

        # Find the most recent backup file
        backup_files = [
            f
            for f in os.listdir()
            if f.startswith(f"{self.db_type}_{db_name}_") and f.endswith(".gz")
        ]
        if not backup_files:
            logger.error(f"No backup files found for {db_name}")
            return None

        latest_backup = max(backup_files, key=os.path.getctime)
        uncompressed_file = latest_backup[:-3]  # Remove .gz extension

        try:
            # Decompress the backup file
            self.decompress_file(latest_backup, uncompressed_file)

            # Restore the database
            conn = pymssql.connect(
                server=server, user=user, password=password, database="master"
            )
            cursor = conn.cursor()

            # Set database to single user mode
            cursor.execute(
                f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
            )

            # Restore database
            restore_query = f"""
            RESTORE DATABASE [{db_name}] FROM DISK = N'{uncompressed_file}'
            WITH FILE = 1, NOUNLOAD, REPLACE, STATS = 5
            """
            cursor.execute(restore_query)

            # Set database back to multi user mode
            cursor.execute(f"ALTER DATABASE [{db_name}] SET MULTI_USER")

            cursor.close()
            conn.close()

            logger.info(f"Successfully restored {db_name} from {latest_backup}")
            return latest_backup
        except pymssql.Error as e:
            logger.error(f"Failed to restore MSSQL database {db_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"An error occurred while restoring {db_name}: {e}")
            return None
        finally:
            if os.path.exists(uncompressed_file):
                os.remove(uncompressed_file)  # Ensure cleanup

    def decompress_file(self, compressed_file, output_file):
        """
        Decompress a gzip file.

        Args:
            compressed_file (str): The name of the compressed input file.
            output_file (str): The name of the decompressed output file.
        """
        with gzip.open(compressed_file, "rb") as f_in:
            with open(output_file, "wb") as f_out:
                f_out.write(f_in.read())
