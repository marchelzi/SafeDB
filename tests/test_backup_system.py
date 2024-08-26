import pytest
from unittest.mock import Mock, patch, mock_open, MagicMock
from datetime import datetime, timedelta
import os
import configparser
from tqdm import tqdm
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

from src.db import DatabaseBackup, MariaDBBackup, PostgreSQLBackup
from src.manager import BackupManager
from src.store import LocalStorage, AzureStorage

# Test DatabaseBackup abstract base class
def test_database_backup_abstract():
    with pytest.raises(TypeError):
        DatabaseBackup({})

# Test MariaDBBackup
@patch('subprocess.run')
@patch('builtins.open', new_callable=mock_open)
@patch('os.remove')
@patch('gzip.open', new_callable=mock_open)
def test_mariadb_backup(mock_gzip_open, mock_remove, mock_file_open, mock_run):
    config = {
        'MariaDB': {
            'host': 'localhost',
            'port': '3306',
            'user': 'root',
            'password': 'password'
        }
    }
    backup = MariaDBBackup(config)

    # Mock the subprocess.run calls
    mock_run.side_effect = [Mock(returncode=0), Mock(returncode=0)]

    result = backup.backup('test_db')

    assert result == 'test_db.sql.gz'
    assert mock_run.call_count == 2
    mock_gzip_open.assert_called_once()
    mock_remove.assert_called_once_with('test_db.sql')

# Test PostgreSQLBackup
@patch('subprocess.run')
@patch('builtins.open', new_callable=mock_open)
@patch('os.remove')
@patch('gzip.open', new_callable=mock_open)
def test_postgresql_backup(mock_gzip_open, mock_remove, mock_file_open, mock_run):
    config = {
        'PostgreSQL': {
            'host': 'localhost',
            'port': '5432',
            'user': 'postgres',
            'password': 'password'
        }
    }
    backup = PostgreSQLBackup(config)

    # Mock the subprocess.run calls
    mock_run.side_effect = [Mock(returncode=0), Mock(returncode=0)]

    result = backup.backup('test_db')

    assert result == 'test_db.sql.gz'
    assert mock_run.call_count == 2
    mock_gzip_open.assert_called_once()
    mock_remove.assert_called_once_with('test_db.sql')

# Fixture for creating a dummy config file
@pytest.fixture
def dummy_config(tmp_path):
    config = configparser.ConfigParser()
    config['General'] = {
        'databases': 'db1,db2',
        'backup_destination': 'local',
        'retention_days': '7',
        'default_db_type': 'mariadb'
    }
    config['db1'] = {'type': 'mariadb'}
    config['db2'] = {'type': 'postgresql'}
    config['Local'] = {'backup_path': str(tmp_path / 'backups')}
    config['MariaDB'] = {
        'host': 'localhost',
        'port': '3306',
        'user': 'root',
        'password': 'password'
    }
    config['PostgreSQL'] = {
        'host': 'localhost',
        'port': '5432',
        'user': 'postgres',
        'password': 'password'
    }

    config_path = tmp_path / "dummy_config.ini"
    with open(config_path, 'w') as configfile:
        config.write(configfile)

    return str(config_path)

class CaseInsensitiveDict(dict):
    def __getitem__(self, key):
        return super().__getitem__(key.lower())

    def __contains__(self, key):
        return super().__contains__(key.lower())

# Test BackupManager
@patch('configparser.ConfigParser')
@patch('src.manager.MariaDBBackup')
@patch('src.manager.PostgreSQLBackup')
@patch('src.manager.LocalStorage')
@patch('builtins.open', new_callable=mock_open)
@patch('os.path.exists')
@patch('os.remove')
def test_backup_manager(mock_remove, mock_exists, mock_file_open, mock_local_storage, mock_postgresql, mock_mariadb, mock_config_parser, dummy_config):
    # Create a mock config object
    mock_config = CaseInsensitiveDict({
        'General': {'databases': 'db1,db2', 'backup_destination': 'local', 'retention_days': '7', 'default_db_type': 'mariadb'},
        'db1': {'type': 'mariadb'},
        'db2': {'type': 'postgresql'},
        'Local': {'backup_path': '/tmp/backups'},
        'MariaDB': {'host': 'localhost', 'port': '3306', 'user': 'root', 'password': 'password'},
        'PostgreSQL': {'host': 'localhost', 'port': '5432', 'user': 'postgres', 'password': 'password'}
    })
    mock_config_parser.return_value.read.return_value = None
    mock_config_parser.return_value.__getitem__.side_effect = mock_config.__getitem__
    mock_config_parser.return_value.__contains__ = mock_config.__contains__

    manager = BackupManager(dummy_config)

    # Mock the backup methods
    mock_mariadb_instance = Mock()
    mock_mariadb_instance.backup.return_value = 'db1.sql.gz'
    mock_mariadb_instance.compute_file_hash.return_value = 'hash1'
    mock_mariadb.return_value = mock_mariadb_instance

    mock_postgresql_instance = Mock()
    mock_postgresql_instance.backup.return_value = 'db2.sql.gz'
    mock_postgresql_instance.compute_file_hash.return_value = 'hash2'
    mock_postgresql.return_value = mock_postgresql_instance

    # Mock the storage
    mock_storage = Mock()
    mock_local_storage.return_value = mock_storage

    # Mock file operations
    mock_exists.return_value = True
    mock_file_open.return_value.__enter__.return_value.read.return_value = b'mock file content'

    manager.run_backup()

    assert mock_mariadb_instance.backup.call_count == 1
    assert mock_postgresql_instance.backup.call_count == 1
    assert mock_storage.upload.call_count == 2
    assert mock_storage.apply_retention_policy.call_count == 1
    mock_file_open.assert_any_call('db1.sql.gz', 'rb')
    mock_file_open.assert_any_call('db2.sql.gz', 'rb')


# Test LocalStorage
@patch('os.rename')
@patch('os.makedirs')
def test_local_storage_upload(mock_makedirs, mock_rename):
    config = {'Local': {'backup_path': '/tmp/backups'}}
    storage = LocalStorage(config)

    result = storage.upload('/tmp/test_backup.sql.gz')

    assert result is True
    mock_makedirs.assert_called_once_with('/tmp/backups', exist_ok=True)
    mock_rename.assert_called_once_with('/tmp/test_backup.sql.gz', '/tmp/backups/test_backup.sql.gz')

@patch('os.path.getmtime')
@patch('os.remove')
@patch('os.path.join')
@patch('os.listdir')
def test_local_storage_retention_policy(mock_listdir, mock_join, mock_remove, mock_getmtime):
    config = {'Local': {'backup_path': '/tmp/backups'}}
    storage = LocalStorage(config)

    mock_listdir.return_value = ['old_backup.sql.gz', 'new_backup.sql.gz']
    mock_join.side_effect = lambda *args: '/'.join(args)
    current_time = datetime.now()
    mock_getmtime.side_effect = [
        (current_time - timedelta(days=10)).timestamp(),
        (current_time - timedelta(days=5)).timestamp()
    ]

    cutoff_date = current_time - timedelta(days=7)
    storage.apply_retention_policy(cutoff_date)

    mock_remove.assert_called_once_with('/tmp/backups/old_backup.sql.gz')

# Test AzureStorage
@patch('azure.storage.blob.BlobServiceClient.from_connection_string')
def test_azure_storage_upload(mock_blob_service):
    config = {
        'AzureBlob': {
            'connection_string': 'dummy_connection_string',
            'container_name': 'backups'
        }
    }
    storage = AzureStorage(config)

    mock_blob_client = Mock()
    mock_blob_service.return_value.get_blob_client.return_value = mock_blob_client

    with patch('builtins.open', new_callable=mock_open):
        result = storage.upload('/tmp/test_backup.sql.gz')

    assert result is True
    mock_blob_client.upload_blob.assert_called_once()

@patch('azure.storage.blob.BlobServiceClient.from_connection_string')
def test_azure_storage_retention_policy(mock_blob_service):
    config = {
        'AzureBlob': {
            'connection_string': 'dummy_connection_string',
            'container_name': 'backups'
        }
    }
    storage = AzureStorage(config)

    mock_container_client = Mock()
    mock_blob_service.return_value.get_container_client.return_value = mock_container_client

    current_time = datetime.now()
    old_blob = Mock()
    old_blob.name = 'old_backup.sql.gz'
    old_blob.last_modified = current_time - timedelta(days=10)
    new_blob = Mock()
    new_blob.name = 'new_backup.sql.gz'
    new_blob.last_modified = current_time - timedelta(days=5)
    mock_container_client.list_blobs.return_value = [old_blob, new_blob]

    cutoff_date = current_time - timedelta(days=7)
    storage.apply_retention_policy(cutoff_date)

    mock_container_client.delete_blob.assert_called_once_with('old_backup.sql.gz')

@pytest.fixture
def azure_config():
    return {
        'AzureBlob': {
            'connection_string': 'dummy_connection_string',
            'container_name': 'backups'
        }
    }

@pytest.fixture
def azure_storage(azure_config):
    return AzureStorage(azure_config)

@patch('azure.storage.blob.BlobServiceClient.from_connection_string')
@patch('os.path.getsize')
def test_azure_storage_upload_success(mock_getsize, mock_blob_service, azure_storage):
    mock_getsize.return_value = 1024  # 1 KB file size
    mock_blob_client = Mock()
    mock_blob_service.return_value.get_blob_client.return_value = mock_blob_client

    with patch('builtins.open', new_callable=mock_open, read_data=b'test data'):
        result = azure_storage.upload('/tmp/test_backup.sql.gz', 'test_db', 'mariadb')

    assert result is True
    mock_blob_client.upload_blob.assert_called_once()
    mock_blob_service.return_value.get_blob_client.assert_called_once_with(
        container='backups',
        blob='mariadb/test_db/test_backup.sql.gz'
    )

@patch('azure.storage.blob.BlobServiceClient.from_connection_string')
def test_azure_storage_upload_failure(mock_blob_service, azure_storage):
    mock_blob_service.return_value.get_blob_client.side_effect = Exception("Connection error")

    result = azure_storage.upload('/tmp/test_backup.sql.gz', 'test_db', 'mariadb')

    assert result is False

@patch('azure.storage.blob.BlobServiceClient.from_connection_string')
def test_azure_storage_apply_retention_policy(mock_blob_service, azure_storage):
    mock_container_client = Mock(spec=ContainerClient)
    mock_blob_service.return_value.get_container_client.return_value = mock_container_client

    current_time = datetime.now()
    old_blob = Mock()
    old_blob.name = 'mariadb/test_db/old_backup.sql.gz'
    old_blob.last_modified = current_time - timedelta(days=10)
    new_blob = Mock()
    new_blob.name = 'mariadb/test_db/new_backup.sql.gz'
    new_blob.last_modified = current_time - timedelta(days=5)
    mock_container_client.list_blobs.return_value = [old_blob, new_blob]

    cutoff_date = current_time - timedelta(days=7)
    azure_storage.apply_retention_policy(cutoff_date, 'test_db', 'mariadb')

    mock_container_client.delete_blob.assert_called_once_with('mariadb/test_db/old_backup.sql.gz')
    assert mock_container_client.delete_blob.call_count == 1

@patch('azure.storage.blob.BlobServiceClient.from_connection_string')
def test_azure_storage_apply_retention_policy_exception(mock_blob_service, azure_storage):
    mock_blob_service.return_value.get_container_client.side_effect = Exception("Connection error")

    cutoff_date = datetime.now() - timedelta(days=7)
    azure_storage.apply_retention_policy(cutoff_date)

if __name__ == '__main__':
    pytest.main()
