from manager import BackupManager
import typer

app = typer.Typer()


@app.command()
def backup(config_file: str):
    """
    Perform a backup operation using the provided configuration file.

    Args:
        config_file (str): The path to the configuration file.

    Returns:
        None
    """
    manager = BackupManager(config_file)
    manager.run_backup()


@app.command()
def validate(config_file: str):
    """
    Validate the configuration file.

    Parameters:
    - config_file (str): The path to the configuration file.

    Returns:
    None

    Raises:
    - FileNotFoundError: If the configuration file is not found.
    - ValueError: If the configuration file is invalid.

    """

    manager = BackupManager(config_file)
    manager.validate_config()
    print("Config file is valid")


if __name__ == "__main__":
    app()
