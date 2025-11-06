# src/liquibase_clickhouse/db.py
from .core import IChangeLogExecutor
from clickhouse_driver import Client
import traceback
import logging # Added logging import

# Get a logger instance for this module.
# Basic configuration is typically done once at the application's entry point (e.g., cli.py).
logger = logging.getLogger(__name__)

class ClickHouseExecutor(IChangeLogExecutor):
    """
    Implements the IChangeLogExecutor interface for ClickHouse databases.

    This class provides the concrete logic for connecting to a ClickHouse instance
    and executing SQL changes or performing dry runs.
    """
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        """
        Initializes the ClickHouseExecutor with database connection parameters.

        Args:
            host (str): The hostname or IP address of the ClickHouse server.
            port (int): The port number for the ClickHouse server.
            user (str): The username for database authentication.
            password (str): The password for database authentication.
            database (str): The name of the database to connect to.
        """
        try:
            self.client = Client(
                host=host, port=port, user=user, password=password, database=database
            )
            logger.info(f"ClickHouseExecutor initialized. Connected to {user}@{host}:{port}/{database}")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse database {database}@{host}:{port}: {e}")
            raise # Re-raise the exception after logging

    def execute_change(self, sql_change: str):
        """
        Executes a given SQL change against the connected ClickHouse database.

        Args:
            sql_change (str): The SQL string representing the change to be executed.

        Raises:
            Exception: If the SQL execution fails.
        """
        logger.info("Executing SQL change...")
        logger.debug(f"SQL to execute:\n{sql_change[:200]}...") # Log first 200 chars of SQL
        try:
            self.client.execute(sql_change)
            logger.info("SQL change executed successfully.")
        except Exception as e:
            logger.error(f"Failed to execute SQL change: {e}")
            logger.debug(f"SQL that failed:\n{sql_change}")
            logger.debug(traceback.format_exc()) # Log full traceback for debugging
            raise # Re-raise the exception after logging

    def dry_run(self, sql_change: str):
        """
        Simulates the execution of a SQL change by logging it, without
        actually sending it to the ClickHouse database.

        Args:
            sql_change (str): The SQL string representing the change to be dry-run.
        """
        logger.info("DRY RUN: Would execute the following SQL change:")
        logger.info(f"\n{sql_change}\n--- END DRY RUN SQL ---")