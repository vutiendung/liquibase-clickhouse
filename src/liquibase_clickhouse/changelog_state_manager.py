from clickhouse_driver import Client
import os
from typing import Optional
from datetime import datetime
from .util.id_generator import generate_unique_id_int
import json
import logging # Added logging import

# Get a logger instance for this module.
# Basic configuration is typically done once at the application's entry point (e.g., cli.py).
logger = logging.getLogger(__name__)

class ChangelogStateManager:
    """
    Manages the state of changelog applications in a ClickHouse database.

    This class provides methods to interact with a dedicated state table in ClickHouse
    to track which database changes have been applied, their status, and other metadata.
    It uses the clickhouse_driver to connect and execute queries.

    Attributes:
        client (clickhouse_driver.Client): The ClickHouse database client instance.
        table_name (str): The name of the changelog state table in the database.
    """
    def __init__(self, host: str, port: int, user: str, password: str, database: str, table_name: str = 'changelog_state'):
        """
        Initializes the ChangelogStateManager with ClickHouse connection details.

        Args:
            host (str): The hostname or IP address of the ClickHouse server.
            port (int): The port number for the ClickHouse server.
            user (str): The username for database authentication.
            password (str): The password for database authentication.
            database (str): The name of the database to connect to.
            table_name (str, optional): The name of the table used to store changelog state.
                                        Defaults to 'changelog_state'.
        """
        self.client = Client(host=host, user=user, password=password, database=database, port=port)
        self.table_name = table_name
        logger.debug(f"ChangelogStateManager initialized for database '{database}' on '{host}:{port}' with state table '{table_name}'.")

    def create_state_table(self):
        """
        Creates the changelog state table in the ClickHouse database if it does not already exist.

        The table schema includes fields for change ID, changelog path, type, file path,
        description, start/finish timestamps, status, dependencies, and error messages.
        """
        try:
            self.client.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id Int64,
                change_id String,
                changelog_path String,
                type String,
                file String,
                description String,
                started_at DateTime,
                finished_at DateTime,
                status String,            -- pending, success, failed
                depends_on String,        -- JSON string of dependencies
                error_message String
            ) ENGINE = MergeTree()
            ORDER BY started_at
            """)
            logger.info(f"Changelog state table '{self.table_name}' ensured to exist.")
        except Exception as e:
            logger.error(f"Failed to create or ensure changelog state table '{self.table_name}': {e}")
            raise # Re-raise the exception after logging

    def log_start(self, change, changelog_path: str):
        """
        Logs the start of a changelog application for a specific change.

        The change is initially logged with a 'pending' status.

        Args:
            change (ChangeLog): The ChangeLog object representing the change being applied.
            changelog_path (str): The absolute path to the changelog file defining this change.
        """
        now = datetime.now()
        unique_id = generate_unique_id_int() # Ensure this generates a truly unique ID for the state entry
        try:
            self.client.execute(f"""
                INSERT INTO {self.table_name} (id, change_id, changelog_path, type, file, description, started_at, status, depends_on)
                VALUES ({unique_id}, %(change_id)s, %(changelog_path)s, %(type)s, %(file)s, %(description)s, %(started_at)s, %(status)s, %(depends_on)s)
            """, {
                "change_id": change.id,
                "changelog_path": changelog_path,
                "type": change.type,
                "file": change.file_path,
                "description": change.description,
                "started_at": now,
                "status": "pending",
                "depends_on": change.to_json_depends_on_string()
            })
            logger.info(f"Logged start of change '{change.id}' from '{changelog_path}' with status 'pending'.")
        except Exception as e:
            logger.error(f"Failed to log start of change '{change.id}' from '{changelog_path}': {e}")
            raise

    def update_status(self, change_id: str, changelog_path: str, status: str, error_message: Optional[str] = None):
        """
        Updates the status of a previously logged changelog entry.

        Args:
            change_id (str): The ID of the change to update.
            changelog_path (str): The absolute path to the changelog file where the change is defined.
            status (str): The new status ('success', 'failed').
            error_message (Optional[str], optional): An error message if the status is 'failed'.
                                                     Defaults to None.
        """
        now = datetime.now()
        if error_message is None:
            error_message = ""
        try:
            self.client.execute(f"""
                ALTER TABLE {self.table_name} UPDATE
                    status = %(status)s,
                    finished_at = %(finished_at)s,
                    error_message = %(error_message)s
                WHERE change_id = %(change_id)s AND changelog_path = %(changelog_path)s
            """, {
                "status": status,
                "finished_at": now,
                "error_message": error_message,
                "change_id": change_id,
                "changelog_path": changelog_path
            })
            logger.info(f"Updated status for change '{change_id}' from '{changelog_path}' to '{status}'.")
            if status == "failed":
                logger.error(f"Error details for change '{change_id}': {error_message}")
        except Exception as e:
            logger.error(f"Failed to update status for change '{change_id}' from '{changelog_path}' to '{status}': {e}")
            raise

    def get_activity_by_id(self, change_id: str) -> list:
        """
        Retrieves all activity records for a specific change ID.

        Args:
            change_id (str): The ID of the change to retrieve activity for.

        Returns:
            list: A list of rows (tuples) representing the activity records.
        """
        try:
            rows = self.client.execute(f"""
                SELECT * FROM {self.table_name} WHERE change_id = %(change_id)s
            """, {"change_id": change_id})
            logger.debug(f"Retrieved activity for change_id '{change_id}'. Found {len(rows)} records.")
            return rows
        except Exception as e:
            logger.error(f"Failed to get activity for change_id '{change_id}': {e}")
            raise

    def get_activity_by_changelog_path(self, changelog_path: str) -> list:
        """
        Retrieves all activity records for changes defined in a specific changelog file.

        Args:
            changelog_path (str): The absolute path to the changelog file.

        Returns:
            list: A list of rows (tuples) representing the activity records, ordered by start time.
        """
        try:
            rows = self.client.execute(f"""
                SELECT * FROM {self.table_name} WHERE changelog_path = %(changelog_path)s
                ORDER BY started_at
            """, {"changelog_path": changelog_path})
            logger.debug(f"Retrieved activity for changelog_path '{changelog_path}'. Found {len(rows)} records.")
            return rows
        except Exception as e:
            logger.error(f"Failed to get activity for changelog_path '{changelog_path}': {e}")
            raise

    def get_pending_changes(self) -> list:
        """
        Retrieves all changelog entries that currently have a 'pending' status.

        Returns:
            list: A list of rows (tuples) representing the pending changes.
        """
        try:
            rows = self.client.execute(f"""
                SELECT * FROM {self.table_name} WHERE status = 'pending'
            """)
            logger.debug(f"Retrieved {len(rows)} pending changes.")
            return rows
        except Exception as e:
            logger.error(f"Failed to get pending changes: {e}")
            raise

    def get_successful_changes(self) -> list:
        """
        Retrieves all changelog entries that have been successfully applied.

        Returns:
            list: A list of rows (tuples) representing the successful changes.
        """
        try:
            rows = self.client.execute(f"""
                SELECT * FROM {self.table_name} WHERE status = 'success'
            """)
            logger.debug(f"Retrieved {len(rows)} successful changes.")
            return rows
        except Exception as e:
            logger.error(f"Failed to get successful changes: {e}")
            raise

    def get_failed_changes(self) -> list:
        """
        Retrieves all changelog entries that have failed to apply.

        Returns:
            list: A list of rows (tuples) representing the failed changes.
        """
        try:
            rows = self.client.execute(f"""
                SELECT * FROM {self.table_name} WHERE status = 'failed'
            """)
            logger.debug(f"Retrieved {len(rows)} failed changes.")
            return rows
        except Exception as e:
            logger.error(f"Failed to get failed changes: {e}")
            raise