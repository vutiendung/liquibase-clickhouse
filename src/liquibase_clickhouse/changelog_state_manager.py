from clickhouse_driver import Client
import os
from datetime import datetime
from .util.id_generator import generate_unique_id_int
import json

class ChangelogStateManager:
    def __init__(self, host, port, user, password, database, table_name='changelog_state'):
        self.client = Client(host=host, user=user, password=password, database=database, port=port)
        self.table_name = table_name

    def create_state_table(self):
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
            depends_on String,
            error_message String
        ) ENGINE = MergeTree()
        ORDER BY started_at
        """)

    def log_start(self, change, changelog_path):
        # Log as "pending"
        now = datetime.now()
        id = generate_unique_id_int()
        self.client.execute(f"""
            INSERT INTO {self.table_name} (id, change_id, changelog_path, type, file, description, started_at, status, depends_on)
            VALUES ({id}, %(change_id)s, %(changelog_path)s, %(type)s, %(file)s, %(description)s, %(started_at)s, %(status)s, %(depends_on)s)
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

    def update_status(self, change_id, changelog_path, status, error_message=None):
        now = datetime.now()
        if error_message is None:
            error_message = ""
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

    def get_activity_by_id(self, change_id):
        rows = self.client.execute(f"""
            SELECT * FROM {self.table_name} WHERE change_id = %(change_id)s
        """, {"change_id": change_id})
        return rows

    def get_activity_by_changelog_path(self, changelog_path):
        rows = self.client.execute(f"""
            SELECT * FROM {self.table_name} WHERE changelog_path = %(changelog_path)s
            ORDER BY started_at
        """, {"changelog_path": changelog_path})
        return rows

    def get_pending_changes(self):
        rows = self.client.execute(f"""
            SELECT * FROM {self.table_name} WHERE status = 'pending'
        """)
        return rows

    def get_successful_changes(self):
        rows = self.client.execute(f"""
            SELECT * FROM {self.table_name} WHERE status = 'success'
        """)
        return rows

    def get_failed_changes(self):
        rows = self.client.execute(f"""
            SELECT * FROM {self.table_name} WHERE status = 'failed'
        """)
        return rows