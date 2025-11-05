# src/liquibase_clickhouse/db.py
from .core import IChangeLogExecutor
from clickhouse_driver import Client
import traceback

class ClickHouseExecutor(IChangeLogExecutor):
    def __init__(self, host, port, user, password, database):
        self.client = Client(
            host=host, port=port, user=user, password=password, database=database
        )

    def execute_change(self, change):
        self.client.execute(change)

    def dry_run(self, change):
        print("DRY RUN:", change)