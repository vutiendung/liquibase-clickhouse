# src/liquibase_clickhouse/core.py

from abc import ABC, abstractmethod

class IChangeLogExecutor(ABC):
    @abstractmethod
    def execute_change(self, change):
        pass

    @abstractmethod
    def dry_run(self, change):
        pass