# src/liquibase_clickhouse/core.py

from abc import ABC, abstractmethod

class IChangeLogExecutor(ABC):
    """
    Abstract Base Class (ABC) defining the interface for a changelog executor.

    Any class that implements this interface must provide concrete implementations
    for `execute_change` and `dry_run` methods. This ensures a consistent way
    to interact with different types of change execution mechanisms.
    """
    @abstractmethod
    def execute_change(self, change: any):
        """
        Executes a given database change.

        This method should contain the logic to apply the change (e.g., run SQL)
        against the target database.

        Args:
            change (Any): The change object to be executed.
                          The exact type of 'change' depends on the concrete
                          implementation (e.g., a SQL string, a ChangeLog object).
        """
        pass

    @abstractmethod
    def dry_run(self, change: any):
        """
        Performs a dry run for a given database change without actually executing it.

        This method should provide feedback on what *would* be executed,
        useful for previewing changes. It should not modify the database state.

        Args:
            change (Any): The change object to be dry-run.
                          The exact type of 'change' depends on the concrete
                          implementation (e.g., a SQL string, a ChangeLog object).
        """
        pass