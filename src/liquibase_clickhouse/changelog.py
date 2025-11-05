# src/liquibase_clickhouse/changelog.py
from typing import List, Optional
from dataclasses import dataclass # Using dataclass for ChangeLogDependency for immutability and clarity
import json

@dataclass(frozen=True) # Dependencies are typically immutable and hashable for set/dict keys
class ChangeLogDependency:
    """Represents a dependency on another ChangeLog entry."""
    changelog_path: str # Relative path to the changelog file that defines the dependency
    change_id: str      # The ID of the change within that file

class ChangeLog:
    """
    Represents a single change (SQL script or included YAML) defined in a changelog file.
    """
    def __init__(self,
                 change_id: str,
                 type_: str,
                 description: str,
                 file_path: str, # Renamed from 'file_' to 'file_path' for consistency
                 depends_on: Optional[List[ChangeLogDependency]] = None, # Expects a list of ChangeLogDependency
                 changelog_file: Optional[str] = None,
                 index: int = -1): # New: Added index with a default value
        
        self.id = change_id
        self.type = type_
        self.description = description
        self.file_path = file_path # Storing as file_path
        self.depends_on = depends_on if depends_on is not None else [] # Ensure it's always a list
        self.changelog_file = changelog_file
        self.index = index # New: Store the index

    def __repr__(self):
        return (
            f"ChangeLog(id={self.id!r}, type={self.type!r}, description={self.description!r}, "
            f"file_path={self.file_path!r}, depends_on={self.depends_on!r}, "
            f"changelog_file={self.changelog_file!r}, index={self.index!r})"
        )

    def __eq__(self, other):
        if not isinstance(other, ChangeLog):
            return NotImplemented
        # Define equality based on the unique identifiers
        return (self.id == other.id and
                self.changelog_file == other.changelog_file and
                self.index == other.index)

    def __hash__(self):
        # Allow ChangeLog objects to be used in sets/dict keys if needed, based on unique identifiers
        return hash((self.id, self.changelog_file, self.index))

    def to_json_depends_on_string(self) -> str:
        """
        Converts the list of ChangeLogDependency objects into a JSON string.
        Each dependency is represented as a dictionary.

        Returns:
            str: A JSON string representing the dependencies. Returns "[]" if no dependencies.
        """
        if not self.depends_on:
            return "[]" # Return an empty JSON array if no dependencies

        # Convert each ChangeLogDependency dataclass instance to a dictionary
        dependencies_as_dicts = [
            {"changelog_path": dep.changelog_path, "change_id": dep.change_id}
            for dep in self.depends_on
        ]
        return json.dumps(dependencies_as_dicts)
