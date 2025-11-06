# src/liquibase_clickhouse/changelog.py
from typing import List, Optional
from dataclasses import dataclass
import json
import logging # Added logging import

# Get a logger instance for this module.
# Basic configuration is typically done once at the application's entry point (e.g., cli.py).
logger = logging.getLogger(__name__)

@dataclass(frozen=True) # Dependencies are typically immutable and hashable for set/dict keys
class ChangeLogDependency:
    """
    Represents a dependency on another ChangeLog entry.

    This dataclass defines a specific dependency by referencing the changelog file
    and the unique ID of the change within that file.
    It's frozen to ensure immutability, which is good for using it in sets or as dictionary keys.

    Attributes:
        changelog_path (str): The relative path to the changelog file that defines the dependency.
        change_id (str): The unique ID of the change within that file.
    """
    changelog_path: str
    change_id: str

class ChangeLog:
    """
    Represents a single change (e.g., SQL script, included YAML) defined in a changelog file.

    This class encapsulates all necessary information about a database change,
    including its unique identifier, type, description, the path to its associated
    file (if any), and any dependencies on other changes.

    Attributes:
        id (str): A unique identifier for this change within its changelog file.
        type (str): The type of change (e.g., 'sql', 'include').
        description (str): A brief description of the change.
        file_path (str): The absolute path to the SQL script or included YAML file
                         associated with this change.
        depends_on (List[ChangeLogDependency]): A list of other ChangeLog entries
                                                that this change depends on.
        changelog_file (Optional[str]): The absolute path to the changelog file
                                        where this change is defined.
        index (int): The 0-based index of this change within its defining changelog file.
                     Used for stable ordering and unique identification.
    """
    def __init__(self,
                 change_id: str,
                 type_: str,
                 description: str,
                 file_path: str,
                 depends_on: Optional[List[ChangeLogDependency]] = None,
                 changelog_file: Optional[str] = None,
                 index: int = -1):
        """
        Initializes a new ChangeLog instance.

        Args:
            change_id (str): A unique identifier for this change within its changelog file.
            type_ (str): The type of change (e.g., 'sql', 'include').
            description (str): A brief description of the change.
            file_path (str): The absolute path to the SQL script or included YAML file
                             associated with this change.
            depends_on (Optional[List[ChangeLogDependency]]): A list of ChangeLogDependency
                                                              objects representing other
                                                              changes this one depends on.
                                                              Defaults to an empty list.
            changelog_file (Optional[str]): The absolute path to the changelog file
                                            where this change is defined.
            index (int): The 0-based index of this change within its defining changelog file.
                         Defaults to -1 if not specified.
        """
        self.id = change_id
        self.type = type_
        self.description = description
        self.file_path = file_path
        self.depends_on = depends_on if depends_on is not None else []
        self.changelog_file = changelog_file
        self.index = index

        # Optional: Add a debug log for when a ChangeLog object is created
        # logger.debug(f"ChangeLog object created: ID={self.id}, File={self.changelog_file}, Index={self.index}")

    def __repr__(self):
        """
        Provides a string representation of the ChangeLog object for debugging.
        """
        return (
            f"ChangeLog(id={self.id!r}, type={self.type!r}, description={self.description!r}, "
            f"file_path={self.file_path!r}, depends_on={self.depends_on!r}, "
            f"changelog_file={self.changelog_file!r}, index={self.index!r})"
        )

    def __eq__(self, other):
        """
        Compares two ChangeLog objects for equality.
        Equality is based on the unique combination of ID, changelog file, and index.
        """
        if not isinstance(other, ChangeLog):
            return NotImplemented
        return (self.id == other.id and
                self.changelog_file == other.changelog_file and
                self.index == other.index)

    def __hash__(self):
        """
        Computes a hash value for the ChangeLog object.
        This allows ChangeLog objects to be used in sets or as dictionary keys.
        The hash is based on the unique identifiers: ID, changelog file, and index.
        """
        return hash((self.id, self.changelog_file, self.index))

    def to_json_depends_on_string(self) -> str:
        """
        Converts the list of ChangeLogDependency objects into a JSON string.
        Each dependency is represented as a dictionary with 'changelog_path' and 'change_id'.

        Returns:
            str: A JSON string representing the dependencies. Returns "[]" if no dependencies.
        """
        if not self.depends_on:
            return "[]"

        dependencies_as_dicts = [
            {"changelog_path": dep.changelog_path, "change_id": dep.change_id}
            for dep in self.depends_on
        ]
        return json.dumps(dependencies_as_dicts)
