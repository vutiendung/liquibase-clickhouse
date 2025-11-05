import os
import yaml
from typing import List, Set, Optional, Dict, Any
from .changelog import ChangeLog

class ChangelogParser:
    """
    Parses a master changelog YAML file and recursively processes included changelogs
    to collect all defined database changes. It can also filter out already applied
    changes using an optional state manager.
    """
    def __init__(self, master_changelog_path: str, state_manager: Optional[Any] = None):
        """
        Initializes the ChangelogParser.

        Args:
            master_changelog_path (str): The absolute or relative path to the main changelog YAML file.
            state_manager (Optional[Any]): An optional object responsible for tracking applied changes.
                                           It's expected to have a `client` with an `execute` method
                                           and a `table_name` attribute for querying.
        Raises:
            FileNotFoundError: If the master changelog file does not exist.
        """
        if not os.path.isfile(master_changelog_path):
            raise FileNotFoundError(f"Master changelog file not found: {master_changelog_path}")

        self.master_changelog_path = os.path.abspath(master_changelog_path)
        self.state_manager = state_manager
        # The root of the project is considered the directory containing the master changelog
        self.project_root = os.path.dirname(self.master_changelog_path)

    def _load_yaml(self, filepath: str) -> Dict[str, Any]:
        """
        Loads and parses a YAML file safely.

        Args:
            filepath (str): The path to the YAML file.

        Returns:
            Dict[str, Any]: The parsed YAML content as a dictionary.

        Raises:
            FileNotFoundError: If the YAML file does not exist.
            ValueError: If there's an error parsing the YAML content.
        """
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)
                # Ensure content is a dictionary, return empty dict for empty/invalid YAML
                return content if isinstance(content, dict) else {}
        except FileNotFoundError:
            raise FileNotFoundError(f"Changelog file not found: {filepath}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file {filepath}: {e}")

    def _parse_file_recursively(self,
                                filepath: str,
                                all_parsed_changes: List[ChangeLog],
                                processed_files: Set[str],
                                current_changelog_rel_path: str):
        """
        Recursively parses a changelog YAML file and collects all defined changes.

        Args:
            filepath (str): The absolute path to the current changelog YAML file to parse.
            all_parsed_changes (List[Change]): A list to accumulate all discovered Change objects.
            processed_files (Set[str]): A set of relative file paths already processed to prevent circular includes.
            current_changelog_rel_path (str): The relative path (from project_root) of the YAML file
                                               that is currently being processed. This is used to
                                               attribute changes to their defining changelog.

        Raises:
            FileNotFoundError: If a referenced SQL or YAML file does not exist.
            ValueError: If an unknown change type is encountered or a required field is missing.
        """
        rel_path = os.path.relpath(filepath, self.project_root)

        if rel_path in processed_files:
            return  # Prevent circular includes
        processed_files.add(rel_path)

        data = self._load_yaml(filepath)
        changes_list = data.get("changes", [])

        for idx, entry in enumerate(changes_list):
            type_ = entry.get("type")
            description = entry.get("description", "")
            file_ref = entry.get("file") # Renamed from 'file_' to avoid shadowing built-in 'file'
            depends_on = entry.get("depends_on")
            change_id_raw = entry.get("id")

            if type_ == "sql":
                if not file_ref:
                    raise ValueError(f"SQL change entry is missing 'file' in changelog: {filepath}")

                full_path = os.path.join(os.path.dirname(filepath), file_ref)
                if not os.path.isfile(full_path):
                    raise FileNotFoundError(f"SQL file not found referenced by {filepath}: {full_path}")

                rel_file_path = os.path.relpath(full_path, self.project_root)
                # Generate a default ID if not provided, using the SQL file name and index
                change_id = change_id_raw if change_id_raw else f"{os.path.basename(file_ref).split('.')[0]}_{idx}"
                all_parsed_changes.append(ChangeLog(change_id, type_, description, rel_file_path, depends_on, changelog_file=current_changelog_rel_path))

            elif type_ == "yaml":
                if not file_ref:
                    raise ValueError(f"YAML include entry is missing 'file' in changelog: {filepath}")

                full_path = os.path.join(os.path.dirname(filepath), file_ref)
                if not os.path.isfile(full_path):
                    raise FileNotFoundError(f"Included changelog file not found referenced by {filepath}: {full_path}")

                # Recurse, passing the relative path of the *included* YAML as the new current_changelog_rel_path
                self._parse_file_recursively(full_path, all_parsed_changes, processed_files, os.path.relpath(full_path, self.project_root))

            else:
                raise ValueError(f"Unknown change type: '{type_}' in changelog: {filepath}")

    def get_all_changes(self) -> List[ChangeLog]:
        """
        Parses the master changelog and all recursively included changelogs
        to collect every defined database change without filtering.

        Returns:
            List[Change]: A list of all Change objects found in the changelog structure.
        """
        all_changes: List[ChangeLog] = []
        processed_files: Set[str] = set()
        master_changelog_rel_path = os.path.relpath(self.master_changelog_path, self.project_root)

        self._parse_file_recursively(self.master_changelog_path, all_changes, processed_files, master_changelog_rel_path)
        return all_changes

    def get_unapplied_changes(self) -> List[ChangeLog]:
        """
        Parses all changes defined in the changelog structure and filters out
        those that have already been applied, using the provided state manager.

        If no state manager is configured, all defined changes are returned.

        Returns:
            List[Change]: A list of Change objects that are considered unapplied.
        """
        all_defined_changes = self.get_all_changes()

        if not self.state_manager:
            return all_defined_changes # If no state manager, all changes are considered unapplied

        unapplied_changes: List[ChangeLog] = []
        
        # Group changes by their originating changelog file for efficient database queries
        changes_by_changelog_path: Dict[str, List[ChangeLog]] = {}
        for change in all_defined_changes:
            changes_by_changelog_path.setdefault(change.changelog_file, []).append(change)

        for changelog_rel_path, changes_in_file in changes_by_changelog_path.items():
            try:
                # Assuming state_manager.client.execute returns iterable of (id,) tuples
                # for changes that have been successfully applied in this specific changelog file.
                already_run_ids = set(
                    row[0] for row in self.state_manager.client.execute(
                        f"""SELECT change_id FROM {self.state_manager.table_name}
                        WHERE id = (SELECT MAX(id) FROM {self.state_manager.table_name}
                            WHERE changelog_path = %(changelog_path)s AND status = 'success')
                            AND changelog_path = %(changelog_path)s AND status = 'success'
                        """,
                        {"changelog_path": changelog_rel_path}
                    )
                )

                found = False
                for change in changes_in_file:
                    if len(already_run_ids) == 0:
                        found = True
                    if change.id in already_run_ids and len(already_run_ids) > 0:
                        found = True
                    if change.id not in already_run_ids and found:
                        unapplied_changes.append(change)
            except Exception as e:
                print(f"Warning: Could not query state manager for changelog '{changelog_rel_path}': {e}. Assuming all changes in this file are unapplied.")
                unapplied_changes.extend(changes_in_file)

        return unapplied_changes