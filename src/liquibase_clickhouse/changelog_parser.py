import os
import yaml
from typing import List, Set, Optional, Dict, Any, Tuple
from collections import defaultdict
from .changelog import ChangeLog, ChangeLogDependency
import logging # Added logging import

# Get a logger instance for this module.
# Basic configuration is typically done once at the application's entry point (e.g., cli.py).
logger = logging.getLogger(__name__)

class ChangelogParser:
    """
    Parses a master changelog YAML file and recursively processes included changelogs
    to collect all defined database changes. It then filters out already applied
    changes and orders the remaining changes based on their dependencies using
    a topological sort.

    This class is responsible for understanding the structure of the changelog
    files and building a coherent list of changes to be applied.
    """
    def __init__(self, master_changelog_path: str, state_manager: Optional[Any] = None):
        """
        Initializes the ChangelogParser.

        Args:
            master_changelog_path (str): The absolute path to the main changelog YAML file.
            state_manager (Optional[Any]): An optional object responsible for tracking applied changes.
                                           It's expected to have a `client` with an `execute` method
                                           and a `table_name` attribute for querying successful changes.

        Raises:
            FileNotFoundError: If the master changelog file does not exist at the specified path.
        """
        if not os.path.isfile(master_changelog_path):
            logger.error(f"Master changelog file not found: {master_changelog_path}")
            raise FileNotFoundError(f"Master changelog file not found: {master_changelog_path}")

        self.master_changelog_path = os.path.abspath(master_changelog_path)
        self.state_manager = state_manager
        # The project_root is derived from the master changelog's directory.
        # All relative paths in changelog dependencies are resolved against this root.
        self.project_root = os.path.dirname(self.master_changelog_path)
        logger.debug(f"ChangelogParser initialized. Master changelog: {self.master_changelog_path}, Project root: {self.project_root}")

    def _load_yaml(self, filepath: str) -> Dict[str, Any]:
        """
        Loads and parses a YAML file safely.

        Args:
            filepath (str): The absolute path to the YAML file to load.

        Returns:
            Dict[str, Any]: The parsed content of the YAML file as a dictionary.

        Raises:
            FileNotFoundError: If the specified YAML file does not exist.
            ValueError: If there's an error parsing the YAML content.
        """
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)
                if not isinstance(content, dict):
                    logger.warning(f"YAML file {filepath} content is not a dictionary. Returning empty dict.")
                    return {}
                logger.debug(f"Successfully loaded YAML file: {filepath}")
                return content
        except FileNotFoundError:
            logger.error(f"Changelog file not found: {filepath}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {filepath}: {e}")
            raise ValueError(f"Error parsing YAML file {filepath}: {e}")

    def _parse_file_recursively(self,
                                filepath: str,
                                all_parsed_changes: List[ChangeLog],
                                processed_files: Set[str],
                                current_changelog_rel_path: str):
        """
        Recursively parses a changelog YAML file and collects all defined changes,
        including their index and dependencies. This method handles `sql` and `yaml`
        change types and prevents circular includes.

        Args:
            filepath (str): The absolute path to the current changelog YAML file being parsed.
            all_parsed_changes (List[ChangeLog]): A list to accumulate all ChangeLog objects found.
            processed_files (Set[str]): A set of relative file paths of changelogs already processed
                                        in the current parsing chain to detect circular dependencies.
            current_changelog_rel_path (str): The relative path of the current changelog file
                                              from the project root, used for ChangeLog objects.

        Raises:
            ValueError: If a change entry has an invalid format, missing required fields,
                        or an unknown change type.
            FileNotFoundError: If a referenced SQL or included YAML file does not exist.
        """
        rel_path_for_processed_files = os.path.relpath(filepath, self.project_root)

        if rel_path_for_processed_files in processed_files:
            logger.warning(f"Circular include detected: {rel_path_for_processed_files}. Skipping to prevent infinite loop.")
            return
        processed_files.add(rel_path_for_processed_files)
        logger.debug(f"Parsing changelog file: {filepath} (Relative: {current_changelog_rel_path})")

        data = self._load_yaml(filepath)
        changes_list = data.get("changes", [])

        for idx, entry in enumerate(changes_list):
            type_ = entry.get("type")
            description = entry.get("description", "")
            file_ref = entry.get("file")
            change_id_raw = entry.get("id")

            # --- PARSING DEPENDENCIES ---
            depends_on_raw = entry.get("depends_on", [])
            parsed_dependencies: List[ChangeLogDependency] = []
            if not isinstance(depends_on_raw, list):
                error_msg = f"Invalid 'depends_on' format in {filepath} for change ID '{change_id_raw}'. Expected a list of dictionaries."
                logger.error(error_msg)
                raise ValueError(error_msg)

            for dep_entry in depends_on_raw:
                if not isinstance(dep_entry, dict):
                    error_msg = f"Invalid dependency format in {filepath} for change ID '{change_id_raw}'. Expected a dictionary with 'changelog_path' and 'change_id'."
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                dep_changelog_path = dep_entry.get("changelog_path")
                dep_change_id = dep_entry.get("change_id")
                if not dep_changelog_path or not dep_change_id:
                    error_msg = f"Missing 'changelog_path' or 'change_id' in dependency for change ID '{change_id_raw}' in {filepath}. Dependency: {dep_entry}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                parsed_dependencies.append(ChangeLogDependency(dep_changelog_path, dep_change_id))
            # --- END PARSING DEPENDENCIES ---

            if type_ == "sql":
                if not file_ref:
                    error_msg = f"SQL change entry is missing 'file' in changelog: {filepath} for change ID '{change_id_raw}'."
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                full_sql_path = os.path.join(os.path.dirname(filepath), file_ref)
                if not os.path.isfile(full_sql_path):
                    error_msg = f"SQL file not found referenced by {filepath}: {full_sql_path} for change ID '{change_id_raw}'."
                    logger.error(error_msg)
                    raise FileNotFoundError(error_msg)

                change_id = change_id_raw if change_id_raw else f"{os.path.basename(file_ref).split('.')[0]}_{idx}"
                change_obj = ChangeLog(change_id, type_, description, full_sql_path,
                                       parsed_dependencies,
                                       changelog_file=current_changelog_rel_path, index=idx)
                all_parsed_changes.append(change_obj)
                logger.debug(f"Added SQL change: ID='{change_obj.id}', File='{change_obj.file_path}', Dependencies={len(parsed_dependencies)}")

            elif type_ == "yaml":
                if not file_ref:
                    error_msg = f"YAML include entry is missing 'file' in changelog: {filepath}."
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                full_yaml_path = os.path.join(os.path.dirname(filepath), file_ref)
                if not os.path.isfile(full_yaml_path):
                    error_msg = f"Included changelog file not found referenced by {filepath}: {full_yaml_path}."
                    logger.error(error_msg)
                    raise FileNotFoundError(error_msg)

                # Recursively parse the included YAML file
                included_changelog_rel_path = os.path.relpath(full_yaml_path, self.project_root)
                self._parse_file_recursively(full_yaml_path, all_parsed_changes, processed_files, included_changelog_rel_path)
                logger.debug(f"Recursively parsed included YAML: {full_yaml_path}")

            else:
                error_msg = f"Unknown change type: '{type_}' in changelog: {filepath} for change ID '{change_id_raw}'."
                logger.error(error_msg)
                raise ValueError(error_msg)

    def get_all_changes(self) -> List[ChangeLog]:
        """
        Parses the master changelog and all recursively included changelogs
        to collect every defined database change without filtering.

        Returns:
            List[ChangeLog]: A list of all ChangeLog objects found across all changelog files.
        """
        logger.info(f"Starting to parse all changes from master changelog: {self.master_changelog_path}")
        all_changes: List[ChangeLog] = []
        processed_files: Set[str] = set()
        master_changelog_rel_path = os.path.relpath(self.master_changelog_path, self.project_root)

        self._parse_file_recursively(self.master_changelog_path, all_changes, processed_files, master_changelog_rel_path)
        logger.info(f"Finished parsing all changes. Found {len(all_changes)} total changes.")
        return all_changes

    def _get_applied_changes_from_state_manager(self) -> Set[Tuple[str, str]]:
        """
        Queries the state manager to get a set of (changelog_path, change_id) for all
        successfully applied changes.

        Returns:
            Set[Tuple[str, str]]: A set of tuples, where each tuple represents a
                                  successfully applied change (changelog_path, change_id).
                                  Returns an empty set if no state manager is provided
                                  or if there's an error querying the state.
        """
        if not self.state_manager:
            logger.debug("No state manager provided. Assuming no changes have been applied.")
            return set()
        try:
            rows = self.state_manager.client.execute(
                f"SELECT changelog_path, change_id FROM {self.state_manager.table_name} WHERE status = 'success'"
            )
            applied_set = set((row[0], row[1]) for row in rows)
            logger.info(f"Retrieved {len(applied_set)} successfully applied changes from state table '{self.state_manager.table_name}'.")
            return applied_set
        except Exception as e:
            logger.warning(f"Could not query state manager for applied changes: {e}. Assuming no changes have been applied.")
            return set()

    def get_unapplied_changes(self) -> List[ChangeLog]:
        """
        Parses all changes, filters out applied ones, and returns a topologically
        sorted list of pending changes, respecting dependencies.

        Dependencies not found in the currently parsed changelog set or already
        applied in the database are considered met and do not create an active
        dependency edge in the graph for pending changes.

        Returns:
            List[ChangeLog]: A list of ChangeLog objects that are considered unapplied,
                             ordered such that all their active dependencies among
                             other pending changes are met first.

        Raises:
            ValueError: If a circular dependency is detected among the pending changes.
        """
        logger.info("Determining unapplied changes and their execution order.")
        all_defined_changes = self.get_all_changes()
        applied_changes_from_db = self._get_applied_changes_from_state_manager()

        # Map for quick lookup: (changelog_path, change_id) -> ChangeLog object
        change_lookup: Dict[Tuple[str, str], ChangeLog] = {
            (c.changelog_file, c.id): c for c in all_defined_changes
        }

        # Identify pending changes (those defined in YAML but not yet applied in DB)
        pending_changes: List[ChangeLog] = []
        for change in all_defined_changes:
            change_identifier = (change.changelog_file, change.id)
            if change_identifier not in applied_changes_from_db:
                pending_changes.append(change)
        logger.info(f"Found {len(pending_changes)} pending changes to consider for application.")

        if not pending_changes:
            logger.info("No pending changes found. Nothing to apply.")
            return []

        # --- Build Dependency Graph for Topological Sort ---
        # Graph stores: (dependency_node) -> [list of nodes that depend on it]
        graph: Dict[Tuple[str, str], List[Tuple[str, str]]] = defaultdict(list)
        # In-degrees store: (node) -> count of its unmet dependencies among pending changes
        in_degrees: Dict[Tuple[str, str], int] = defaultdict(int)

        # Initialize all pending changes in the graph with 0 in-degree.
        # This is crucial for changes that have no dependencies or whose
        # dependencies are already met (either applied or not in `all_defined_changes`).
        for change in pending_changes:
            node_id = (change.changelog_file, change.id)
            in_degrees[node_id] = 0

        # Populate graph and in-degrees for pending changes
        for change in pending_changes:
            node_id = (change.changelog_file, change.id)
            for dependency in change.depends_on:
                dep_node_id = (dependency.changelog_path, dependency.change_id)

                # An active dependency edge is added ONLY IF:
                # 1. The dependency is defined in *any* changelog (`change_lookup`).
                # 2. The dependency has *not* been successfully applied yet (`applied_changes_from_db`).
                # If these conditions are not met, the dependency is considered "met" or irrelevant
                # for the current topological sort of *pending* changes.
                if dep_node_id in change_lookup and dep_node_id not in applied_changes_from_db:
                    graph[dep_node_id].append(node_id) # dep_node_id is a prerequisite for node_id
                    in_degrees[node_id] += 1
                    logger.debug(f"Adding dependency: {dep_node_id} -> {node_id}")
                else:
                    logger.debug(f"Dependency {dep_node_id} for {node_id} is considered met (either applied or not defined).")

        # --- Topological Sort (Kahn's Algorithm) ---
        # Start with all nodes that have no unmet dependencies among the pending changes
        queue: List[Tuple[str, str]] = [node_id for node_id, degree in in_degrees.items() if degree == 0]
        sorted_changes_nodes: List[Tuple[str, str]] = []
        logger.debug(f"Initial nodes with no active dependencies: {queue}")

        while queue:
            current_node = queue.pop(0) # Dequeue a node
            sorted_changes_nodes.append(current_node)
            logger.debug(f"Processing node: {current_node}")

            # For each neighbor (node that depends on current_node)
            for neighbor_node in graph[current_node]:
                in_degrees[neighbor_node] -= 1
                logger.debug(f"Decremented in-degree for {neighbor_node} to {in_degrees[neighbor_node]}")
                if in_degrees[neighbor_node] == 0:
                    queue.append(neighbor_node)
                    logger.debug(f"Added {neighbor_node} to queue as its dependencies are now met.")

        # Check for cycles: If not all pending changes were added to the sorted list, there's a cycle.
        # We compare the count of nodes in the sorted list against the count of nodes that were
        # initially part of the `in_degrees` map (i.e., relevant pending changes).
        if len(sorted_changes_nodes) != len(in_degrees):
            # Identify which pending changes were not sorted due to a cycle
            cyclic_nodes_in_pending = {node_id for node_id in in_degrees if in_degrees[node_id] > 0}
            if cyclic_nodes_in_pending:
                cyclic_change_details = [
                    f"'{change_lookup[c_node_id].id}' from '{change_lookup[c_node_id].changelog_file}'"
                    for c_node_id in cyclic_nodes_in_pending
                ]
                error_msg = f"Circular dependency detected among pending changes involving: {', '.join(cyclic_change_details)}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            else:
                # This case should ideally not happen if Kahn's algorithm is implemented correctly
                # and `in_degrees` is correctly initialized for all relevant pending nodes.
                error_msg = "Graph processing error: Not all pending changes could be sorted, but no cycle detected (unexpected state)."
                logger.error(error_msg)
                raise ValueError(error_msg)

        # Convert sorted node IDs back to ChangeLog objects
        final_sorted_changes: List[ChangeLog] = []
        for node_id in sorted_changes_nodes:
            # Only add changes that are actually pending (not already applied)
            if node_id in change_lookup: # and node_id not in applied_changes_from_db is implicitly handled by how `pending_changes` and `in_degrees` were built
                final_sorted_changes.append(change_lookup[node_id])
        logger.info(f"Successfully determined execution order for {len(final_sorted_changes)} pending changes.")
        return final_sorted_changes