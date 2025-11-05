import os
import yaml
from typing import List, Set, Optional, Dict, Any, Tuple
from collections import defaultdict
from .changelog import ChangeLog, ChangeLogDependency # Import the new dependency class

class ChangelogParser:
    """
    Parses a master changelog YAML file and recursively processes included changelogs
    to collect all defined database changes. It then filters out already applied
    changes and orders the remaining changes based on their dependencies.
    """
    def __init__(self, master_changelog_path: str, state_manager: Optional[Any] = None):
        """
        Initializes the ChangelogParser.

        Args:
            master_changelog_path (str): The absolute path to the main changelog YAML file.
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
        """
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)
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
        Recursively parses a changelog YAML file and collects all defined changes,
        including their index and dependencies.
        """
        rel_path_for_processed_files = os.path.relpath(filepath, self.project_root)

        if rel_path_for_processed_files in processed_files:
            return  # Prevent circular includes
        processed_files.add(rel_path_for_processed_files)

        data = self._load_yaml(filepath)
        changes_list = data.get("changes", [])

        for idx, entry in enumerate(changes_list):
            type_ = entry.get("type")
            description = entry.get("description", "")
            file_ref = entry.get("file")
            
            # --- PARSING DEPENDENCIES ---
            depends_on_raw = entry.get("depends_on", []) # Expect a list of dicts now
            parsed_dependencies: List[ChangeLogDependency] = []
            if not isinstance(depends_on_raw, list):
                raise ValueError(f"Invalid 'depends_on' format in {filepath} for change ID '{entry.get('id', 'N/A')}'. Expected a list of dictionaries.")

            for dep_entry in depends_on_raw:
                if not isinstance(dep_entry, dict):
                    raise ValueError(f"Invalid dependency format in {filepath} for change ID '{entry.get('id', 'N/A')}'. Expected a dictionary with 'changelog_path' and 'change_id'.")
                dep_changelog_path = dep_entry.get("changelog_path")
                dep_change_id = dep_entry.get("change_id")
                if not dep_changelog_path or not dep_change_id:
                    raise ValueError(f"Missing 'changelog_path' or 'change_id' in dependency for change ID '{entry.get('id', 'N/A')}' in {filepath}.")
                parsed_dependencies.append(ChangeLogDependency(dep_changelog_path, dep_change_id))
            # --- END PARSING DEPENDENCIES ---

            change_id_raw = entry.get("id")

            if type_ == "sql":
                if not file_ref:
                    raise ValueError(f"SQL change entry is missing 'file' in changelog: {filepath}")

                # Use ABSOLUTE path for file_path
                full_sql_path = os.path.join(os.path.dirname(filepath), file_ref)
                if not os.path.isfile(full_sql_path):
                    raise FileNotFoundError(f"SQL file not found referenced by {filepath}: {full_sql_path}")

                change_id = change_id_raw if change_id_raw else f"{os.path.basename(file_ref).split('.')[0]}_{idx}"
                all_parsed_changes.append(ChangeLog(change_id, type_, description, full_sql_path,
                                                    parsed_dependencies, # Pass the parsed list of dependencies
                                                    changelog_file=current_changelog_rel_path, index=idx))

            elif type_ == "yaml":
                if not file_ref:
                    raise ValueError(f"YAML include entry is missing 'file' in changelog: {filepath}")

                # Use ABSOLUTE path for included YAML
                full_yaml_path = os.path.join(os.path.dirname(filepath), file_ref)
                if not os.path.isfile(full_yaml_path):
                    raise FileNotFoundError(f"Included changelog file not found referenced by {filepath}: {full_yaml_path}")

                self._parse_file_recursively(full_yaml_path, all_parsed_changes, processed_files, os.path.relpath(full_yaml_path, self.project_root))

            else:
                raise ValueError(f"Unknown change type: '{type_}' in changelog: {filepath}")

    def get_all_changes(self) -> List[ChangeLog]:
        """
        Parses the master changelog and all recursively included changelogs
        to collect every defined database change without filtering.
        """
        all_changes: List[ChangeLog] = []
        processed_files: Set[str] = set()
        master_changelog_rel_path = os.path.relpath(self.master_changelog_path, self.project_root)

        self._parse_file_recursively(self.master_changelog_path, all_changes, processed_files, master_changelog_rel_path)
        return all_changes

    def _get_applied_changes_from_state_manager(self) -> Set[Tuple[str, str]]:
        """
        Queries the state manager to get a set of (changelog_path, change_id) for all
        successfully applied changes.
        """
        if not self.state_manager:
            return set()
        try:
            # We query for change_id and changelog_path, which are the identifiers for dependencies
            rows = self.state_manager.client.execute(
                f"SELECT changelog_path, change_id FROM {self.state_manager.table_name} WHERE status = 'success'"
            )
            return set((row[0], row[1]) for row in rows)
        except Exception as e:
            print(f"Warning: Could not query state manager for applied changes: {e}. Assuming no changes have been applied.")
            return set()

    def get_unapplied_changes(self) -> List[ChangeLog]:
        """
        Parses all changes, filters out applied ones, and returns a topologically
        sorted list of pending changes, respecting dependencies.

        Dependencies not found in the currently parsed changelog set or already
        applied are considered met.

        Returns:
            List[ChangeLog]: A list of ChangeLog objects that are considered unapplied,
                             ordered such that dependencies are met first.
        """
        all_defined_changes = self.get_all_changes()
        applied_changes_from_db = self._get_applied_changes_from_state_manager()

        # Map for quick lookup: (changelog_path, change_id) -> ChangeLog object
        change_lookup: Dict[Tuple[str, str], ChangeLog] = {
            (c.changelog_file, c.id): c for c in all_defined_changes
        }

        # Identify pending changes (those defined in YAML but not yet applied in DB)
        pending_changes: List[ChangeLog] = []
        for change in all_defined_changes:
            if (change.changelog_file, change.id) not in applied_changes_from_db:
                pending_changes.append(change)

        if not pending_changes:
            return [] # No pending changes found

        # --- Build Dependency Graph for Topological Sort ---
        # Graph stores: (dependency_node) -> [list of nodes that depend on it]
        graph: Dict[Tuple[str, str], List[Tuple[str, str]]] = defaultdict(list)
        # In-degrees store: (node) -> count of its unmet dependencies
        in_degrees: Dict[Tuple[str, str], int] = defaultdict(int)

        # Initialize all pending changes in the graph with 0 in-degree
        # This is important for changes that have no dependencies or whose dependencies are already met
        for change in pending_changes:
            node_id = (change.changelog_file, change.id)
            in_degrees[node_id] = 0 # Initialize, will be updated by active dependencies

        # Populate graph and in-degrees for pending changes
        for change in pending_changes:
            node_id = (change.changelog_file, change.id)
            for dependency in change.depends_on:
                dep_node_id = (dependency.changelog_path, dependency.change_id)

                # Rule: "if the upstream is not in the change list then consider it has been applied"
                # This means we only add a dependency edge if:
                # 1. The dependency actually exists in `all_defined_changes` (i.e., it's a known change) AND
                # 2. The dependency is NOT already applied (i.e., it's also a pending change that *must* be run)
                if dep_node_id in change_lookup and dep_node_id not in applied_changes_from_db:
                    # This is an active, pending dependency that needs to be run before `change`
                    graph[dep_node_id].append(node_id) # dep_node_id -> node_id (dep_node_id is a prerequisite for node_id)
                    in_degrees[node_id] += 1
                # Else (dependency not found in YAML, or already applied in DB), it's considered met,
                # so no edge is added to the *pending changes* graph.

        # --- Topological Sort (Kahn's Algorithm) ---
        # Start with all nodes that have no unmet dependencies among the pending changes
        queue: List[Tuple[str, str]] = [node_id for node_id, degree in in_degrees.items() if degree == 0]
        sorted_changes_nodes: List[Tuple[str, str]] = []

        while queue:
            current_node = queue.pop(0) # Dequeue a node
            sorted_changes_nodes.append(current_node)

            # For each neighbor (node that depends on current_node)
            for neighbor_node in graph[current_node]:
                in_degrees[neighbor_node] -= 1
                if in_degrees[neighbor_node] == 0:
                    queue.append(neighbor_node)

        # Check for cycles: If not all pending changes were added to the sorted list, there's a cycle.
        # We should only consider nodes that were initially part of `pending_changes` for this check.
        if len(sorted_changes_nodes) != len(pending_changes):
            # Identify which pending changes were not sorted (due to a cycle)
            # This logic needs to be robust to only report nodes that were truly part of the cycle
            # and were originally in `pending_changes`.
            cyclic_nodes_in_pending = set(node_id for node_id in in_degrees if in_degrees[node_id] > 0)
            
            if cyclic_nodes_in_pending:
                # To make the error message more user-friendly, list the actual ChangeLog IDs
                cyclic_change_ids = [f"'{c.id}' from '{c.changelog_file}'" for c_node_id in cyclic_nodes_in_pending for c in pending_changes if (c.changelog_file, c.id) == c_node_id]
                raise ValueError(f"Circular dependency detected among pending changes involving: {', '.join(cyclic_change_ids)}")
            else:
                # This case should ideally not happen if in_degrees is correctly initialized and updated.
                # It might indicate some pending changes were not even part of the graph because their
                # dependencies were all considered met, but they themselves weren't roots.
                # For now, we'll raise a generic error, but it might need further debugging if hit.
                raise ValueError("Graph processing error: Not all pending changes could be sorted, but no cycle detected (unexpected state).")


        # Convert sorted node IDs back to ChangeLog objects
        final_sorted_changes: List[ChangeLog] = []
        for node_id in sorted_changes_nodes:
            # Ensure we only add actual ChangeLog objects that were part of our initial pending set
            if node_id in change_lookup and (node_id[0], node_id[1]) not in applied_changes_from_db:
                final_sorted_changes.append(change_lookup[node_id])

        return final_sorted_changes