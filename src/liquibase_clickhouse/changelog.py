import os
import yaml
from jinja2 import Environment, FileSystemLoader

class Change:
    def __init__(self, change_id, type_, description, file_, depends_on=None, changelog_file=None):
        self.id = change_id
        self.type = type_
        self.description = description
        self.file = file_
        self.depends_on = depends_on
        self.changelog_file = changelog_file

    def __repr__(self):
        return (
            f"Change(id={self.id!r}, type={self.type!r}, description={self.description!r}, "
            f"file={self.file!r}, depends_on={self.depends_on!r}, changelog_file={self.changelog_file!r})"
        )

class ChangelogFile:
    def __init__(self, master_changelog_path, state_manager=None):
        self.master_changelog_path = os.path.abspath(master_changelog_path)
        self.changes = []
        self.state_manager = state_manager
        self.project_root = os.path.dirname(os.path.abspath(master_changelog_path))

    def load_yaml(self, filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Changelog file not found: {filepath}")

    def _process(self, filepath, processed_files=None, current_changelog_path=None):
        if processed_files is None:
            processed_files = set()
        if current_changelog_path is None:
            current_changelog_path = filepath  # On first call, this is master

        rel_path = os.path.relpath(filepath, self.project_root)
        rel_changelog_path = os.path.relpath(current_changelog_path, self.project_root)
        if rel_path in processed_files:
            return  # Prevent circular includes
        processed_files.add(rel_path)

        data = self.load_yaml(filepath)
        changes_list = data.get("changes", [])
        file_changes = []

        for idx, entry in enumerate(changes_list):
            type_ = entry.get("type")
            description = entry.get("description", "")
            file_ = entry.get("file")
            depends_on = entry.get("depends_on", None)
            change_id = entry.get("id", f"{os.path.basename(file_)}_{idx}")
            full_path = os.path.join(os.path.dirname(filepath), file_)
            rel_file_path = os.path.relpath(full_path, self.project_root)

            if type_ == "sql":
                if not os.path.isfile(full_path):
                    raise FileNotFoundError(f"SQL file not found: {full_path}")
                # The change is from THIS changelog, not master
                # You may want to store rel_changelog_path with the Change object as well!
                file_changes.append(Change(change_id, type_, description, rel_file_path, depends_on, changelog_file=rel_changelog_path))
            elif type_ == "yaml":
                if not os.path.isfile(full_path):
                    raise FileNotFoundError(f"Changelog file not found: {full_path}")
                # Recurse, setting current_changelog_path to the sub-changelog YAML
                self._process(full_path, processed_files=processed_files, current_changelog_path=full_path)
            else:
                raise ValueError(f"Unknown change type: {type_} in {filepath}")

        if self.state_manager and file_changes:
            already_run_ids = set(
                row[0] for row in self.state_manager.client.execute(
                    f"SELECT id FROM {self.state_manager.table_name} WHERE changelog_path = %(changelog_path)s AND status = 'success'",
                    {"changelog_path": rel_changelog_path}
                )
            )
            file_changes = [change for change in file_changes if change.id not in already_run_ids]

        self.changes.extend(file_changes)

    def get_unapplied_changes(self):
        self.changes = []
        self._process(self.master_changelog_path)
        return self.changes

def render_sql(sql_file, variables=None, macros_dir=None):
    # Setup variables and Jinja2 environment
    variables = variables or {}
    # Use the macros_dir if provided, else use the SQL file's directory
    search_dirs = []
    if macros_dir:
        search_dirs.append(macros_dir)
    search_dirs.append(os.path.dirname(sql_file))
    env = Environment(loader=FileSystemLoader(search_dirs))
    # Load and render template
    template_name = os.path.basename(sql_file)
    template = env.get_template(template_name)
    return template.render(**variables)

# Usage example:
if __name__ == "__main__":
    from changelog_state_manager import ChangelogStateManager
    # Setup state manager (replace params with your config)
    manager = ChangelogStateManager(host="localhost", user="liquibase", password="admin@123", database="liquibase")
    changelog = ChangelogFile("master-changelogs.yaml", state_manager=manager)
    changes = changelog.get_unapplied_changes()
    for change in changes:
        print(change)