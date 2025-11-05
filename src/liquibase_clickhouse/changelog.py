import os
import yaml
from jinja2 import Environment, FileSystemLoader

class ChangeLog:
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