import os
from jinja2 import Environment, FileSystemLoader, Template
from typing import Optional, Dict, Any

def render_sql(sql_file: str, variables: Optional[Dict[str, Any]] = None, macros_dir: Optional[str] = None) -> str:
    """
    Renders an SQL template file using Jinja2, substituting variables and globally available macros.

    Args:
        sql_file (str): The ABSOLUTE path to the SQL template file.
        variables (Optional[Dict[str, Any]]): A dictionary of variables to inject into the template.
        macros_dir (Optional[str]): The ABSOLUTE directory where Jinja2 macro files are located.
                                    All macros in .j2 files within this directory will be
                                    globally available.

    Returns:
        str: The rendered SQL content.
    """
    variables = variables or {}

    search_dirs = []

    if macros_dir and os.path.isdir(macros_dir):
        search_dirs.append(macros_dir)
    elif macros_dir:
        print(f"Warning: Macros directory '{macros_dir}' not found or is not a directory. No global macros will be loaded from it.")

    sql_file_dir = os.path.dirname(sql_file)
    if sql_file_dir and os.path.isdir(sql_file_dir) and sql_file_dir not in search_dirs:
        search_dirs.append(sql_file_dir)
    elif sql_file_dir and not os.path.isdir(sql_file_dir):
        print(f"Warning: Directory of SQL file '{sql_file_dir}' not found or is not a directory. Template might not resolve includes correctly.")

    if not search_dirs:
        raise ValueError("No valid template search directories provided or found for Jinja2.")

    env = Environment(
        loader=FileSystemLoader(search_dirs),
        trim_blocks=True,
        lstrip_blocks=True
    )

    template_name = os.path.basename(sql_file)
    try:
        template = env.get_template(template_name)
    except Exception as e:
        raise FileNotFoundError(f"Could not find or load SQL template '{sql_file}' (looked for '{template_name}' in {search_dirs}): {e}")

    return template.render(**variables)
