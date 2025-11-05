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

    # Load macros globally
    #if macros_dir and os.path.isdir(macros_dir):
    #    for root, _, files in os.walk(macros_dir):
    #        for filename in files:
    #            if filename.endswith(('.j2', '.jinja', '.jinja2')):
    #                macro_filepath = os.path.join(root, filename)
    #                macro_template_name = os.path.relpath(macro_filepath, macros_dir)
#
    #                try:
    #                    macro_template = env.get_template(macro_template_name)
    #                    # --- ADDED ROBUSTNESS CHECK ---
    #                    if hasattr(macro_template.module, 'macros'):
    #                        for macro_name, macro_func in macro_template.module.macros.items():
    #                            if macro_name not in env.globals:
    #                                env.globals[macro_name] = macro_func
    #                            # else:
    #                            #     print(f"Debug: Macro '{macro_name}' from '{macro_template_name}' was already defined globally.")
    #                        # print(f"Debug: Successfully loaded macros from '{macro_template_name}': {list(macro_template.module.macros.keys())}")
    #                    else:
    #                        # This means the file was parsed, but no macros were found within it.
    #                        print(f"Warning: Macro file '{macro_template_name}' processed, but no macros were found within it (or 'module.macros' attribute is missing). Ensure it contains '{{% macro ... %}}' definitions.")
#
    #                except Exception as e:
    #                    print(f"Warning: Could not load macros from '{macro_filepath}': {e}")

    template_name = os.path.basename(sql_file)
    try:
        template = env.get_template(template_name)
    except Exception as e:
        raise FileNotFoundError(f"Could not find or load SQL template '{sql_file}' (looked for '{template_name}' in {search_dirs}): {e}")

    return template.render(**variables)
