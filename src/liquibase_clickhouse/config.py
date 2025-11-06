# src/liquibase_clickhouse/config.py

import yaml
import os
import logging # Added logging import

# Get a logger instance for this module.
# Basic configuration is typically done once at the application's entry point (e.g., cli.py).
logger = logging.getLogger(__name__)

def load_yaml(filepath: str) -> dict:
    """
    Loads and parses a YAML file safely.

    Args:
        filepath (str): The absolute or relative path to the YAML file to load.

    Returns:
        dict: The parsed content of the YAML file as a dictionary.

    Raises:
        FileNotFoundError: If the specified YAML file does not exist.
        yaml.YAMLError: If there's an error parsing the YAML content.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = yaml.safe_load(f)
            logger.debug(f"Successfully loaded YAML file: {filepath}")
            return content if isinstance(content, dict) else {}
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {filepath}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file {filepath}: {e}")
        raise ValueError(f"Error parsing YAML file {filepath}: {e}")

def load_config() -> dict:
    """
    Loads the main application configuration from 'config.yaml'.

    It expects 'config.yaml' to be located in the current working directory
    when the application is run.

    Returns:
        dict: A dictionary containing the application configuration.

    Raises:
        FileNotFoundError: If 'config.yaml' is not found.
        ValueError: If there's an error parsing 'config.yaml'.
    """
    config_path = os.path.join(os.getcwd(), "config.yaml")
    logger.info(f"Loading main configuration from: {config_path}")
    return load_yaml(config_path)

def load_variables(env: str) -> dict:
    """
    Loads environment-specific variables by merging common variables with
    variables specific to the given environment.

    It expects 'variables/common.yaml' and 'variables/{env}.yaml' to exist
    relative to the current working directory.

    Args:
        env (str): The name of the environment (e.g., 'dev', 'uat', 'prd').

    Returns:
        dict: A dictionary containing the merged variables for the specified environment.

    Raises:
        FileNotFoundError: If 'common.yaml' or the environment-specific
                           '{env}.yaml' file is not found.
        ValueError: If there's an error parsing any of the YAML variable files.
    """
    common_vars_path = os.path.join(os.getcwd(), "variables/common.yaml")
    env_vars_path = os.path.join(os.getcwd(), f"variables/{env}.yaml")

    logger.info(f"Loading common variables from: {common_vars_path}")
    common = load_yaml(common_vars_path)

    logger.info(f"Loading environment-specific variables for '{env}' from: {env_vars_path}")
    env_vars = load_yaml(env_vars_path)

    result = common.copy()
    result.update(env_vars)
    logger.info(f"Successfully merged variables for environment '{env}'.")
    return result