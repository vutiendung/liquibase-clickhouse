# src/liquibase_clickhouse/config.py

import yaml
import os

def load_yaml(filepath):
    with open(filepath, 'r') as f:
        return yaml.safe_load(f)

def load_config():
    return load_yaml(os.path.join(os.getcwd(), "config.yaml"))

def load_variables(env):
    common = load_yaml("variables/common.yaml")
    env_vars = load_yaml(f"variables/{env}.yaml")
    result = common.copy()
    result.update(env_vars)
    return result