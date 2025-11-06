import click
import os
import logging # Added logging import
from .config import load_config, load_variables
from .db import ClickHouseExecutor
from .changelog_parser import ChangelogParser
from .util.templating import render_sql
from .changelog import ChangeLog
from .changelog_state_manager import ChangelogStateManager

# Configure basic logging
logging.basicConfig(
    level=logging.INFO, # Default logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__) # Get a logger instance for this module

# Define PROJECT_ROOT and MACROS_ABS_PATH once, relative to this cli.py file.
# Assuming cli.py is in src/liquibase_clickhouse/,
# PROJECT_ROOT should be the directory two levels up (the project root).
# This path will remain constant even if os.chdir() is called.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
MACROS_ABS_PATH = os.path.join(PROJECT_ROOT, 'src', 'liquibase_clickhouse', 'macros')


@click.group()
def main():
    """
    Main CLI for liquibase-clickhouse operations.

    This tool helps manage database schema changes for ClickHouse using YAML
    changelog files and SQL templates. It supports applying changes,
    performing dry runs, and initializing the changelog state table.
    """
    pass


@main.command()
@click.option("--env", default="dev", help="Environment name (e.g., dev, uat, prd).")
@click.option("--change-log-file", default="master-changelogs.yaml", help="Path to the master changelog YAML file.")
@click.option("--db-host", default=None, help="Overrides the database host from config.")
@click.option("--db-port", type=int, default=None, help="Overrides the database port from config.")
@click.option("--db-name", default=None, help="Overrides the database name from config.")
@click.option("--db-user", default=None, help="Overrides the database user from config.")
@click.option("--db-password", default=None, help="Overrides the database password from config.")
def update(env, change_log_file, db_host, db_port, db_name, db_user, db_password):
    """
    Applies pending database changes to the ClickHouse database.

    This command reads the master changelog file, identifies unapplied changes
    based on the changelog state table, and executes them in topological order.
    Database connection parameters can be optionally overridden via command-line
    options, taking precedence over values in the configuration file.

    Parameters:
        env (str): The environment name (e.g., dev, uat, prd) to load variables for.
        change_log_file (str): Path to the master changelog YAML file.
        db_host (str, optional): Overrides the database host.
        db_port (int, optional): Overrides the database port.
        db_name (str, optional): Overrides the database name.
        db_user (str, optional): Overrides the database user.
        db_password (str, optional): Overrides the database password.
    """
    original_cwd = os.getcwd()
    try:
        changelog_file_abs_path = os.path.abspath(change_log_file)
        if not os.path.isfile(changelog_file_abs_path):
            logger.error(f"Master changelog file not found: {changelog_file_abs_path}")
            raise FileNotFoundError(f"Master changelog file not found: {changelog_file_abs_path}")

        changelog_dir = os.path.dirname(changelog_file_abs_path)
        os.chdir(changelog_dir)
        logger.debug(f"Changed current directory to: {os.getcwd()}")

        logger.info(f"Processing changelogs from: {changelog_file_abs_path}")

        config = load_config()
        variables = load_variables(env)
        db_config = config['database'].copy()

        # Apply overrides from CLI options if provided
        if db_host:
            db_config['host'] = db_host
        if db_port:
            db_config['port'] = db_port
        if db_name:
            db_config['database'] = db_name
        if db_user:
            db_config['user'] = db_user
        if db_password:
            db_config['password'] = db_password

        manager = ChangelogStateManager(
            host=db_config['host'],
            port=db_config.get('port', 9000),
            user=db_config['user'],
            password=db_config.get('password', ''),
            database=db_config['database'],
            table_name='changelog_state'
        )
        manager.create_state_table()
        logger.info(f"Changelog state table '{manager.table_name}' ensured to exist.")

        executor = ClickHouseExecutor(**db_config)
        logger.info(f"Connected to ClickHouse database: {db_config['database']}@{db_config['host']}:{db_config.get('port', 9000)}")

        changelog_parser = ChangelogParser(changelog_file_abs_path, state_manager=manager)
        changes_to_apply = changelog_parser.get_unapplied_changes()

        applied_count = 0
        if not changes_to_apply:
            logger.info("No new changes to apply.")
            return

        logger.info(f"Found {len(changes_to_apply)} pending change(s) to apply, ordered by dependencies.")
        for change in changes_to_apply:
            if change.type != "sql":
                logger.info(f"Skipping non-SQL change type: {change.type} (ID: {change.id})")
                continue

            try:
                display_file_path = os.path.relpath(change.file_path, PROJECT_ROOT)
                logger.info(f"Applying change: {change.id} ({change.description}) from {display_file_path}")

                manager.log_start(change, change.changelog_file)

                sql = render_sql(change.file_path, variables, macros_dir=MACROS_ABS_PATH)
                logger.debug(f"Executing SQL from: {display_file_path}")
                # logger.debug(f"SQL:\n{sql}\n---") # Uncomment for debugging SQL content
                executor.execute_change(sql)

                manager.update_status(change.id, change.changelog_file, "success")
                applied_count += 1
                logger.info(f"Successfully applied change: {change.id}")
            except Exception as e:
                manager.update_status(change.id, change.changelog_file, "failed", error_message=str(e))
                logger.error(f"Failed to apply change: {change.id} ({change.description}) -- Error: {e}")
                logger.error("Aborting update due to failed change.")
                exit(1)

        logger.info(f"Update complete. {applied_count} change(s) applied.")

    except Exception as e:
        logger.error(f"An unexpected error occurred during update: {e}")
        exit(1)
    finally:
        os.chdir(original_cwd)


@main.command()
@click.option("--env", default="dev", help="Environment name (e.g., dev, uat, prd).")
@click.option("--change-log-file", default="master-changelogs.yaml", help="Path to the master changelog YAML file.")
@click.option("--db-host", default=None, help="Overrides the database host from config.")
@click.option("--db-port", type=int, default=None, help="Overrides the database port from config.")
@click.option("--db-name", default=None, help="Overrides the database name from config.")
@click.option("--db-user", default=None, help="Overrides the database user from config.")
@click.option("--db-password", default=None, help="Overrides the database password from config.")
def dry_run(env, change_log_file, db_host, db_port, db_name, db_user, db_password):
    """
    Shows which database changes would be applied without actually executing them.

    This command performs all the parsing and dependency resolution steps of the
    'update' command but instead of executing the SQL, it logs what *would* be
    applied. It's useful for previewing changes. Database connection parameters
    can be optionally overridden via command-line options.

    Parameters:
        env (str): The environment name (e.g., dev, uat, prd) to load variables for.
        change_log_file (str): Path to the master changelog YAML file.
        db_host (str, optional): Overrides the database host.
        db_port (int, optional): Overrides the database port.
        db_name (str, optional): Overrides the database name.
        db_user (str, optional): Overrides the database user.
        db_password (str, optional): Overrides the database password.
    """
    original_cwd = os.getcwd()
    try:
        changelog_file_abs_path = os.path.abspath(change_log_file)
        if not os.path.isfile(changelog_file_abs_path):
            logger.error(f"Master changelog file not found: {changelog_file_abs_path}")
            raise FileNotFoundError(f"Master changelog file not found: {changelog_file_abs_path}")

        changelog_dir = os.path.dirname(changelog_file_abs_path)
        os.chdir(changelog_dir)
        logger.debug(f"Changed current directory to: {os.getcwd()}")

        logger.info(f"Performing dry run for changelogs from: {changelog_file_abs_path}")

        config = load_config()
        variables = load_variables(env)

        db_config = config['database'].copy()
        # Apply overrides from CLI options if provided
        if db_host:
            db_config['host'] = db_host
        if db_port:
            db_config['port'] = db_port
        if db_name:
            db_config['database'] = db_name
        if db_user:
            db_config['user'] = db_user
        if db_password:
            db_config['password'] = db_password

        manager = ChangelogStateManager(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config.get('password', ''),
            database=db_config['database'],
            table_name='changelog_state',
            port=db_config.get('port', 9000)
        )
        manager.create_state_table()
        logger.info(f"Changelog state table '{manager.table_name}' ensured to exist for dry run context.")


        changelog_parser = ChangelogParser(changelog_file_abs_path, state_manager=manager)
        unapplied_changes = changelog_parser.get_unapplied_changes()

        executor = ClickHouseExecutor(**db_config) # Executor still needed for dry_run method

        if not unapplied_changes:
            logger.info("No pending changes. All SQL has already been applied or no changes defined.")
            return

        logger.info(f"Dry run report for {len(unapplied_changes)} unapplied change(s), ordered by dependencies:")
        for change in unapplied_changes:
            if change.type != "sql":
                logger.info(f"  Skipping non-SQL change type: {change.type} (ID: {change.id})")
                continue

            display_file_path = os.path.relpath(change.file_path, PROJECT_ROOT)
            sql = render_sql(change.file_path, variables, macros_dir=MACROS_ABS_PATH)
            executor.dry_run(sql) # This method should just print/log the SQL, not execute
            logger.info(f"  Would apply: ID='{change.id}', Description='{change.description}', File='{display_file_path}', Defined in='{change.changelog_file}'")
            if change.depends_on:
                dep_str = ", ".join([f"({d.changelog_path}, {d.change_id})" for d in change.depends_on])
                logger.info(f"    Depends on: {dep_str}")

    except Exception as e:
        logger.error(f"An unexpected error occurred during dry run: {e}")
        exit(1)
    finally:
        os.chdir(original_cwd)


@main.command()
@click.option("--env", default="dev", help="Environment name (dev, uat, prd)")
@click.option("--change-log-file", default="master-changelogs.yaml", help="Path to master changelog YAML")
@click.option("--db-host", default=None, help="Overrides the database host from config.")
@click.option("--db-port", type=int, default=None, help="Overrides the database port from config.")
@click.option("--db-name", default=None, help="Overrides the database name from config.")
@click.option("--db-user", default=None, help="Overrides the database user from config.")
@click.option("--db-password", default=None, help="Overrides the database password from config.")
def init(env, change_log_file, db_host, db_port, db_name, db_user, db_password):
    """
    Initializes the changelog state table in the database.

    This command ensures that the `changelog_state` table exists in the target
    ClickHouse database. This table is crucial for tracking applied changes.
    Database connection parameters can be optionally overridden via command-line
    options.

    Parameters:
        env (str): The environment name (e.g., dev, uat, prd) for configuration.
        change_log_file (str): Path to the master changelog YAML file.
                                (Note: This file is not parsed by `init`, but required
                                for consistent CLI signature and path resolution.)
        db_host (str, optional): Overrides the database host.
        db_port (int, optional): Overrides the database port.
        db_name (str, optional): Overrides the database name.
        db_user (str, optional): Overrides the database user.
        db_password (str, optional): Overrides the database password.
    """
    original_cwd = os.getcwd()
    try:
        changelog_file_abs_path = os.path.abspath(change_log_file)
        if not os.path.isfile(changelog_file_abs_path):
            logger.warning(f"Warning: Master changelog file not found at '{changelog_file_abs_path}'. "
                           "Proceeding with state table initialization. Ensure your config is correct.")

        changelog_dir = os.path.dirname(changelog_file_abs_path)
        os.chdir(changelog_dir)
        logger.debug(f"Changed current directory to: {os.getcwd()}")

        config = load_config()
        db_config = config['database'].copy()

        # Apply overrides from CLI options if provided
        if db_host:
            db_config['host'] = db_host
        if db_port:
            db_config['port'] = db_port
        if db_name:
            db_config['database'] = db_name
        if db_user:
            db_config['user'] = db_user
        if db_password:
            db_config['password'] = db_password

        manager = ChangelogStateManager(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config.get('password', ''),
            database=db_config['database'],
            port=db_config.get('port', 9000)
        )

        manager.create_state_table()
        logger.info(f"State table '{manager.table_name}' created or already exists in database '{db_config['database']}' on host '{db_config['host']}:{db_config.get('port', 9000)}'.")

    except Exception as e:
        logger.error(f"An unexpected error occurred during initialization: {e}")
        exit(1)
    finally:
        os.chdir(original_cwd)


@main.command()
def help():
    """
    Displays the help message for the main CLI and its subcommands.
    """
    click.echo(main.get_help(click.Context(main)))


if __name__ == "__main__":
    main()