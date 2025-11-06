# src/liquibase_clickhouse/cli.py

import click
import os
from .config import load_config, load_variables
from .db import ClickHouseExecutor
from .changelog_parser import ChangelogParser
from .util.templating import render_sql # Corrected import path (assuming utils/sql_renderer.py)
from .changelog import ChangeLog # Still need this for type hinting
from .changelog_state_manager import ChangelogStateManager

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
    """
    pass


@main.command()
@click.option("--env", default="dev", help="Environment name (e.g., dev, uat, prd).")
@click.option("--change-log-file", default="master-changelogs.yaml", help="Path to the master changelog YAML file.")
# New optional database connection parameters
@click.option("--db-host", default=None, help="Overrides the database host from config.")
@click.option("--db-port", type=int, default=None, help="Overrides the database port from config.")
@click.option("--db-name", default=None, help="Overrides the database name from config.")
@click.option("--db-user", default=None, help="Overrides the database user from config.")
@click.option("--db-password", default=None, help="Overrides the database password from config.")
def update(env, change_log_file, db_host, db_port, db_name, db_user, db_password):
    """
    Applies pending database changes to the ClickHouse database.
    """
    original_cwd = os.getcwd()
    try:
        # Resolve master changelog path absolutely
        changelog_file_abs_path = os.path.abspath(change_log_file)
        if not os.path.isfile(changelog_file_abs_path):
            raise FileNotFoundError(f"Master changelog file not found: {changelog_file_abs_path}")

        # Change directory to where the master changelog file is located
        # This ensures relative paths *within changelogs* are resolved correctly
        # by the YAML parser itself (if it were to use relative paths directly).
        # However, our ChangelogParser and render_sql use absolute paths internally.
        changelog_dir = os.path.dirname(changelog_file_abs_path)
        os.chdir(changelog_dir)
        click.echo(f"Changed current directory to: {os.getcwd()}") # Debugging current working directory

        click.echo(f"Processing changelogs from: {changelog_file_abs_path}")

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
        click.echo(f"Changelog state table '{manager.table_name}' ensured to exist.")

        executor = ClickHouseExecutor(**db_config)
        click.echo(f"Connected to ClickHouse database: {db_config['database']}@{db_config['host']}:{db_config.get('port', 9000)}")

        # Pass the absolute path to ChangelogParser
        changelog_parser = ChangelogParser(changelog_file_abs_path, state_manager=manager)
        # This returns a topologically sorted list of unapplied changes
        changes_to_apply = changelog_parser.get_unapplied_changes()

        applied_count = 0
        if not changes_to_apply:
            click.echo("No new changes to apply.")
            return

        click.echo(f"Found {len(changes_to_apply)} pending change(s) to apply, ordered by dependencies.")
        for change in changes_to_apply:
            if change.type != "sql":
                click.echo(f"Skipping non-SQL change type: {change.type} (ID: {change.id})")
                continue

            try:
                # Display path relative to project root for user readability
                display_file_path = os.path.relpath(change.file_path, PROJECT_ROOT)
                click.echo(f"Applying change: {change.id} ({change.description}) from {display_file_path}")

                manager.log_start(change, change.changelog_file)

                # --- DEPENDENCY-RELATED ADJUSTMENTS ---
                # Use change.file_path (which is absolute) and MACROS_ABS_PATH (also absolute)
                sql = render_sql(change.file_path, variables, macros_dir=MACROS_ABS_PATH)
                click.echo(f"Executing SQL from: {display_file_path}")
                # click.echo(f"SQL:\n{sql}\n---") # Uncomment for debugging SQL
                executor.execute_change(sql)

                manager.update_status(change.id, change.changelog_file, "success")
                applied_count += 1
                click.echo(f"Successfully applied change: {change.id}")
            except Exception as e:
                manager.update_status(change.id, change.changelog_file, "failed", error_message=str(e))
                click.echo(f"Failed to apply change: {change.id} ({change.description}) -- Error: {e}", err=True)
                click.echo("Aborting update due to failed change.", err=True)
                exit(1)

        click.echo(f"Update complete. {applied_count} change(s) applied.")

    except Exception as e:
        click.echo(f"An unexpected error occurred during update: {e}", err=True)
        exit(1)
    finally:
        os.chdir(original_cwd)


@main.command()
@click.option("--env", default="dev", help="Environment name (e.g., dev, uat, prd).")
@click.option("--change-log-file", default="master-changelogs.yaml", help="Path to the master changelog YAML file.")
# New optional database connection parameters
@click.option("--db-host", default=None, help="Overrides the database host from config.")
@click.option("--db-port", type=int, default=None, help="Overrides the database port from config.")
@click.option("--db-name", default=None, help="Overrides the database name from config.")
@click.option("--db-user", default=None, help="Overrides the database user from config.")
@click.option("--db-password", default=None, help="Overrides the database password from config.")
def dry_run(env, change_log_file, db_host, db_port, db_name, db_user, db_password):
    """
    Shows which database changes would be applied without actually executing them.
    """
    original_cwd = os.getcwd()
    try:
        changelog_file_abs_path = os.path.abspath(change_log_file)
        if not os.path.isfile(changelog_file_abs_path):
            raise FileNotFoundError(f"Master changelog file not found: {changelog_file_abs_path}")

        changelog_dir = os.path.dirname(changelog_file_abs_path)
        os.chdir(changelog_dir)
        click.echo(f"Changed current directory to: {os.getcwd()}") # Debugging current working directory

        click.echo(f"Performing dry run for changelogs from: {changelog_file_abs_path}")

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
            port=db_config.get('port', 9000) # Ensure port is passed
        )
        manager.create_state_table()

        changelog_parser = ChangelogParser(changelog_file_abs_path, state_manager=manager)
        unapplied_changes = changelog_parser.get_unapplied_changes() # Topologically sorted list

        executor = ClickHouseExecutor(**db_config)

        if not unapplied_changes:
            click.echo("No pending changes. All SQL has already been applied or no changes defined.")
            return

        click.echo(f"Dry run report for {len(unapplied_changes)} unapplied change(s), ordered by dependencies:")
        for change in unapplied_changes:
            if change.type != "sql":
                click.echo(f"  Skipping non-SQL change type: {change.type} (ID: {change.id})")
                continue

            # --- DEPENDENCY-RELATED ADJUSTMENTS ---
            display_file_path = os.path.relpath(change.file_path, PROJECT_ROOT)
            sql = render_sql(change.file_path, variables, macros_dir=MACROS_ABS_PATH)
            executor.dry_run(sql)
            click.echo(f"  Would apply: ID='{change.id}', Description='{change.description}', File='{display_file_path}', Defined in='{change.changelog_file}'")
            # If you want to show dependencies:
            if change.depends_on:
                dep_str = ", ".join([f"({d.changelog_path}, {d.change_id})" for d in change.depends_on])
                click.echo(f"    Depends on: {dep_str}")

    except Exception as e:
        click.echo(f"An unexpected error occurred during dry run: {e}", err=True)
        exit(1)
    finally:
        os.chdir(original_cwd)


@main.command()
@click.option("--env", default="dev", help="Environment name (dev, uat, prd)")
@click.option("--change-log-file", default="master-changelogs.yaml", help="Path to master changelog YAML")
# New optional database connection parameters
@click.option("--db-host", default=None, help="Overrides the database host from config.")
@click.option("--db-port", type=int, default=None, help="Overrides the database port from config.")
@click.option("--db-name", default=None, help="Overrides the database name from config.")
@click.option("--db-user", default=None, help="Overrides the database user from config.")
@click.option("--db-password", default=None, help="Overrides the database password from config.")
def init(env, change_log_file, db_host, db_port, db_name, db_user, db_password):
    """
    Initializes the changelog state table in the database.
    """
    original_cwd = os.getcwd()
    try:
        changelog_file_abs_path = os.path.abspath(change_log_file)
        if not os.path.isfile(changelog_file_abs_path):
            click.echo(f"Warning: Master changelog file not found at '{changelog_file_abs_path}'. "
                       "Proceeding with state table initialization. Ensure your config is correct.", err=True)

        changelog_dir = os.path.dirname(changelog_file_abs_path)
        os.chdir(changelog_dir)
        click.echo(f"Changed current directory to: {os.getcwd()}") # Debugging current working directory

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
            port=db_config.get('port', 9000) # Ensure port is passed
        )

        manager.create_state_table()
        click.echo(f"State table '{manager.table_name}' created or already exists in database '{db_config['database']}' on host '{db_config['host']}:{db_config.get('port', 9000)}'.")

    except Exception as e:
        click.echo(f"An unexpected error occurred during initialization: {e}", err=True)
        exit(1)
    finally:
        os.chdir(original_cwd)


@main.command()
def help():
    """
    Shows this help message.
    """
    click.echo(main.get_help(click.Context(main)))


if __name__ == "__main__":
    main()