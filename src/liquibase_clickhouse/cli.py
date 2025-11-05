# src/liquibase_clickhouse/cli.py

import click
import os
from .config import load_config, load_variables
from .db import ClickHouseExecutor
from .changelog_parser import ChangelogParser
from .util.templating import render_sql
from .changelog import ChangeLog
from .changelog_state_manager import ChangelogStateManager


@click.group()
def main():
    """
    Main CLI for liquibase-clickhouse operations.
    """
    pass


@main.command()
@click.option("--env", default="dev", help="Environment name (e.g., dev, uat, prd).")
@click.option("--change-log-file", default="master-changelogs.yaml", help="Path to the master changelog YAML file.")
def update(env, change_log_file):
    """
    Applies pending database changes to the ClickHouse database.
    """
    # Change directory to where the master changelog file is located
    # This ensures relative paths within changelogs are resolved correctly.
    original_cwd = os.getcwd()
    try:
        changelog_file_abs_path = os.path.abspath(change_log_file)
        changelog_dir = os.path.dirname(changelog_file_abs_path)
        os.chdir(changelog_dir)

        click.echo(f"Processing changelogs from: {changelog_file_abs_path}")

        config = load_config()
        variables = load_variables(env)
        db_config = config['database'].copy()

        # Setup state manager and ensure the state table exists
        manager = ChangelogStateManager(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config.get('password', ''),
            database=db_config['database'],
            table_name='changelog_state'
        )
        manager.create_state_table()
        click.echo(f"Changelog state table '{manager.table_name}' ensured to exist.")

        executor = ClickHouseExecutor(**db_config)
        click.echo(f"Connected to ClickHouse database: {db_config['database']}@{db_config['host']}")

        changelog_parser = ChangelogParser(changelog_file_abs_path, state_manager=manager)
        changes_to_apply = changelog_parser.get_unapplied_changes()

        applied_count = 0
        if not changes_to_apply:
            click.echo("No new changes to apply.")
            return

        click.echo(f"Found {len(changes_to_apply)} pending change(s) to apply.")
        for change in changes_to_apply:
            if change.type != "sql":
                click.echo(f"Skipping non-SQL change type: {change.type} (ID: {change.id})")
                continue # Only process SQL changes in the update command

            try:
                click.echo(f"Applying change: {change.id} ({change.description}) from {change.changelog_file}")
                # Log start (pending) before execution
                manager.log_start(change, change.changelog_file) # Ensure change_id and changelog_file are passed

                # --- ADJUSTED ATTRIBUTE ACCESS ---
                sql = render_sql(change.file, variables, 'macros') # Use change.file_path
                click.echo(f"Executing SQL from: {change.file}")
                # click.echo(f"SQL:\n{sql}\n---") # Uncomment for debugging SQL
                executor.execute_change(sql)

                # Log success
                manager.update_status(change.id, change.changelog_file, "success")
                applied_count += 1
                click.echo(f"Successfully applied change: {change.id}")
            except Exception as e:
                # Log failure
                manager.update_status(change.id, change.changelog_file, "failed", error_message=str(e))
                click.echo(f"Failed to apply change: {change.id} ({change.description}) -- Error: {e}", err=True)
                click.echo("Aborting update due to failed change.", err=True)
                exit(1) # Exit on first failure

        click.echo(f"Update complete. {applied_count} change(s) applied.")

    finally:
        os.chdir(original_cwd) # Always change back to original directory


@main.command()
@click.option("--env", default="dev", help="Environment name (e.g., dev, uat, prd).")
@click.option("--change-log-file", default="master-changelogs.yaml", help="Path to the master changelog YAML file.")
def dry_run(env, change_log_file):
    """
    Shows which database changes would be applied without actually executing them.
    """
    original_cwd = os.getcwd()
    try:
        changelog_file_abs_path = os.path.abspath(change_log_file)
        changelog_dir = os.path.dirname(changelog_file_abs_path)
        os.chdir(changelog_dir)

        click.echo(f"Performing dry run for changelogs from: {changelog_file_abs_path}")

        config = load_config()
        variables = load_variables(env)

        db_config = config['database'].copy()
        # Setup state manager
        manager = ChangelogStateManager(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config.get('password', ''),
            database=db_config['database'],
            table_name='changelog_state'
        )
        manager.create_state_table()  # Ensure table exists

        # --- ADJUSTED INSTANTIATION ---
        # Collect only unapplied changes (using new logic)
        changelog_parser = ChangelogParser(changelog_file_abs_path, state_manager=manager)
        unapplied_changes = changelog_parser.get_unapplied_changes()

        executor = ClickHouseExecutor(**db_config)

        if not unapplied_changes:
            click.echo("No pending changes. All SQL has already been applied or no changes defined.")
            return

        click.echo(f"Dry run report for {len(unapplied_changes)} unapplied change(s):")
        for change in unapplied_changes:
            # Only run SQL changes
            if change.type != "sql":
                click.echo(f"  Skipping non-SQL change type: {change.type} (ID: {change.id})")
                continue

            # --- ADJUSTED ATTRIBUTE ACCESS ---
            sql = render_sql(change.file, variables, 'macros')
            executor.dry_run(sql) # The dry_run method in ClickHouseExecutor should just print or log
            click.echo(f"  Would apply: ID='{change.id}', Description='{change.description}', File='{change.file}', Defined in='{change.changelog_file}'")
            # click.echo(f"    SQL Content (first 100 chars):\n    {sql[:100]}...\n") # Uncomment for debugging SQL content

    finally:
        os.chdir(original_cwd)


@main.command()
@click.option("--env", default="dev", help="Environment name (dev, uat, prd)")
@click.option("--change-log-file", default="master-changelogs.yaml", help="Path to master changelog YAML")
def init(env, change_log_file):
    """
    Initializes the changelog state table in the database.
    """
    original_cwd = os.getcwd()
    try:
        changelog_file_abs_path = os.path.abspath(change_log_file)
        changelog_dir = os.path.dirname(changelog_file_abs_path)
        os.chdir(changelog_dir)

        config = load_config()
        db_config = config['database'].copy()

        # Initialize the manager
        manager = ChangelogStateManager(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config.get('password', ''),
            database=db_config['database'],
        )

        # Ensure state table is created
        manager.create_state_table()
        click.echo(f"State table '{manager.table_name}' created or already exists in database '{db_config['database']}'.")

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
