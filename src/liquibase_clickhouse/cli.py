# src/liquibase_clickhouse/cli.py

import click
import os
from .config import load_config, load_variables
from .db import ClickHouseExecutor
from .changelog import ChangelogFile, render_sql
from .changelog_state_manager import ChangelogStateManager

@click.group()
def main():
    pass

@main.command()
@click.option("--env", default="dev")
@click.option("--change-log-file", default="master-changelogs.yaml")
def update(env, change_log_file):
    os.chdir(os.path.dirname(os.path.abspath(change_log_file)))
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

    executor = ClickHouseExecutor(**db_config)

    # Get only unapplied changes (recursively, with state check)
    changelog = ChangelogFile(change_log_file, state_manager=manager)
    changes = changelog.get_unapplied_changes()

    applied_count = 0
    for change in changes:
        if change.type != "sql":
            continue
        try:
            # Log start (pending) before execution
            manager.log_start(change, change.changelog_file)
            
            sql = render_sql(change.file, variables, 'macros')
            click.echo(f"Executing: {sql}")
            executor.execute_change(sql)
            
            # Log success
            manager.update_status(change.id, change.changelog_file, "success")
            applied_count += 1
            click.echo(f"Applied change: {change.id} ({change.description})")
        except Exception as e:
            # Log failure
            manager.update_status(change.id, change.changelog_file, "failed", error_message=str(e))
            click.echo(f"Failed change: {change.id} ({change.description}) -- {e}")
            exit(1)

    if applied_count == 0:
        click.echo("No new changes to apply.")
    else:
        click.echo(f"Update complete. {applied_count} change(s) applied.")

@main.command()
@click.option("--env", default="dev")
@click.option("--change-log-file", default="master-changelogs.yaml")
def dry_run(env, change_log_file):
    os.chdir(os.path.dirname(os.path.abspath(change_log_file)))
    
    config = load_config()
    variables = load_variables(env)
    
    # Setup state manager
    db_config = config['database'].copy()
    manager = ChangelogStateManager(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config.get('password', ''),
        database=db_config['database'],
        table_name='changelog_state'
    )
    manager.create_state_table()  # Ensure table exists

    # Collect only unapplied changes (using new logic)
    changelog = ChangelogFile(change_log_file, state_manager=manager)
    unapplied_changes = changelog.get_unapplied_changes()

    executor = ClickHouseExecutor(**db_config)

    if not unapplied_changes:
        click.echo("No pending changes. All SQL has already been applied.")
        return

    click.echo(f"Dry run for unapplied SQL changes in {change_log_file}:")
    for change in unapplied_changes:
        # Only run SQL changes
        if change.type != "sql":
            continue
        sql = render_sql(change.file, variables, 'macros')
        executor.dry_run(sql)
        click.echo(f"Would run: {change.id} ({change.file}) -- {change.description}")

@main.command()
@click.option("--env", default="dev", help="Environment name (dev, uat, prd)")
@click.option("--change-log-file", default="master-changelogs.yaml", help="Path to master changelog YAML")
def init(env, change_log_file):  # <-- Add change_log_file param
    os.chdir(os.path.dirname(os.path.abspath(change_log_file)))
    
    config = load_config()
    # variables = load_variables(env)
    db_config = config['database'].copy()

    # Initialize the executor and manager
    manager = ChangelogStateManager(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config.get('password', ''),
        database=db_config['database'],
    )

    # Ensure state table is created
    manager.create_state_table()
    click.echo(f"State table '{manager.table_name}' created or already exists in schema '{db_config['database']}'.")

@main.command()
def help():
    click.echo(main.get_help(click.Context(main)))

if __name__ == "__main__":
    main()