I want to write a tool similar as liquibase but just for clickhouse using python.
It only needs to handle execute the query
name of tool is liquibase-clickhouse

requirement:
use clean OOP and interface if possible to easier to extends
can build to wheel file to install standalone and run as command after install like flask
code should under src folder, has the setup file to setup
use ruff to format the code and check

folder structure:
in the main folder it should have the foolowing file, folder:
- config.yaml: config for project
- master-changelogs.yaml: entry point which will include other config in sub folder that we will run
- in each sub folder it will have other changelogs.yaml it will define other change log or include .sql file to run
the file path should be relative
- macros: the macro allow us define jinja2 macro to use in the .sql so we can re-use the code
- variables: contains variable to for each env: it should hav common.yaml variable that will be appear in all evn, and also env<dev, uat, prd>.yaml that will contains overwrite or specify var for this env

functions:
cli
it has the following command using click
- update: allow find the change which is not run yet and sequency apply to database
- dry-run: preview the query to be run
- init: create database, table in target database to store the log, file change has been run before (act like state) similar to liquibase
- help: show help

the sub parameters should be --env, --change-log-file