# liquibase-clickhouse Setup Guide

This guide walks you through installing, configuring, and running the `liquibase-clickhouse` migration tool for ClickHouse databases.

---

## 1. **Prerequisites**

- Python 3.8+
- ClickHouse server accessible from your environment
- [pip](https://pip.pypa.io/en/stable/) package manager

---

## 2. **Clone the Repository**

```bash
git clone <your-repo-url>
cd liquibase-clickhouse
```

---

## 3. **Install the Package**

You can install directly from source:
```bash
pip install .
```

Or, build and install a wheel file:
```bash
python setup.py bdist_wheel
pip install dist/liquibase_clickhouse-<version>-py3-none-any.whl
```

---

## 4. **Prepare Project Configuration**

Ensure the following files/folders are present at your project root:

- `config.yaml` — main config for DB connection
- `master-changelogs.yaml` — entry point for changelogs
- `changelogs/` — subfolders for each logical database (e.g. `ods/`, `edw/`, `mart/`), each with their own `changelogs.yaml` and `.sql` files
- `macros/` — Jinja2 macro files, e.g. `macros.sql.jinja`
- `variables/` — environment variables (`common.yaml`, `dev.yaml`, `uat.yaml`, `prd.yaml`)

Example structure:
```
liquibase-clickhouse/
├── config.yaml
├── master-changelogs.yaml
├── changelogs/
│   ├── ods/
│   ├── edw/
│   └── mart/
├── macros/
├── variables/
```

---

## 5. **Configure Your Database Connection**

Fill out your `config.yaml` with the necessary details. Example:

```yaml
database:
  host: localhost
  port: 9000
  user: default
  password: ""
  database: default
```

---

## 6. **Initialize State Table**

Before running migrations, initialize the state tracking table in ClickHouse:

```bash
liquibase-clickhouse init
```

---

## 7. **Preview Pending Migrations**

To preview the SQL queries that will be run (dry run):

```bash
liquibase-clickhouse dry-run --env dev --change-log-file master-changelogs.yaml
```

---

## 8. **Apply Migrations**

To apply all pending changes to the database:

```bash
liquibase-clickhouse update --env dev --change-log-file master-changelogs.yaml
```

Replace `dev` with `uat` or `prd` for other environments, as needed.

---

## 9. **Show Help**

To see all available commands:

```bash
liquibase-clickhouse help
```

---

## 10. **Run From Source (Alternative)**

If you prefer not to install the CLI, you can run directly:

```bash
python src/liquibase_clickhouse/cli.py update --env dev --change-log-file master-changelogs.yaml
```

---

## 11. **Troubleshooting**

- If the CLI is not found, ensure your Python environment's `bin` directory is in your `PATH`.
- If you modify source code, reinstall with `pip install .` to update the CLI.
- Check your config and changelog YAML files for syntax errors.

---

## 12. **Formatting and Linting**

To check and format your code with Ruff:

```bash
ruff check src/
ruff format src/
```

---

## 13. **Further Documentation**

- See sample changelog files and variables in the README or `changelogs/` and `variables/` folders.
- For advanced usage, customize macros, add dependencies, and adjust environment variables as needed.

---

**You are now ready to use liquibase-clickhouse for managing your ClickHouse schema migrations!**

---