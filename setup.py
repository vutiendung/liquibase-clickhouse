from setuptools import setup, find_packages

setup(
    name="liquibase-clickhouse",
    version="0.1.0",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "click",
        "pyyaml",
        "jinja2",
        "clickhouse-driver",
    ],
    entry_points={
        "console_scripts": [
            "liquibase-clickhouse = liquibase_clickhouse.cli:main"
        ]
    },
)