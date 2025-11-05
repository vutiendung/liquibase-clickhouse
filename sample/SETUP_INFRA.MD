# Clickhouse docker
```cmd
docker run -d \
  --name clickhouse-server \
  -p 8123:8123 \
  -p 9000:9000 \
  -e CLICKHOUSE_USER=liquibase \
  -e CLICKHOUSE_PASSWORD=admin@123 \
  -e CLICKHOUSE_DB=ods \
  -v clickhouse_data:/var/lib/clickhouse \
  -v clickhouse_logs:/var/log/clickhouse-server \
  clickhouse/clickhouse-server
```

```sql
CREATE USER liquibase_user IDENTIFIED BY 'admin@123';
GRANT ALL ON *.* TO admin;
```

# Setup
1. Download driver here [https://github.com/ClickHouse/clickhouse-java/releases/download/v0.9.3/clickhouse-jdbc-0.9.3-all-dependencies.jar]   
and save to `lib` folder of `liquibase`