# Analysis Visualizer

The **Analysis Visualizer** helps you explore and analyze database performance data collected with **WorkloadTools** using preconfigured **Grafana** dashboards.

## Features

- Automatically provisions a Grafana instance with the required data sources.
- Includes ready-to-use dashboards.
- Supports SQLite, DuckDB, and SQL Server.
- Simple deployment using Docker.

---

# Installation

You can install the Analysis Visualizer in one of two ways:

1. **Use the preconfigured Docker image** (recommended).
2. **Import the dashboards into an existing Grafana installation**.

---

## Option 1: Use the Docker Image

### Download the Image

The Docker image is available [here]().

A complete deployment guide is available [here]().

### Configure Docker Compose

Create a `compose.yaml` file and update the values according to your environment.

```yaml
services:
  grafana:
    image: workloadtools/visualizer:latest
    container_name: grafana

    ports:
      - "YOUR_PORT:3000"

    environment:
      # SQLite
      - Sqlite_Baseline_DBName=baseline.db
      - Sqlite_Benchmark_DBName=benchmark.db

      # DuckDB
      - DuckDB_DBName=benchmark.duckdb

      # SQL Server
      - MSSQL_HOST=host.docker.internal
      - MSSQL_PORT=1433
      - MSSQL_USER=username
      - MSSQL_DB=database_name
      - MSSQL_PASSWORD=password

    volumes:
      - grafana-data:/var/lib/grafana

      # SQLite databases
      - type: bind
        source: /full/sqlite/directory/path
        target: /data/sqlite
        read_only: true

      # DuckDB databases
      - type: bind
        source: /full/duckdb/directory/path
        target: /data/duckdb
        read_only: false

    restart: unless-stopped

volumes:
  grafana-data: {}
  duckdb-data: {}
```

### Notes

#### SQL Server Connection

If your SQL Server instance is running on the host machine:

- **Windows:** use `host.docker.internal`
- **Linux:** use the host machine IP address

#### Database Files

Make sure the database filenames specified in the environment variables exactly match the files mounted inside the container.

### Build the Image

Run:

```bash
docker build \
  --build-arg "GRAFANA_VERSION=latest" \
  --build-arg "GF_INSTALL_PLUGINS=https://github.com/motherduckdb/grafana-duckdb-datasource/releases/download/v0.4.3/motherduck-duckdb-datasource-0.4.3.zip;motherduck-duckdb-datasource,frser-sqlite-datasource" \
  -t workloadtools/visualizer:latest .
```

### Start the Container

```bash
docker compose up
```

If Docker reports that it cannot find the compose file, make sure you are running the command from the directory containing `compose.yaml`.

---

# Option 2: Use an Existing Grafana Installation

## Download the Dashboards

1. Download the dashboards from [here]().
2. Create the required data source(s) in Grafana.
3. Import the dashboards.

---

## Create the Data Sources

### SQL Server

No additional plugins are required because SQL Server support is included with Grafana.

### SQLite

Install the SQLite data source plugin:

```bash
grafana-cli plugins install frser-sqlite-datasource
```

Restart Grafana after installation.

To verify that the plugin was installed successfully:

1. Open Grafana.
2. Navigate to **Administration → Plugins**.
3. Confirm that **SQLite Datasource** appears in the list.

For additional documentation, see:

- https://grafana.com/grafana/plugins/frser-sqlite-datasource/

#### SQLite Configuration

The dashboards expect two SQLite data sources:

| Data Source | Database |
|-------------|------------|
| Baseline | Baseline database |
| Benchmark | Benchmark database |

---

### DuckDB

Install the DuckDB plugin by following the instructions provided by the project:

- https://github.com/motherduckdb/grafana-duckdb-datasource

After installation:

1. Create a DuckDB data source.
2. Configure it to point to your database file.
3. Import the dashboards.

---

# Troubleshooting

### Grafana Cannot See the Database

Verify that:

- The database file exists.
- The filename matches the configured environment variable.
- The database directory is correctly mounted inside the container.

### SQL Server Connection Fails

Verify that:

- The hostname and port are correct.
- SQL Server accepts remote connections.
- The specified user has permission to access the database.

### Plugin Not Found

Restart Grafana after installing plugins and verify the installation under **Administration → Plugins**.