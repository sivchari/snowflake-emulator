# snowflake-emulator

A Snowflake emulator written in Rust for local development and testing.
Built on [DataFusion](https://github.com/apache/datafusion), compatible with Snowflake SQL API v2.

## Quick Start

### Using Docker

```bash
docker run -p 8080:8080 ghcr.io/sivchari/snowflake-emulator:latest
```

### Building from source

```bash
cargo build --release --package server
./target/release/server
```

The server starts on `localhost:8080`.

### Connecting with drivers

Use any Snowflake-compatible driver. Example with Go:

```go
import "github.com/snowflakedb/gosnowflake"

cfg := &gosnowflake.Config{
    Account:   "emulator",
    User:      "user",
    Password:  "password",
    Host:      "localhost",
    Port:      8080,
    Protocol:  "http",
}

dsn, _ := gosnowflake.DSN(cfg)
db, _ := sql.Open("snowflake", dsn)
```

## Supported SQL

### Data Types

| Type | Description |
|------|-------------|
| VARIANT | JSON-compatible dynamic type |
| ARRAY | JSON array |
| OBJECT | JSON object |
| Standard types | INT, DOUBLE, VARCHAR, BOOLEAN, DATE, TIMESTAMP |

### Snowflake-Specific Syntax

- **LATERAL FLATTEN** - Expand arrays/objects into rows
- **QUALIFY** - Filter window function results
- **PIVOT / UNPIVOT** - Transform rows to columns and vice versa
- **MERGE INTO** - Upsert operations
- **COPY INTO** - Load data from stages

### Functions (90+)

| Category | Examples |
|----------|----------|
| JSON | `PARSE_JSON`, `GET`, `GET_PATH`, `OBJECT_KEYS` |
| Array | `ARRAY_CONSTRUCT`, `ARRAY_AGG`, `ARRAY_FLATTEN` |
| Object | `OBJECT_CONSTRUCT`, `OBJECT_INSERT`, `OBJECT_DELETE` |
| Window | `ROW_NUMBER`, `LAG`, `LEAD`, `CONDITIONAL_TRUE_EVENT` |
| String | `SPLIT`, `REGEXP_LIKE`, `REGEXP_REPLACE` |
| Date | `DATEADD`, `DATEDIFF`, `TO_TIMESTAMP` |
| Conditional | `IFF`, `NVL`, `NVL2`, `DECODE` |

### DDL / DML

- `CREATE/DROP DATABASE`, `CREATE/DROP SCHEMA`
- `CREATE/DROP TABLE`, `CREATE/DROP VIEW`
- `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`
- `BEGIN`, `COMMIT`, `ROLLBACK` (transactions)
- `SHOW TABLES`, `DESCRIBE`, `INFORMATION_SCHEMA`

## CI Usage

### GitHub Actions

```yaml
name: Test with Snowflake Emulator

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      snowflake:
        image: ghcr.io/sivchari/snowflake-emulator:latest
        ports:
          - 8080:8080

    steps:
      - uses: actions/checkout@v4

      - name: Run tests
        run: go test ./...
        env:
          SNOWFLAKE_HOST: localhost
          SNOWFLAKE_PORT: 8080
          SNOWFLAKE_PROTOCOL: http
```

### Docker Compose

```yaml
services:
  snowflake:
    image: ghcr.io/sivchari/snowflake-emulator:latest
    ports:
      - "8080:8080"

  app:
    build: .
    depends_on:
      - snowflake
    environment:
      SNOWFLAKE_HOST: snowflake
      SNOWFLAKE_PORT: 8080
```

### testcontainers

```go
container, _ := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
    ContainerRequest: testcontainers.ContainerRequest{
        Image:        "ghcr.io/sivchari/snowflake-emulator:latest",
        ExposedPorts: []string{"8080/tcp"},
        WaitingFor:   wait.ForHTTP("/health").WithPort("8080"),
    },
    Started: true,
})
```

## Limitations

This emulator is designed for testing, not production use:

- No distributed execution (single-node only)
- No access control (GRANT/REVOKE are no-ops)
- No warehouse management
- Transaction isolation is not guaranteed

## License

MIT
