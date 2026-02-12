# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Async query execution support with `?async=true` parameter (#30)
- Statement status polling via `GET /api/v2/statements/{handle}`
- Statement cancellation via `POST /api/v2/statements/{handle}/cancel`
- DATABASE/SCHEMA commands using SnowflakeCatalog (#29)
  - `CREATE DATABASE`, `DROP DATABASE`
  - `CREATE SCHEMA`, `DROP SCHEMA`
  - `USE DATABASE`, `USE SCHEMA`
- INFORMATION_SCHEMA support (#28)
  - `INFORMATION_SCHEMA.TABLES`
  - `INFORMATION_SCHEMA.COLUMNS`
  - `INFORMATION_SCHEMA.SCHEMATA`
- COPY INTO with stage support (#27)
- Transaction support: `BEGIN`, `COMMIT`, `ROLLBACK` (#26)
- DML operations: `INSERT`, `UPDATE`, `DELETE`, `MERGE INTO` (#26)
- DDL operations: `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`, `TRUNCATE` (#26)
- Window functions: `CONDITIONAL_TRUE_EVENT`, `CONDITIONAL_CHANGE_EVENT` (#25)
- Extended window functions (#24)
  - `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`
  - `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`
  - `PERCENT_RANK`, `CUME_DIST`, `RATIO_TO_REPORT`
- Advanced SQL features (#23)
  - `PIVOT`, `UNPIVOT`
  - `TABLESAMPLE`, `SAMPLE`
  - `QUALIFY` clause
- Context functions (#22)
  - `CURRENT_USER`, `CURRENT_ROLE`, `CURRENT_DATABASE`
  - `CURRENT_SCHEMA`, `CURRENT_WAREHOUSE`
- Hash functions: `SHA1`, `SHA2`, `MD5` (#22)
- Numeric functions: `DIV0`, `DIV0NULL` (#22)
- Date/Time functions (#20)
  - `DATEADD`, `DATEDIFF`, `DATE_TRUNC`, `DATE_PART`
  - `TO_DATE`, `TO_TIMESTAMP`, `LAST_DAY`, `DAYNAME`, `MONTHNAME`
- String functions (#21)
  - `SPLIT`, `STRTOK`, `STRTOK_TO_ARRAY`
  - `REGEXP_LIKE`, `REGEXP_SUBSTR`, `REGEXP_REPLACE`, `REGEXP_COUNT`
  - `CONTAINS`, `STARTSWITH`, `ENDSWITH`
  - `LPAD`, `RPAD`, `REVERSE`, `TRANSLATE`, `CHARINDEX`
- LATERAL FLATTEN support
- Aggregate functions: `ARRAY_AGG`, `OBJECT_AGG`, `LISTAGG`
- VARIANT/ARRAY/OBJECT type support
- JSON functions: `PARSE_JSON`, `TO_JSON`, `GET`, `GET_PATH`
- Array functions: `ARRAY_CONSTRUCT`, `ARRAY_APPEND`, `ARRAY_CAT`, etc.
- Object functions: `OBJECT_CONSTRUCT`, `OBJECT_INSERT`, `OBJECT_DELETE`, etc.
- TRY_* functions: `TRY_PARSE_JSON`, `TRY_TO_NUMBER`, `TRY_TO_DATE`, `TRY_TO_BOOLEAN`
- Conditional functions: `IFF`, `NVL`, `NVL2`, `DECODE`
- v1 query API for gosnowflake driver compatibility
- Go integration tests

### Changed

- Updated gosnowflake driver to v1.18.1

## [0.1.0] - Initial Release

### Added

- Basic SQL execution via Snowflake SQL API v2
- HTTP server with `/api/v2/statements` endpoint
- Health check endpoint at `/health`
- Login endpoint at `/session/v1/login-request`
- DataFusion-based SQL execution engine
