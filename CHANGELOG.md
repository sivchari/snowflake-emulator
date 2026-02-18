# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.1.1](https://github.com/sivchari/snowflake-emulator/compare/v0.1.0...v0.1.1) - 2026-02-18
- docs: improve README with quick start, supported SQL, and CI usage by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/51
- chore(deps): bump the rust-dependencies group with 3 updates by @dependabot[bot] in https://github.com/sivchari/snowflake-emulator/pull/50
- chore(deps): bump actions/checkout from 4 to 6 by @dependabot[bot] in https://github.com/sivchari/snowflake-emulator/pull/49

## [v0.1.0](https://github.com/sivchari/snowflake-emulator/commits/v0.1.0) - 2026-02-17
- feat: setup by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/1
- add gitkeep by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/2
- feat: setup by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/3
- Feat server by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/4
- feat setup by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/5
- exec: select 1 by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/6
- Add wasm by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/7
- Swich ts by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/8
- convert type by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/9
- fix: workflow by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/10
- fix: script by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/11
- Add sqlite by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/12
- feat: exec query on sqlite3 by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/14
- feat: replace SQLite with DataFusion engine by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/15
- test: add integration tests for CREATE TABLE / INSERT / SELECT by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/16
- feat: add gosnowflake driver compatibility with v1 API by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/17
- feat: implement Phase 2 Snowflake UDF support (14 functions) by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/18
- feat: Phase 4 - Extended function support by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/20
- feat: Phase 5 - Extended Features (String/Window/QUALIFY/DDL/GET) by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/21
- feat: Phase 6 - Functions and Metadata Commands by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/22
- feat: add Phase 7 advanced SQL features by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/23
- feat: add Phase 8 extended window functions by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/24
- feat: implement CONDITIONAL_TRUE_EVENT and CONDITIONAL_CHANGE_EVENT window functions by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/25
- feat: implement Phase 9 DML/DDL operations by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/26
- feat: implement COPY INTO with stage support by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/27
- feat(info-schema): add INFORMATION_SCHEMA support by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/28
- feat(catalog): implement DATABASE/SCHEMA commands by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/29
- feat(server): add async query execution support by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/30
- ci: add release infrastructure and CI improvements by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/31
- feat: add Phase 4 function extensions (DateTime, Context) by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/38
- chore(deps): bump actions/setup-go from 5 to 6 by @dependabot[bot] in https://github.com/sivchari/snowflake-emulator/pull/32
- chore(deps): bump github.com/snowflakedb/gosnowflake from 1.18.1 to 1.19.0 in /go by @dependabot[bot] in https://github.com/sivchari/snowflake-emulator/pull/33
- chore(deps): bump actions/upload-artifact from 4 to 6 by @dependabot[bot] in https://github.com/sivchari/snowflake-emulator/pull/34
- chore(deps): bump actions/checkout from 4 to 6 by @dependabot[bot] in https://github.com/sivchari/snowflake-emulator/pull/35
- chore(deps): bump actions/download-artifact from 4 to 7 by @dependabot[bot] in https://github.com/sivchari/snowflake-emulator/pull/36
- feat: upgrade to DataFusion 52 by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/39
- ci: add tagpr for automated release management by @sivchari in https://github.com/sivchari/snowflake-emulator/pull/48

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
