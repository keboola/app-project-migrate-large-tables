# Project Migration - Tables Data

[![GitHub Actions](https://github.com/keboola/app-project-migrate-tables-data/actions/workflows/push.yml/badge.svg)](https://github.com/keboola/app-project-migrate-tables-data/actions/workflows/push.yml)

This application migrates Storage data from one project to another and is designed for use within the Keboola Connection platform.

## Usage

The application migrates table data from one project to another. If a bucket does not exist, it will be created. Similarly, if a table does not exist, it will also be created.

### SAPI mode

In SAPI mode, the application migrates data using the Keboola Storage API.

### Database mode

In Database mode, the application migrates data directly between databases, which is significantly faster. Please note the following requirements:
- Replication from the source database must be enabled.
- Both Snowflake accounts must belong to the same organization.
- The Snowflake user must have the `ACCOUNTADMIN` role.

The following schemas are automatically excluded from migration:
- `INFORMATION_SCHEMA` and `PUBLIC` (system schemas)
- `READER_*` schemas (temporary read-only workspace schemas used for Snowflake sharing)
- `WORKSPACE*` schemas (unless explicitly included via configuration)

To enable replication, the source Snowflake account must first [allow replication](https://docs.snowflake.com/user-guide/account-replication-config#prerequisite-enable-replication-for-accounts-in-the-organization) and then execute the following SQL statement:
```sql
ALTER DATABASE {{SOURCE_DATABASE_NAME}} ENABLE REPLICATION TO ACCOUNTS {{DESTINATION_ACCOUNT_REGION}}.{{DESTINATION_ACCOUNT_NAME}};
```

If these conditions are not met, please use SAPI mode.

## Configuration

The `config.json` configuration file contains the following properties:

- `parameters` - object (required): Application configuration
    - `mode` - string (optional): `sapi` (default) or `database`
    - `dryRun` - boolean (optional): If set to `true`, the application will only simulate the migration.
    - `sourceKbcUrl` - string (required): URL of the source project
    - `sourceKbcToken` - string (required): Storage API token of the source project
    - `tables` - array (optional): List of tables to migrate. If not set, all tables will be migrated.
    - `db` - object (optional in `database` mode; extends the `db` object in `image_parameters`):
        - `host` - string (required): Snowflake host
        - `username` - string (required): Snowflake username
        - `#password` - string (required): Snowflake password
        - `warehouse` - string (required): Snowflake warehouse
- `image_parameters` - object (required): Included from Developer Portal
    - `db` - object (required in `database` mode):
        - `host` - string (required): Snowflake host
        - `username` - string (required): Snowflake username
        - `#password` - string (required): Snowflake password
        - `warehouse` - string (required): Snowflake warehouse

## Example Configurations

### DRY run

```json
{
  "parameters": {
    "mode": "sapi",
    "dryRun": true,
    "sourceKbcUrl": "https://connection.keboola.com/",
    "#sourceKbcToken": "SOURCE_KBC_TOKEN"
  }
}
```

### SAPI mode

```json
{
  "parameters": {
    "mode": "sapi",
    "sourceKbcUrl": "https://connection.keboola.com/",
    "#sourceKbcToken": "SOURCE_KBC_TOKEN"
  }
}
```

### Database mode

```json
{
  "parameters": {
    "mode": "database",
    "sourceKbcUrl": "https://connection.keboola.com/",
    "#sourceKbcToken": "SOURCE_KBC_TOKEN"
  }
}
```

### Database mode with your own Snowflake

```json
{
  "parameters": {
    "mode": "database",
    "sourceKbcUrl": "https://connection.keboola.com/",
    "#sourceKbcToken": "SOURCE_KBC_TOKEN",
    "db": {
      "host": "SNOWFLAKE_HOST",
      "username": "SNOWFLAKE_USERNAME",
      "#password": "SNOWFLAKE_PASSWORD",
      "warehouse": "SNOWFLAKE_WAREHOUSE"
    }
  }
}
```

## Development

Clone this repository and initialize the workspace with the following command:

```shell
git clone https://github.com/keboola/app-project-migrate-tables-data
cd app-project-migrate-tables-data
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Save a `.env` file with the following content:

```shell
SOURCE_CLIENT_URL=
SOURCE_CLIENT_TOKEN=
DESTINATION_CLIENT_URL=
DESTINATION_CLIENT_TOKEN=
```

Run the test suite with the following command:

```shell
docker-compose run --rm dev composer tests
```
