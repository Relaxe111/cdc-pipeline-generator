# Test Database Setup - Avansas Customer

## Overview

This setup includes complete test databases for the **avansas** customer with 1000 test records in each table.

## Database Structure

### MSSQL Source Database

**Database**: `avansas`  
**Tables**: 2  
**Records**: 1000 per table

#### Table: Actor
Complete person/company information table with 100+ columns including:
- **Primary Key**: `actno` (INT, 1-1000)
- **Personal Info**: Navn, Navn2, MellomNavn, fdt, Personnr, Kjonn
- **Address**: Adresse1, Adresse2, Postnr, Poststed, Kommune, Fylke
- **Contact**: tlfprivat, tlfmobil, tlfjobb, epost
- **Employment**: Stillingskode, Arbeidstid, AktivStilllinsgprosent
- **Flags**: pasient, firma, ansatt, HcPerson, etc.
- **Audit**: createdt, createuser, changedt, changeuser

**CDC Enabled**: ✅ Yes

#### Table: AdgangLinjer
Access rights/permissions table:
- **Composite Primary Key**: `SoknadId`, `BrukerNavn`
- **Access Info**: Adgangkode, Fradato, Tildato
- **Audit**: createdt, createuser, changedt, changeuser
- **Sync**: syncedAsma

**CDC Enabled**: ✅ Yes

### PostgreSQL Target Database

**Database**: `consolidated_db`  
**Schema**: `avansas`  
**Tables**: 2  
**Naming**: snake_case (transformed from CamelCase)

#### Table: avansas.Actor
Same structure as MSSQL Actor, but with snake_case column names:
- `actno` → `actno` (primary key, no change)
- `Navn` → `navn`
- `Navn2` → `navn2`
- `MellomNavn` → `mellom_navn`
- `tlfmobil` → `tlfmobil`
- `AktivStilllinsgprosent` → `aktiv_stilllinsgprosent`
- etc.

#### Table: avansas.adgangs_linjer
Same structure as MSSQL AdgangLinjer, with snake_case:
- `SoknadId` → `soknad_id`
- `BrukerNavn` → `bruker_navn`
- `Adgangkode` → `adgangkode`
- etc.

## Initialization Scripts

### MSSQL Scripts (Auto-executed on first startup)

Located in: `scripts/mssql-init/`

1. **01-create-avansas-database.sql**
   - Creates `avansas` database
   - Drops existing if present (for clean restarts)

2. **02-enable-cdc.sql**
   - Enables CDC on `avansas` database
   - Required for Debezium connector

3. **03-create-tables.sql**
   - Creates `Actor` table (100+ columns)
   - Creates `AdgangLinjer` table
   - Enables CDC on both tables
   - Creates performance indexes

4. **04-insert-test-data.sql**
   - Inserts 1000 test records into `Actor`
   - Inserts 1000 test records into `AdgangLinjer`
   - Progress indicators every 100 records

5. **init-mssql.sh**
   - Orchestrates execution of all SQL scripts
   - Waits for MSSQL to be ready
   - Executes scripts in order
   - Reports success/failure

### PostgreSQL Scripts (Auto-executed on first startup)

Located in: `scripts/postgres-init/`

1. **01-init-databases.sql**
   - Creates `consolidated_db` database
   - Creates `avansas` schema
   - Creates `Actor` table (PascalCase)
   - Creates `AdgangLinjer` table (PascalCase)
   - Creates indexes for performance

## Test Data Characteristics

### Actor Table (1000 records)

**Sample Data Patterns:**
- **actno**: Sequential 1-1000
- **Navn**: "Person 1" through "Person 1000"
- **Navn2**: "Etternavn 1" through "Etternavn 1000"
- **Adresse1**: "Testveien 1" through "Testveien 1000"
- **Postnr**: Varied Norwegian postal codes (0123, 5020, 7030, etc.)
- **Poststed**: Rotates through major Norwegian cities (Oslo, Bergen, Trondheim, Stavanger, etc.)
- **Kommune/Fylke**: Realistic Norwegian municipality/county names
- **fdt**: Random birthdates spread over 70 years
- **Personnr**: Every other record has a Norwegian national ID number
- **FdtAr**: Birth years 1950-2010
- **Kjonn**: Alternates 0/1 (female/male)
- **tlfmobil**: "+47 90000001" through "+47 90001000"
- **epost**: "person1@avansas.no" through "person1000@avansas.no"
- **BrukerNavn**: "user1" through "user1000"
- **Stillingskode**: Mix of health professional codes (7170, 2211)
- **Arbeidstid**: "Heltid", "Deltid", or NULL
- **AktivStilllinsgprosent**: 100.0, 50.0, 75.0, or NULL
- **Flags**: Varied boolean flags (pasient, firma, ansatt, etc.)
- **createdt/changedt**: Recent timestamps with slight variations

### AdgangLinjer Table (1000 records)

**Sample Data Patterns:**
- **SoknadId**: Sequential 1-1000
- **BrukerNavn**: "user1" through "user1000" (matches Actor.BrukerNavn)
- **Adgangkode**: Rotates 1-10
- **Fradato**: Dates within the last year
- **Tildato**: Future dates or NULL (20% NULL)
- **CreateActno**: Matches SoknadId (foreign key to Actor.actno)
- **ChangeActno**: Every 3rd record has a value
- **syncedAsma**: Alternates boolean values

## How to Use

### 1. Start the Environment

```bash
# Option 1: Automated setup
./scripts/setup-local.sh

# Option 2: Manual start
docker compose up -d

# The MSSQL init scripts run automatically on first startup
# Watch the logs to see progress:
docker compose logs -f mssql
```

### 2. Verify MSSQL Data

```bash
# Connect to MSSQL
docker exec -it cdc-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd'

# Check database and tables
USE avansas;
SELECT COUNT(*) AS ActorCount FROM dbo.Actor;
SELECT COUNT(*) AS AdgangLinjerCount FROM dbo.AdgangLinjer;

# View sample Actor records
SELECT TOP 10 actno, Navn, Navn2, epost, tlfmobil, Poststed FROM dbo.Actor;

# View sample AdgangLinjer records
SELECT TOP 10 SoknadId, BrukerNavn, Adgangkode, Fradato FROM dbo.AdgangLinjer;

# Check CDC is enabled
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'avansas';
SELECT name FROM sys.tables WHERE is_tracked_by_cdc = 1;
GO
EXIT
```

### 3. Verify PostgreSQL Schema

```bash
# Connect to PostgreSQL
docker exec -it cdc-postgres psql -U postgres -d consolidated_db

# List schemas
\dn

# Check avansas schema tables
\dt avansas.*

# View table structures
\d avansas.Actor
\d avansas.AdgangLinjer

# The tables are empty initially - they will be populated by CDC
SELECT COUNT(*) FROM avansas.Actor;
SELECT COUNT(*) FROM avansas.AdgangLinjer;

\q
```

### 4. Deploy CDC Connector

The connector will automatically sync data from MSSQL to PostgreSQL.

```bash
# Deploy the avansas source connector (Debezium)
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @pipelines/nonprod/avansas-nonprod-source.json

# Deploy the avansas sink connector (JDBC)
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @pipelines/nonprod/avansas-nonprod-sink.json

# Check connector status
curl http://localhost:8083/pipelines/avansas-nonprod-source/status | jq
curl http://localhost:8083/pipelines/avansas-nonprod-sink/status | jq
```

### 5. Monitor Data Flow

```bash
# Watch Redpanda Console
# Open: http://localhost:8080
# Navigate to Topics → Look for:
#   - nonprod.avansas.dbo.Actor
#   - nonprod.avansas.dbo.AdgangLinjer

# Check PostgreSQL for replicated data (after connectors are running)
docker exec -it cdc-postgres psql -U postgres -d consolidated_db -c \
  "SELECT COUNT(*) AS actor_count FROM avansas.Actor;"

docker exec -it cdc-postgres psql -U postgres -d consolidated_db -c \
  "SELECT COUNT(*) AS adgangs_linjer_count FROM avansas.AdgangLinjer;"

# View sample data (should match MSSQL but in snake_case)
docker exec -it cdc-postgres psql -U postgres -d consolidated_db -c \
  "SELECT actno, navn, navn2, epost, tlfmobil, poststed FROM avansas.Actor LIMIT 10;"
```

### 6. Test CDC in Real-Time

```bash
# Insert a new record in MSSQL
docker exec -it cdc-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -Q \
  "USE avansas; INSERT INTO dbo.Actor (actno, Navn, Navn2, epost, createdt, createuser) VALUES (1001, 'Test', 'User', 'test@avansas.no', GETDATE(), 'manual');"

# Wait a few seconds, then check PostgreSQL
docker exec -it cdc-postgres psql -U postgres -d consolidated_db -c \
  "SELECT * FROM avansas.Actor WHERE actno = 1001;"

# Should see the new record with snake_case columns!
```

## Database Management

### Reset MSSQL Database

```bash
# Stop containers
docker compose stop mssql

# Remove MSSQL volume (deletes all data)
docker volume rm adopus-cdc-pipeline_mssql-data

# Restart (init scripts will run again)
docker compose up -d mssql
docker compose logs -f mssql
```

### Reset PostgreSQL Database

```bash
# Stop containers
docker compose stop postgres

# Remove PostgreSQL volume
docker volume rm adopus-cdc-pipeline_postgres-data

# Restart (init scripts will run again)
docker compose up -d postgres
```

### Re-run Init Scripts Manually

```bash
# If you need to re-run without removing volumes:

# MSSQL
docker exec -it cdc-mssql /docker-entrypoint-initdb.d/init-mssql.sh

# Or run individual scripts:
docker exec -it cdc-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' \
  -i /docker-entrypoint-initdb.d/01-create-avansas-database.sql

docker exec -it cdc-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' \
  -i /docker-entrypoint-initdb.d/02-enable-cdc.sql

docker exec -it cdc-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' \
  -i /docker-entrypoint-initdb.d/03-create-tables.sql

docker exec -it cdc-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' \
  -i /docker-entrypoint-initdb.d/04-insert-test-data.sql
```

## Connector Configuration

You'll need to update the connector JSON files to use the avansas database:

**Source Connector** (`pipelines/nonprod/avansas-nonprod-source.json`):
```json
{
  "name": "avansas-nonprod-source",
  "config": {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "database.hostname": "mssql",
    "database.port": "1433",
    "database.user": "sa",
    "database.password": "YourStrong!Passw0rd",
    "database.dbname": "avansas",
    "database.server.name": "nonprod.avansas",
    "table.include.list": "dbo.Actor,dbo.AdgangLinjer",
    "database.history.kafka.bootstrap.servers": "redpanda:9092",
    "database.history.kafka.topic": "schema-changes.avansas"
  }
}
```

**Sink Connector** (`pipelines/nonprod/avansas-nonprod-sink.json`):
```json
{
  "name": "avansas-nonprod-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "connection.url": "jdbc:postgresql://postgres:5432/consolidated_db",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "topics": "nonprod.avansas.dbo.Actor,nonprod.avansas.dbo.AdgangLinjer",
    "table.name.format": "avansas.${topic}",
    "auto.create": "true",
    "auto.evolve": "true",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "transforms": "unwrap,route",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "nonprod\\.avansas\\.dbo\\.(.*)",
    "transforms.route.replacement": "avansas.$1"
  }
}
```

## Troubleshooting

### MSSQL Init Scripts Not Running

```bash
# Check if init script exists
docker exec cdc-mssql ls -la /docker-entrypoint-initdb.d/

# Check MSSQL logs
docker compose logs mssql | grep -i "init\|error\|database"

# Manually run init script
docker exec -it cdc-mssql /docker-entrypoint-initdb.d/init-mssql.sh
```

### CDC Not Capturing Changes

```bash
# Verify CDC is enabled
docker exec -it cdc-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -Q \
  "USE avansas; SELECT name, is_tracked_by_cdc FROM sys.tables WHERE is_tracked_by_cdc = 1;"

# Check SQL Server Agent is running (required for CDC)
docker exec -it cdc-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -Q \
  "EXEC sp_help_job;"
```

### Connector Fails

```bash
# Check connector logs
docker compose logs connect | grep -i error

# Check connector status
curl http://localhost:8083/pipelines/avansas-nonprod-source/status | jq
curl http://localhost:8083/pipelines/avansas-nonprod-sink/status | jq

# Delete and recreate connector
curl -X DELETE http://localhost:8083/pipelines/avansas-nonprod-source
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @pipelines/nonprod/avansas-nonprod-source.json
```

## Summary

- ✅ MSSQL `avansas` database with 2 tables and 1000 records each
- ✅ PostgreSQL `avansas` schema with corresponding snake_case tables
- ✅ CDC enabled on MSSQL tables
- ✅ Automatic initialization on first startup
- ✅ Ready for Debezium connector deployment
- ✅ Full end-to-end CDC testing environment

All scripts and configurations are in place. Just start the environment and deploy the connectors!
