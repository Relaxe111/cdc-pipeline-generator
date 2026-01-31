# 🎛️ Local Monitoring - Quick Reference Card

## 🌐 Web UIs

### Redpanda Console - Kafka Monitoring
- **URL**: http://localhost:8080
- **Login**: No authentication required ✓
- **Use For**:
  - View CDC topics (`nonprod.avansas.AdOpusTest.dbo.Actor`)
  - Inspect CDC messages in real-time
  - Monitor consumer groups
  - Check consumer lag

### Adminer - Database Management
- **URL**: http://localhost:8090
- **Use For**: Browse databases, run SQL queries, compare source vs target

#### PostgreSQL (Target Database)
```
System:   PostgreSQL
Server:   postgres
Username: postgres
Password: postgres
Database: consolidated_db
```

#### SQL Server (Source Database)
```
System:   MS SQL (server)
Server:   mssql
Username: sa
Password: YourStrong!Passw0rd
Database: avansas
```

---

## 📊 API Endpoints

### Source Pipeline (MSSQL → Redpanda)
```bash
# Health check
curl http://localhost:4195/ping

# Statistics
curl http://localhost:4195/stats | jq

# Prometheus metrics
curl http://localhost:4195/metrics
```

### Sink Pipeline (Redpanda → PostgreSQL)
```bash
# Health check
curl http://localhost:4196/ping

# Statistics
curl http://localhost:4196/stats | jq

# Prometheus metrics
curl http://localhost:4196/metrics
```

---

## 🔍 Quick Tests

### Check CDC is Working
```bash
# Insert test record
docker exec cdc-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' -d avansas -C \
  -Q "INSERT INTO Actor (ActNo, FirstName, LastName, Email) 
      VALUES (9999, 'Test', 'User', 'test@example.com')"

# View in Redpanda Console
# → http://localhost:8080 → Topics → nonprod.avansas.AdOpusTest.dbo.Actor → Messages

# Verify in PostgreSQL (via Adminer or CLI)
docker exec cdc-postgres psql -U postgres -d consolidated_db \
  -c "SELECT * FROM avansas.actor WHERE actno = 9999;"
```

### Check Transformations
```bash
# Compare field names
# MSSQL: ActorId, FirstName, LastName
# PostgreSQL: actor_id, first_name, last_name (snake_case!)

# Check metadata fields (only in PostgreSQL)
docker exec cdc-postgres psql -U postgres -d consolidated_db \
  -c "SELECT actno, first_name, __sync_timestamp, __cdc_operation 
      FROM avansas.actor ORDER BY __sync_timestamp DESC LIMIT 5;"
```

---

## 🗂️ Database Access

### PostgreSQL (CLI)
```bash
docker exec -it cdc-postgres psql -U postgres -d consolidated_db
```

### SQL Server (CLI)
```bash
docker exec -it cdc-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' -d avansas -C
```

### List Topics
```bash
docker exec cdc-redpanda rpk topic list
```

### Consume Topic Messages
```bash
docker exec cdc-redpanda rpk topic consume \
  nonprod.avansas.AdOpusTest.dbo.Actor -n 5
```

---

## 📋 Service Status

### Check All Services
```bash
docker ps
```

Should show:
- ✅ `cdc-postgres` - PostgreSQL target
- ✅ `cdc-mssql` - SQL Server source
- ✅ `cdc-redpanda` - Message broker
- ✅ `cdc-redpanda-connect-source` - CDC source pipeline
- ✅ `cdc-redpanda-connect-sink` - CDC sink pipeline
- ✅ `cdc-redpanda-console` - Monitoring UI
- ✅ `cdc-adminer` - Database UI

### View Logs
```bash
# All services
docker compose logs -f

# Specific service
docker logs cdc-redpanda-connect-source -f
docker logs cdc-redpanda-connect-sink -f
docker logs cdc-redpanda -f
docker logs cdc-postgres -f
docker logs cdc-mssql -f
```

---

## 🚀 Common Commands

```bash
# Start everything
docker compose up -d

# Stop everything
docker compose down

# Restart pipelines after config change
docker compose restart redpanda-connect-source redpanda-connect-sink

# Generate pipelines
cd pipelines
python3 generate_pipelines.py avansas local

# Validate customer configs
cd pipelines
python3 scripts/4-validate-customers.py

# Clean slate (WARNING: deletes all data)
docker compose down -v
```

---

## 🆘 Quick Troubleshooting

| Problem | Solution |
|---------|----------|
| Can't access localhost:8080 | `docker logs cdc-redpanda-console` |
| Can't access localhost:8090 | `docker logs cdc-adminer` |
| Adminer login fails | Check credentials in `.env` file |
| No topics in Redpanda Console | `cd pipelines && python3 generate_pipelines.py` |
| Pipeline not running | `curl http://localhost:4195/ping` |
| No data flowing | Check `docker logs cdc-redpanda-connect-source` |

---

## 📚 Full Documentation

- **[MONITORING_GUIDE.md](MONITORING_GUIDE.md)** - Complete monitoring guide
- **[GETTING_STARTED.md](GETTING_STARTED.md)** - Setup and usage
- **[CUSTOMER_CONFIG_WORKFLOW.md](CUSTOMER_CONFIG_WORKFLOW.md)** - Add customers
- **[README.md](README.md)** - Architecture overview

---

**💡 Tip**: Bookmark http://localhost:8080 and http://localhost:8090 for easy access!
