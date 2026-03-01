# 🖥️ Web UI Access Guide

Quick reference for accessing the CDC pipeline web interfaces.

## 🚀 Prerequisites

Start the services first:
```bash
docker compose up -d
```

Or use the setup script:
```bash
cdc setup-local
```

Wait 30-60 seconds for all services to be healthy.

---

## 🎛️ Redpanda Console (Kafka Monitoring)

**URL**: http://localhost:8080

**No login required** ✓

### What you can do:
- ✅ View all CDC topics
- ✅ Browse Kafka messages in real-time
- ✅ Monitor consumer groups and lag
- ✅ Inspect Debezium CDC event structure

### Quick Navigation:
1. **Topics** → See all `local.avansas.*` topics
2. Click a topic → **Messages** tab → View CDC events
3. **Consumer Groups** → Check pipeline health

---

## 🗄️ Adminer (Database Management)

**URL**: http://localhost:8090

**Login required** (credentials below)

### PostgreSQL (Target Database)
```
System:   PostgreSQL
Server:   postgres
Username: postgres
Password: postgres
Database: consolidated_db
```

### MSSQL (Source Database)
```
System:   MS SQL (beta)
Server:   mssql
Username: sa
Password: YourStrong!Passw0rd
Database: avansas
```

### What you can do:
- ✅ Browse database tables and schemas
- ✅ Run SQL queries
- ✅ Export data to CSV/SQL
- ✅ Compare source and target data
- ✅ Verify CDC replication

### Quick Tasks:

**Check if data was replicated:**
1. Login to PostgreSQL
2. Navigate to `avansas` schema
3. Click on `Actor` table
4. Click **Select data** → See replicated records

**Compare source and target:**
1. Open Adminer in two browser tabs
2. Tab 1: Login to MSSQL → Query source table
3. Tab 2: Login to PostgreSQL → Query target schema

---

## 📊 Pipeline Metrics

### Source Pipeline (MSSQL → Kafka)
- **URL**: http://localhost:4195/stats
- **Health Check**: http://localhost:4195/ping (should return "pong")

### Sink Pipeline (Kafka → PostgreSQL)
- **URL**: http://localhost:4196/stats
- **Health Check**: http://localhost:4196/ping (should return "pong")

### View Stats (Command Line)
```bash
# Source pipeline metrics
curl http://localhost:4195/stats | jq

# Sink pipeline metrics  
curl http://localhost:4196/stats | jq

# Quick health check
curl http://localhost:4195/ping && curl http://localhost:4196/ping
```

---

## 🔍 Troubleshooting

### Can't access Redpanda Console (localhost:8080)
```bash
# Check if container is running
docker ps | grep redpanda-console

# Check logs
docker logs cdc-redpanda-console

# Restart if needed
docker compose restart redpanda-console
```

### Can't access Adminer (localhost:8090)
```bash
# Check if container is running
docker ps | grep adminer

# Check logs
docker logs cdc-adminer

# Restart if needed
docker compose restart adminer
```

### Adminer login fails
1. Check credentials in `.env` file
2. For PostgreSQL, ensure container is running: `docker ps | grep postgres`
3. For MSSQL, ensure container is running: `docker ps | grep mssql`

### Pipeline stats return error
```bash
# Check if pipelines are running
docker ps | grep bento

# Check pipeline logs
docker logs cdc-bento-source --tail 20
docker logs cdc-bento-sink --tail 20

# Restart pipelines
cdc reload-pipelines
```

---

## 💡 Pro Tips

### Bookmark These URLs
- 🎛️ Console: http://localhost:8080
- 🗄️ Adminer: http://localhost:8090
- 📊 Source Stats: http://localhost:4195/stats
- 📊 Sink Stats: http://localhost:4196/stats

### Quick Health Check Script
```bash
# Check all services
cdc verify

# Or manually
curl -s http://localhost:8080 > /dev/null && echo "✓ Console OK" || echo "✗ Console FAIL"
curl -s http://localhost:8090 > /dev/null && echo "✓ Adminer OK" || echo "✗ Adminer FAIL"
curl -s http://localhost:4195/ping && echo "✓ Source OK" || echo "✗ Source FAIL"
curl -s http://localhost:4196/ping && echo "✓ Sink OK" || echo "✗ Sink FAIL"
```

### View Real-Time CDC Events
1. Open Redpanda Console: http://localhost:8080
2. Go to **Topics** → `nonprod.avansas.AdOpusTest.dbo.Actor`
3. Click **Messages** tab
4. In another terminal, insert test data:
   ```bash
   docker exec cdc-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa \
     -P 'YourStrong!Passw0rd' -C -d avansas \
     -Q "INSERT INTO Actor (actno, Navn) VALUES (88888, 'Live Test');"
   ```
5. Watch the message appear in Redpanda Console within seconds!

---

## 📚 Related Documentation

- [Full Monitoring Guide](3-operations/MONITORING.md) - Detailed monitoring documentation
- [Troubleshooting](2-reference/TROUBLESHOOTING.md) - Common issues and solutions
- [Quick Reference](2-reference/QUICK_REFERENCE.md) - All commands and workflows

---

**🎉 Quick Start Checklist:**
- [ ] Services running: `docker compose up -d`
- [ ] Console accessible: http://localhost:8080
- [ ] Adminer accessible: http://localhost:8090
- [ ] Pipelines healthy: `cdc reload-pipelines`
- [ ] Test data flow: `cdc verify`
