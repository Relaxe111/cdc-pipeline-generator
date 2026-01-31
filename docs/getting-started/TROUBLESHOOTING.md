# Troubleshooting Guide

## Architecture Notes

This CDC pipeline uses **Redpanda** - a production-ready, Kafka-compatible streaming platform that requires no Zookeeper. Redpanda provides the full Kafka API, so all Kafka client tools work seamlessly, plus built-in schema registry and HTTP proxy.

## Common Issues and Solutions

### 1. Connector Fails to Start

#### Symptoms
- Connector status shows `FAILED`
- Logs show connection errors
- Tasks are not running

#### Diagnosis

```bash
# Check connector status
curl http://localhost:8083/pipelines/{connector-name}/status | jq

# Check detailed logs
kubectl logs -f deployment/kafka-connect -n cdc-pipeline | grep ERROR

# In local environment
docker-compose logs -f connect | grep ERROR

# Check Redpanda broker health
curl http://localhost:9644/v1/status/ready
docker-compose logs -f redpanda | grep ERROR
```

#### Solutions

**Database Connection Issues:**

```bash
# Test MSSQL connectivity
docker exec -it cdc-connect bash
nc -zv mssql-server 1433

# Test PostgreSQL connectivity
nc -zv postgres-server 5432

# Verify credentials
echo $MSSQL_NONPROD_PASSWORD
```

**Fix:** Update secrets or environment variables with correct credentials.

**CDC Not Enabled on MSSQL:**

```sql
-- Check if CDC is enabled on database
SELECT name, is_cdc_enabled 
FROM sys.databases 
WHERE name = 'CustomerDB_Alpha';

-- Enable CDC if not enabled
USE CustomerDB_Alpha;
EXEC sys.sp_cdc_enable_db;

-- Check if CDC is enabled on tables
SELECT name, is_tracked_by_cdc 
FROM sys.tables 
WHERE schema_id = SCHEMA_ID('dbo');

-- Enable CDC on table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'Customers',
    @role_name = NULL;
```

### 2. Data Not Replicating

#### Symptoms
- Changes in MSSQL don't appear in PostgreSQL
- Topics are empty
- Connector is running but no activity

#### Diagnosis

```bash
# Check if topics are created (using Kafka-compatible tools)
kafka-topics --bootstrap-server localhost:9092 --list

# Or using Docker with Redpanda
docker exec cdc-redpanda rpk topic list

# Check topic contents (Kafka console consumer works with Redpanda)
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic alpha_internal.customers \
  --from-beginning \
  --max-messages 10

# Check connector metrics
curl http://localhost:8083/pipelines/{connector-name}/tasks/0/status | jq
```

#### Solutions

**Snapshot Not Completed:**

Check connector logs for snapshot progress:
```
INFO Initial snapshot is running
```

Wait for snapshot to complete. For large databases, this can take hours.

**Table Filter Misconfiguration:**

```json
{
  "table.include.list": "dbo.Customers,dbo.Orders"
}
```

Ensure table names match exactly (case-sensitive).

**Transform Errors:**

```bash
# Check for transform errors in logs
docker-compose logs connect | grep "Transform"

# Common issue: field not found
# Solution: Update field mapping in connector config
```

### 3. Performance Issues

#### Symptoms
- High lag between source and sink
- Slow replication
- Connect worker using high CPU/memory

#### Diagnosis

```bash
# Check resource usage
kubectl top pods -n cdc-pipeline

# Local environment
docker stats cdc-connect

# Check connector lag
curl http://localhost:8083/pipelines/{connector-name}/status | jq '.tasks[].lag'
```

#### Solutions

**Increase Parallelism:**

```json
{
  "tasks.max": "4",
  "max.batch.size": "4096",
  "query.fetch.size": "20000"
}
```

**Increase Resources:**

Kubernetes:
```yaml
resources:
  requests:
    memory: "4Gi"
    cpu: "2000m"
  limits:
    memory: "8Gi"
    cpu: "4000m"
```

Docker Compose:
```yaml
environment:
  KAFKA_HEAP_OPTS: "-Xms2G -Xmx4G"
```

**Optimize PostgreSQL:**

```sql
-- Increase work_mem for bulk inserts
ALTER SYSTEM SET work_mem = '256MB';

-- Tune checkpoint settings
ALTER SYSTEM SET checkpoint_timeout = '15min';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;

-- Reload configuration
SELECT pg_reload_conf();
```

### 4. Schema Evolution Errors

#### Symptoms
- Connector fails after DDL changes
- Sink connector rejects records
- Schema mismatch errors

#### Diagnosis

```bash
# Check schema history topic (using Kafka-compatible tools with Redpanda)
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic schema-history.nonprod.alpha \
  --from-beginning

# Check sink connector auto.evolve setting
curl http://localhost:8083/pipelines/postgres-sink-alpha/config | jq '.auto.evolve'
```

#### Solutions

**Enable Auto Evolution:**

```json
{
  "auto.create": "true",
  "auto.evolve": "true"
}
```

**Manual Schema Update:**

```sql
-- PostgreSQL: Add missing column
ALTER TABLE alpha_internal.customers 
ADD COLUMN new_field VARCHAR(100);
```

**Reset Schema History (Last Resort):**

```bash
# Delete schema history topic (Redpanda supports standard Kafka admin tools)
kafka-topics \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic schema-history.nonprod.alpha

# Restart connector (will do full snapshot)
curl -X POST http://localhost:8083/pipelines/mssql-source-nonprod/restart
```

### 5. CamelCase to snake_case Not Working

#### Symptoms
- PostgreSQL tables have CamelCase columns
- Field mapping errors
- Transform not applied

#### Diagnosis

```bash
# Check actual topic data (Kafka console tools work with Redpanda)
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic alpha_internal.customers \
  --max-messages 1 \
  --from-beginning | jq
```

#### Solutions

**Verify Transform Configuration:**

```json
{
  "transforms": "unwrap,route,renameFields",
  "transforms.renameFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
  "transforms.renameFields.renames": "CustomerId:customer_id,CustomerName:customer_name"
}
```

**Ensure All Fields Are Mapped:**

List all MSSQL fields:
```sql
SELECT COLUMN_NAME 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'Customers';
```

Add all to rename configuration.

**Test Regex Transformation:**

```groovy
// Test in Groovy console
def camelToSnake(String s) {
    s.replaceAll(/([A-Z]+)([A-Z][a-z])/, '$1_$2')
     .replaceAll(/([a-z\d])([A-Z])/, '$1_$2')
     .toLowerCase()
}

println camelToSnake("OrderLineItemId")  // Should output: order_line_item_id
```

### 6. Connector Keeps Restarting

#### Symptoms
- Connector state alternates between RUNNING and FAILED
- CrashLoopBackOff in Kubernetes
- Continuous restart in Docker

#### Diagnosis

```bash
# Check restart count
kubectl get pods -n cdc-pipeline

# Check pod events
kubectl describe pod kafka-connect-xxx -n cdc-pipeline

# Check connector restart attempts
curl http://localhost:8083/pipelines/{connector-name}/status | jq '.connector'
```

#### Solutions

**OOMKilled (Out of Memory):**

```yaml
# Increase memory limits
resources:
  limits:
    memory: "8Gi"
```

**Database Connection Pool Exhausted:**

```json
{
  "connection.pool.size": "10",
  "connection.timeout.ms": "30000"
}
```

**Invalid Configuration:**

```bash
# Validate connector config
curl -X PUT http://localhost:8083/connector-plugins/io.debezium.connector.sqlserver.SqlServerConnector/config/validate \
  -H "Content-Type: application/json" \
  -d @pipelines/mssql-source-nonprod.json | jq
```

### 7. Dead Letter Queue Issues

#### Symptoms
- Records not reaching PostgreSQL
- DLQ topic filling up
- Silent data loss

#### Diagnosis

```bash
# Check DLQ topic (using Kafka tools with Redpanda)
kafka-topics --bootstrap-server localhost:9092 --list | grep dlq

# Read DLQ messages
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic dlq-postgres-sink \
  --from-beginning
```

#### Solutions

**Analyze DLQ Messages:**

```bash
# Get detailed error information
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic dlq-postgres-sink \
  --from-beginning \
  --property print.headers=true \
  --property print.timestamp=true
```

**Common Fixes:**

1. **Data Type Mismatch:**
```json
{
  "transforms": "cast",
  "transforms.cast.type": "org.apache.kafka.connect.transforms.Cast$Value",
  "transforms.cast.spec": "age:int32,price:float64"
}
```

2. **Constraint Violations:**
```sql
-- Temporarily disable constraints
ALTER TABLE alpha_internal.customers DISABLE TRIGGER ALL;

-- Process DLQ messages manually

-- Re-enable constraints
ALTER TABLE alpha_internal.customers ENABLE TRIGGER ALL;
```

3. **Increase Error Tolerance (Not Recommended for Production):**
```json
{
  "errors.tolerance": "all",
  "errors.log.enable": "true"
}
```

### 8. Kafka Connect Won't Start

#### Symptoms
- Container exits immediately
- No REST API available
- "Address already in use" errors

#### Diagnosis

```bash
# Check container logs
docker-compose logs connect

# Kubernetes
kubectl logs deployment/kafka-connect -n cdc-pipeline

# Check port availability
lsof -i :8083
```

#### Solutions

**Port Conflict:**

```bash
# Kill process using port
lsof -ti:8083 | xargs kill -9

# Or change port in config
```

**Plugin Path Issues:**

```bash
# Verify plugins are installed
docker exec cdc-connect ls -la /usr/share/confluent-hub-components/

# Should see:
# - debezium-sqlserver/
# - confluentinc-kafka-connect-jdbc/
```

**Kafka Broker Not Available:**

```bash
# Check Kafka connectivity
docker exec cdc-connect nc -zv kafka 29092

# Wait for Kafka to be ready
docker-compose up -d kafka
sleep 30
docker-compose up -d connect
```

## Debug Mode

### Enable Verbose Logging

**Local (docker-compose):**

Add to `docker-compose.yml`:
```yaml
environment:
  CONNECT_LOG4J_ROOT_LOGLEVEL: DEBUG
  CONNECT_LOG4J_LOGGERS: "io.debezium=DEBUG,org.apache.kafka.connect=DEBUG"
```

**Kubernetes:**

```yaml
env:
  - name: CONNECT_LOG4J_ROOT_LOGLEVEL
    value: "DEBUG"
  - name: CONNECT_LOG4J_LOGGERS
    value: "io.debezium=DEBUG,org.apache.kafka.connect=DEBUG"
```

### Interactive Debugging

```bash
# Shell into Connect container
docker exec -it cdc-connect bash

# Or Kubernetes
kubectl exec -it deployment/kafka-connect -n cdc-pipeline -- bash

# Test database connectivity
/opt/mssql-tools/bin/sqlcmd -S mssql-server -U sa -P password -Q "SELECT @@VERSION"

# Test Redpanda connectivity (Kafka API compatible)
docker exec cdc-redpanda rpk topic list

# Check Java process
ps aux | grep java
jps -v
```

## Redpanda-Specific Troubleshooting

### 1. Redpanda Cluster Issues

#### Check Redpanda Health

```bash
# Local development
curl http://localhost:9644/v1/status/ready
docker-compose logs -f redpanda

# Kubernetes
curl http://redpanda-client:9644/v1/status/ready
kubectl logs -f statefulset/redpanda -n cdc-pipeline

# Check cluster health
docker exec cdc-redpanda rpk cluster health
```

#### Verify Cluster Status

```bash
# Check cluster info
docker exec cdc-redpanda rpk cluster info

# List all brokers
docker exec cdc-redpanda rpk redpanda admin brokers list

# Check partition distribution
docker exec cdc-redpanda rpk cluster partitions
```

#### Redpanda Connection Issues

```bash
# Verify Redpanda is listening
netstat -tuln | grep 9092

# Test connection
telnet localhost 9092

# Check Kafka API compatibility
docker exec cdc-redpanda rpk cluster info --brokers localhost:9092
```

### 2. Storage and Performance

Redpanda uses local disk storage for high performance:

- **Monitor disk usage**: Use `rpk cluster storage` to check disk usage
- **Data retention**: Configure retention policies per topic
- **Horizontal scaling**: Scale StatefulSet replicas as needed
- **Backup strategy**: Backup persistent volumes or use Redpanda Remote Read Replicas
- **Recovery**: StatefulSets maintain data persistence across pod restarts

### 3. Production-Ready Architecture

Redpanda is designed for production CDC workloads:

```bash
# Verify bootstrap servers environment variable
echo $CONNECT_BOOTSTRAP_SERVERS
# Should be: redpanda:9092 or redpanda-client:9092

# Test with Kafka tools (they work with Redpanda)
docker exec cdc-redpanda rpk topic list
docker exec cdc-redpanda rpk cluster info
```

## Emergency Procedures

### Complete Reset (Local Development)

```bash
# Stop all services
docker-compose down -v

# Remove volumes (includes Redpanda storage)
docker volume prune -f

# Restart
docker-compose up -d

# Reinitialize
./scripts/setup-local-env.sh
./scripts/deploy-connectors.sh local
```

### Pause Replication

```bash
# Pause connector
curl -X PUT http://localhost:8083/pipelines/mssql-source-nonprod/pause

# Resume when ready
curl -X PUT http://localhost:8083/pipelines/mssql-source-nonprod/resume
```

### Force Full Resync

```bash
# Delete connector
curl -X DELETE http://localhost:8083/pipelines/mssql-source-nonprod

# Delete offset topic (stored in Redpanda)
kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic cdc-connect-offsets

# Recreate connector
curl -X POST -H "Content-Type: application/json" \
  --data @pipelines/mssql-source-nonprod.json \
  http://localhost:8083/connectors
```

## Getting Help

### Collect Diagnostic Information

```bash
#!/bin/bash
# Save as collect-diagnostics.sh

mkdir -p diagnostics

# Connector status
curl -s http://localhost:8083/connectors | jq > diagnostics/connectors.json

# Logs
kubectl logs deployment/kafka-connect -n cdc-pipeline --tail=5000 > diagnostics/connect.log

# Configuration
kubectl get configmap kafka-connect-config -n cdc-pipeline -o yaml > diagnostics/configmap.yaml

# Create archive
tar -czf diagnostics-$(date +%Y%m%d-%H%M%S).tar.gz diagnostics/
```

### Support Channels

- **Internal Wiki**: https://wiki.carasent.com/cdc-pipeline
- **Slack**: #cdc-support
- **Email**: data-engineering@carasent.com
- **Escalation**: On-call rotation in PagerDuty
