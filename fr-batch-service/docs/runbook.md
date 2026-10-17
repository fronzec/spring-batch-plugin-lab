# Operational Runbook — fr-batch-service

## 1. System Overview

```text
                   ┌─────────────┐
                   │   Grafana   │  :3000 (dashboards)
                   └──────┬──────┘
                          │
                   ┌──────┴──────┐
                   │  Prometheus │  :9090 (metrics scraper)
                   └──────┬──────┘
                          │ scrape /api/batch-service/actuator/prometheus
                   ┌──────┴──────┐
                   │   fr-batch  │  :8080 (Spring Boot + plugin executor)
                   └──────┬──────┘
                          │ JDBC
                   ┌──────┴──────┐
                   │   MySQL     │  :3306 (Flyway migrations, job metadata)
                   └─────────────┘
```

- **fr-batch-service**: Spring Boot 3.x application serving the plugin management REST API and
  executing Spring Batch jobs loaded from dynamic plugin JARs.
- **MySQL 8.2**: Persistent store for job definitions, approval records, audit log, and Spring Batch
  metadata tables.
- **Prometheus**: Scrapes Micrometer metrics every 15 s from the `/actuator/prometheus` endpoint.
- **Grafana**: Pre-built dashboard monitoring plugin lifecycle, JVM health, and HTTP error rates.

______________________________________________________________________

## 2. Startup

### Prerequisites

- Docker Engine ≥ 24.0 and Docker Compose ≥ 2.20 installed.
- Environment variable `DB_PASSWORD` set (root password for MySQL).
- Optional: `GRAFANA_PASSWORD` (defaults to `admin`).

### Procedure

```bash
# 1. Start all services (builds app image if needed)
docker compose up -d

# 2. Wait for all services to report healthy
docker compose ps

# 3. Verify app health endpoint
curl -s http://localhost:8080/api/batch-service/actuator/health

# 4. Verify Prometheus is scraping
curl -s http://localhost:9090/api/v1/targets | grep -A2 '"job":"fr-batch-service"'

# 5. Smoke-test — list plugins (public endpoint)
curl -s http://localhost:8080/api/batch-service/jobs/plugins | jq .

# 6. Login to Grafana at http://localhost:3000 (admin / ${GRAFANA_PASSWORD:-admin})
#    Import the dashboard from fr-batch-service/docs/grafana-dashboard.json
```

### Expected result

All 4 containers show `healthy` in `docker compose ps`. The health endpoint returns
`{"status":"UP"}`. Grafana login works and the plugin dashboard shows live metrics.

______________________________________________________________________

## 3. Shutdown

### Graceful shutdown

```bash
# 1. List running jobs (none should be executing)
curl -s -u admin:admin123 \
  http://localhost:8080/api/batch-service/jobs/executions | jq .

# 2. Gracefully stop Spring Boot (triggers @PreDestroy, drains job threads)
curl -s -X POST -u admin:admin123 \
  http://localhost:8080/api/batch-service/actuator/shutdown

# 3. Wait 10 seconds for graceful drain, then stop containers
docker compose stop

# 4. Remove containers (volumes persist)
docker compose down
```

### Force shutdown (emergency)

```bash
docker compose down -v   # WARNING: destroys volumes — use only in emergency
```

### Verification

```bash
docker compose ps     # Should show "Exited" for all services
```

______________________________________________________________________

## 4. Plugin Lifecycle

All commands assume basic auth (`admin:admin123`). Replace credentials for production.

### 4.1 Upload a plugin JAR

```bash
curl -X POST -u admin:admin123 \
  -F "file=@/path/to/plugin.jar" \
  -F "jobName=my-custom-job" \
  -F "version=1.0.0" \
  -F "mainClassName=com.example.MyJobPlugin" \
  http://localhost:8080/api/batch-service/jobs/upload
```

**Expected**: HTTP 201 Created. Response includes `jobDefinitionId` and `checksumSha256`.

**Error handling**:

- HTTP 400 — invalid JAR, missing manifest, or signature verification failed (strict mode).
- HTTP 409 — job name already exists. Use `PUT` to update the existing definition.
- HTTP 413 — JAR exceeds `max-file-size` (50MB default).

### 4.2 Approve a job definition

```bash
curl -X PUT -u admin:admin123 \
  -H "Content-Type: application/json" \
  -d '{"approved": true}' \
  http://localhost:8080/api/batch-service/jobs/definitions/{jobDefinitionId}/approve
```

**Expected**: HTTP 200. `approvalStatus` changes to `APPROVED`.

**Note**: In production, `app.plugins.approval.auto-approve=false`, so this step is required before
loading.

### 4.3 Load a plugin

```bash
curl -X POST -u admin:admin123 \
  http://localhost:8080/api/batch-service/jobs/definitions/{jobDefinitionId}/load
```

**Expected**: HTTP 200. Plugin is loaded into the classloader and registered as a Spring Batch job.

**Error handling**:

- HTTP 403 — definition not yet approved.
- HTTP 409 — already loaded.
- HTTP 500 — classloader error (check logs for `DynamicJobClassLoader` stack trace).

### 4.4 Execute a job

```bash
curl -X POST -u admin:admin123 \
  http://localhost:8080/api/batch-service/jobs/{jobName}/start
```

**Expected**: HTTP 202 Accepted. Response includes `executionId` for tracking.

### 4.5 Unload a plugin

```bash
curl -X POST -u admin:admin123 \
  http://localhost:8080/api/batch-service/jobs/definitions/{jobDefinitionId}/unload
```

**Expected**: HTTP 200. Plugin classloader is closed; job is no longer available for execution.

**Error handling**:

- HTTP 409 — job is currently executing. Add `?force=true` to force-unload (may interrupt running
  jobs).

______________________________________________________________________

## 5. Backup & Restore

### 5.1 Backup

```bash
# Database dump
docker compose exec mysql mysqldump -u root -p"${DB_PASSWORD}" frbatch \
  > backup-$(date +%Y%m%d-%H%M%S).sql

# Plugin JARs (rsync from the named volume mount point)
docker compose cp app:/data/plugins/jars ./backup-jars-$(date +%Y%m%d-%H%M%S)
```

### 5.2 Restore

```bash
# 1. Stop the app (MySQL stays up for restore)
docker compose stop app

# 2. Restore database
docker compose exec -T mysql mysql -u root -p"${DB_PASSWORD}" frbatch < backup.sql

# 3. Restore plugin JARs
docker compose cp ./backup-jars/. app:/data/plugins/jars

# 4. Restart app (Flyway validates migrations are applied)
docker compose up -d app

# 5. Verify
curl -s http://localhost:8080/api/batch-service/actuator/health
```

______________________________________________________________________

## 6. Incident Response

### 6.1 Health Check DOWN

**Symptoms**: `/actuator/health` returns `DOWN`. Load balancer removes instance.

**Diagnosis**:

```bash
# Check which component is down
curl -s http://localhost:8080/api/batch-service/actuator/health | jq .

# Check MySQL connectivity (TCP/MySQL protocol — not HTTP)
docker compose exec mysql mysqladmin ping -h localhost -u root -p"${DB_PASSWORD}" || echo "MySQL unreachable"

# Check disk space
docker compose exec app df -h /data/plugins/jars
```

**Resolution**:

1. If MySQL is unreachable → check MySQL logs: `docker compose logs mysql`
1. If disk full → see §6.4
1. If plugin indicator DOWN → see §6.2
1. Restart app: `docker compose restart app`

### 6.2 Plugin Load Failure

**Symptoms**: `POST /load` returns HTTP 500. Plugin appears as UNLOADED.

**Diagnosis**:

```bash
# Check app logs for classloader errors
docker compose logs app | grep -i "classloader\|LoadFailure"

# Verify JAR integrity (path lives inside the app container)
docker compose exec app sha256sum /data/plugins/jars/{jobDefinitionId}.jar
# Compare with checksum in DB:
docker compose exec mysql mysql -u root -p"${DB_PASSWORD}" -e \
  "SELECT checksum_sha256 FROM job_definitions WHERE id = {jobDefinitionId}" frbatch
```

**Resolution**:

1. Corrupted JAR → re-upload: `POST /jobs/upload`
1. Missing dependency in JAR → check `MANIFEST.MF` classpath
1. Signature mismatch → re-sign JAR or temporarily set `app.plugins.signature.mode=permissive`
   during investigation

### 6.3 Execution Error

**Symptoms**: Job fails with `FAILED` status. Exception in logs.

**Diagnosis**:

```bash
# View recent job executions
curl -s -u admin:admin123 \
  http://localhost:8080/api/batch-service/jobs/executions | jq '.[:5]'

# Check logs for the failed execution
docker compose logs app | grep "executionId={id}"
```

**Resolution**:

1. Review job logic in plugin JAR (business error)
1. Force-stop stuck execution: `POST /jobs/executions/{id}/stop`
1. Unload and reload the plugin if it left corrupted state
1. If job requires data fix, run ad-hoc SQL after unload

### 6.4 Disk Full

**Symptoms**: Uploads fail with HTTP 507. Write errors in logs.

**Diagnosis**:

```bash
docker compose exec app df -h /data/plugins/jars
docker compose exec app du -sh /data/plugins/jars/*
```

**Resolution**:

1. Identify stale/orphaned JARs (not referenced in `job_definitions` table)
1. Remove orphaned JARs inside the container (never on the host):
   `docker compose exec app sh -c 'test -f /data/plugins/jars/{orphanId}.jar && rm /data/plugins/jars/{orphanId}.jar'`
1. Unload and delete unused job definitions: `DELETE /jobs/definitions/{id}`
1. Expand Docker volume: `docker volume create --opt size=10G plugin-jars` (requires volume
   recreation)

**Escalation**: If disk usage exceeds 90% despite cleanup, expand the host partition or migrate to a
larger volume.

______________________________________________________________________

## 7. Monitoring

### Prometheus Alert Rules (suggested)

Place these in a `prometheus-alerts.yml` file and mount it alongside `prometheus.yml`:

```yaml
groups:
  - name: fr-batch-service
    rules:
      - alert: PluginLoadFailure
        expr: rate(plugins_load_errors_total[5m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Plugin load failures detected"

      - alert: HighHeapUsage
        expr: jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes{area="heap"} * 100 > 85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "JVM heap usage above 85%"

      - alert: ServiceDown
        expr: up{job="fr-batch-service"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "fr-batch-service is DOWN"

      - alert: HighErrorRate
        expr: rate(http_server_requests_seconds_count{status=~"5.."}[5m]) > 0.1
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "HTTP 5xx error rate elevated"
```

### Grafana Dashboard

Import `fr-batch-service/docs/grafana-dashboard.json` via:

1. Grafana → Dashboards → New → Import
1. Upload JSON file or paste content
1. Select the Prometheus datasource

### Log Locations

- **App logs**: `docker compose logs app` (stdout/stderr from the container)
- **MySQL logs**: `docker compose logs mysql`
- **Prometheus logs**: `docker compose logs prometheus`
- **Grafana logs**: `docker compose logs grafana`

For centralized logging in production, configure a logging driver (`json-file`, `fluentd`, `loki`)
in `docker-compose.yml`.

### Escalation Contacts

| Role                 | Contact                          |
| -------------------- | -------------------------------- |
| Primary on-call      | [TBD — update before deployment] |
| Secondary on-call    | [TBD]                            |
| Platform engineering | [TBD]                            |
