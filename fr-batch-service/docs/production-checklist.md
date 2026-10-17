# Production Readiness Checklist

This checklist must be completed before deploying the `fr-batch-service` plugin system to
production.

______________________________________________________________________

## 🔒 Security

### JAR Signing

- [ ] **Strict signature enforcement enabled**: Set `app.plugins.signature.mode=strict` in
  production configuration.
- [ ] **Signing certificate trusted**: All uploaded JARs must be signed with a certificate from the
  trusted keystore.
- [ ] **Keystore secured**: The platform's truststore/keystore is stored in a secure location (not
  in source control).
- [ ] **Signature verification tested**: Upload a test JAR signed with an untrusted certificate and
  confirm rejection.
- [ ] **Unsigned JAR rejection verified**: Upload an unsigned JAR in strict mode and confirm HTTP
  400 with appropriate error message.

### Authentication & Authorization

- [ ] **BCrypt passwords**: Replace `{noop}` plain-text passwords with `{bcrypt}` hashes. Use
  `PasswordEncoderFactories.createDelegatingPasswordEncoder()` or `BCryptPasswordEncoder` directly.
- [ ] **Strong passwords in use**: Admin and viewer passwords meet complexity requirements (≥12
  chars, mix of upper/lower/digit/special).
- [ ] **HTTPS enabled**: All API traffic goes over TLS. Configure SSL in `application.properties` or
  via a reverse proxy (nginx, Traefik).
- [ ] **Role separation verified**: `PLUGIN_VIEWER` cannot execute upload, load, unload, approve, or
  execute operations.
- [ ] **Audit log tamper-proof**: Audit events are stored in a database table with restricted write
  access.

### Application Security

- [ ] **CSRF disabled confirmation**: CSRF protection is intentionally disabled for the REST API.
  Documented and accepted as a design decision.
- [ ] **Session management stateless**: `SessionCreationPolicy.STATELESS` is configured.
- [ ] **No sensitive data in logs**: Verify that logs do not contain passwords, tokens, or PII from
  uploaded JAR metadata.
- [ ] **File upload limits configured**: `spring.servlet.multipart.max-file-size` is set
  appropriately for expected JAR sizes (e.g., 50MB).

______________________________________________________________________

## 📦 Storage

### JAR Directory

- [ ] **Directory exists and permissions correct**: `fr-batch-service.plugins.jar-dir` points to a
  writable directory (e.g., `/var/lib/fr-batch-service/plugins/jars/`).
- [ ] **Disk space monitoring**: Set up alerting when the JAR storage directory exceeds 80% of the
  partition capacity.
- [ ] **JAR cleanup policy**: Stale or disabled JARs are cleaned up periodically (manual or
  automated).
- [ ] **Backup strategy**: The JAR directory and database are backed up regularly. Include backup
  verification steps.

### Database

- [ ] **Flyway migrations validated**: All migration scripts in `db/migration` have been applied
  successfully.
- [ ] **Connection pool sized for production**: Review HikariCP `maximumPoolSize` and `minimumIdle`
  settings for expected load.
- [ ] **Database backup schedule**: Automated backups (e.g., daily full, hourly incremental) with
  point-in-time recovery capability.
- [ ] **JPA DDL auto disabled**: `spring.jpa.hibernate.ddl_auto=none` ensures no accidental schema
  changes.
- [ ] **Open session in view disabled**: `spring.jpa.open-in-view=false` avoids database connection
  leaks.

______________________________________________________________________

## 📊 Monitoring

### Health Checks

- [ ] **Plugin health endpoint accessible**: `GET /api/batch-service/actuator/health/plugins`
  returns accurate UP/DOWN status.
- [ ] **Health check integrated with load balancer**: The load balancer or orchestrator probes the
  health endpoint and removes unhealthy instances.
- [ ] **Alerting configured for DOWN state**: When the plugin health indicator returns DOWN, an
  alert fires (PagerDuty, Slack, email).

### Prometheus Metrics

- [ ] **Prometheus endpoint exposed**: `management.endpoints.web.exposure.include=prometheus`.
- [ ] **Prometheus scrape job configured**: A scrape config targets the
  `/api/batch-service/actuator/prometheus` path with a reasonable interval (e.g., 30s).
- [ ] **Key metric alerts configured**:

| Metric                            | Alert Condition                   | Severity |
| --------------------------------- | --------------------------------- | -------- |
| `plugins_uploaded_total`          | Sudden spike (DoS indicator)      | Warning  |
| `plugins_loaded_count`            | Drops to 0 (all plugins unloaded) | Critical |
| `plugins_load_duration_seconds`   | Exceeds 30s (classloader issues)  | Warning  |
| `plugins_unload_duration_seconds` | Exceeds 10s (stuck unload)        | Warning  |
| `plugins_executed_total`          | Flat for >1h for key jobs         | Warning  |
| JVM heap usage                    | >85%                              | Warning  |
| JVM GC pause time                 | >1s                               | Warning  |

- [ ] **Grafana dashboard created**: Build a dashboard showing plugin metrics over time, with panels
  for counter rates, gauge values, timer percentiles, and health status.

### Logging

- [ ] **Structured logging configured**: JSON log format for ingestion into centralized logging
  (ELK, Loki, Splunk).
- [ ] **Plugin lifecycle events logged**: Upload, load, unload, execution start/end, and errors are
  logged at INFO/WARN/ERROR levels.
- [ ] **Audit events logged separately**: Audit trail events have distinct log markers for
  compliance filtering.

______________________________________________________________________

## ⚖️ Scaling

### Plugin Limits

- [ ] **Maximum loaded plugins defined**: Establish a limit (e.g., 50 loaded plugins) based on
  memory testing. Monitor `plugins_loaded_count`.
- [ ] **Classloader memory profiling completed**: Each loaded plugin creates a
  `DynamicJobClassLoader`. Profile memory usage under simulated load (e.g., 20 concurrent plugins).
- [ ] **Metaspace monitoring**: Monitor `jvm_memory_used_bytes{area="metaspace"}`; excessive loaded
  classes may exhaust Metaspace.

### Known Limitations

- [ ] **No multi-instance coordination**: Plugin load/unload operations are local to a single JVM
  instance. In a multi-instance deployment:
  - Each instance must independently load its plugins.
  - No distributed consensus on load state — instances may diverge.
  - Load/unload via the load balancer hits only one instance unless sticky sessions are configured.
- [ ] **No horizontal plugin execution**: A single plugin job execution runs on a single instance.
  For distributed processing, use Spring Batch remote partitioning instead.
- [ ] **Plugin JAR size limit**: Very large JARs (>100MB) may cause high upload latency and
  increased Metaspace consumption at load time.

### Instance Sizing

- [ ] **Heap size**: Minimum 512MB per instance plus additional heap per loaded plugin (estimate
  10-20MB per plugin for class metadata).
- [ ] **Metaspace**: Set `-XX:MaxMetaspaceSize=256m` minimum; increase based on the number and size
  of plugin JARs.
- [ ] **Thread pool**: Spring Batch's default `SimpleAsyncTaskExecutor` creates unbounded threads.
  Configure a bounded executor for production.

______________________________________________________________________

## ✅ Approval Workflow

- [ ] **Auto-approve disabled**: Set `app.plugins.approval.auto-approve=false` in production.
- [ ] **Approval policy documented**: Define who can approve job definitions, what checks are
  required (code review, security scan, performance test results).
- [ ] **Approval audit trail verified**: Every approval/rejection is recorded with timestamp,
  approver identity, and job name in the audit log.
- [ ] **Rejected jobs not loadable**: Verify that REJECTED job definitions cannot be loaded or
  executed.
- [ ] **Approval gating before load**: Load operation checks `approvalStatus == APPROVED` before
  proceeding.

______________________________________________________________________

## 🔄 Disaster Recovery

### JAR Recovery

- [ ] **JAR re-upload procedure documented**: If the JAR directory is lost, instructions exist for
  re-uploading JARs from artifact storage or rebuilding from source.
- [ ] **JAR checksum verification**: After re-upload, verify the SHA-256 checksum matches the value
  stored in the database (if DB was not lost).

### Database Recovery

- [ ] **Database backup restoration tested**: A full restore from backup has been practiced and
  documented.
- [ ] **Flyway migration replay verified**: After restore, `spring.flyway.enabled=true` applies any
  pending migrations on startup.
- [ ] **Recovery Time Objective (RTO) defined**: Maximum acceptable downtime for the plugin system
  (e.g., 4 hours).

### Rollback Plan

- [ ] **Plugin rollback procedure**: If a loaded plugin causes issues:
  1. `DELETE /jobs/definitions/{id}?force=true` — unload the problematic plugin and remove the
     definition in one operation (running executions are not stopped; the JAR is deleted after the
     database commit, and an orphan JAR is logged and named in the `JOB_DELETED` audit)
  1. If DELETE returns `409` because the definition has execution history (`force=true` does not
     bypass this), keep the definition and replace its JAR in place instead:
     `PUT /jobs/definitions/{id}/jar` with the previous working JAR (`file`, `version`,
     `mainClassName`). It returns `409` while executions are running; wait for them to finish. The
     definition keeps its ID and execution history, stays unloaded, and its approval is reset to
     `PENDING`.
  1. Approve it with `PUT /jobs/definitions/{id}/approve` and load it with
     `POST /jobs/definitions/{id}/load`. After a successful DELETE, upload the previous working
     version with `POST /jobs/upload` instead.
- [ ] **Rollback tested**: A rollback drill has been completed within the RTO window.

______________________________________________________________________

## 🧪 Pre-Deployment Validation

- [ ] **All unit tests pass**: `mvn test -f fr-batch-service/pom.xml` — 120+ tests, 0 failures.
- [ ] **Integration tests pass**: All `@SpringBootTest` integration tests pass against a test
  database.
- [ ] **Plugin end-to-end test**: Full lifecycle test (upload → approve → load → execute → unload →
  delete) with a real plugin JAR.
- [ ] **Security scan**: Dependency vulnerability scan (`mvn dependency-check:check` or OWASP) with
  no critical/high findings.
- [ ] **Load test**: Simulate concurrent upload (5), load (5), and execution (10) operations. Verify
  no deadlocks, OOM, or excessive latency.

______________________________________________________________________

## 📝 Operational Runbooks

- [ ] **Startup procedure**: Documented steps for starting the service (JVM args, env vars,
  dependency startup order).
- [ ] **Shutdown procedure**: Graceful shutdown with `management.endpoint.shutdown.enabled=true` and
  step drain for running jobs.
- [ ] **Plugin lifecycle runbook**: Step-by-step for uploading, approving, loading, and executing a
  new plugin.
- [ ] **Incident response**: Triage steps for common failures (health DOWN, load failure, execution
  error, signature rejection).
- [ ] **Contact information**: On-call rotation or escalation contacts for plugin system issues.

______________________________________________________________________

## ☑️ Final Sign-off

| Role              | Name | Date | Signature |
| ----------------- | ---- | ---- | --------- |
| Developer         |      |      |           |
| Security Review   |      |      |           |
| Operations Review |      |      |           |
| Product Owner     |      |      |           |

**Date of deployment**: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

**Deployed by**: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_
