# Job Plugins Developer Guide

This guide covers the complete lifecycle for developing, packaging, uploading, approving, loading,
and executing external Spring Batch job plugins for the `fr-batch-service` platform.

______________________________________________________________________

## Plugin Lifecycle Overview

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  DEVELOP │───▶│  UPLOAD  │───▶│ APPROVE  │───▶│   LOAD   │───▶│ EXECUTE  │
│  & BUILD │    │   (JAR)  │    │ (review) │    │(classpath│    │  (sync/  │
│          │    │          │    │          │    │ & registry)    │  async)  │
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
                                                       │
                                                       ▼
                                                ┌──────────┐    ┌──────────┐
                                                │  AUDIT   │◀───│ UNLOAD   │
                                                │   LOG    │    │(cleanup) │
                                                └──────────┘    └──────────┘
```

1. **Develop & Build** — Create a Maven project that implements `BatchJobPlugin`
1. **Upload** — `POST /jobs/upload` sends the shaded JAR to the platform
1. **Approve** — `PUT /jobs/definitions/{id}/approve` marks the job ready for loading
1. **Load** — `POST /jobs/definitions/{id}/load` instantiates the plugin and registers it
1. **Execute** — `POST /jobs/start-single/sync` (or `/async`) runs the job with parameters
1. **Unload** — `POST /jobs/definitions/{id}/unload` unregisters and closes the classloader
1. **Audit** — All lifecycle events are logged to the audit trail

______________________________________________________________________

## BatchJobPlugin Contract

Every plugin implements `com.fronzec.api.BatchJobPlugin`:

```java
public interface BatchJobPlugin {
  String getJobName();                         // unique identifier
  String getVersion();                         // semver
  Job configureJob(JobRepository,              // Spring Batch Job definition
      PlatformTransactionManager,
      ApplicationContext parentContext);
  Map<String, String> getDefaultParameters();  // fallback params
  List<String> getRequiredDependencies();      // required platform beans
  JobMetadata getMetadata();                   // display name, author, tags, etc.
}
```

### JobMetadata

```java
public interface JobMetadata {
  String getDisplayName();       // human-readable name
  String getDescription();       // purpose description
  String getAuthor();            // team or individual
  List<String> getTags();        // searchable labels
  Duration getEstimatedRuntime();// expected run time
}
```

______________________________________________________________________

## Plugin Project Structure (Maven)

```
example-payment-job/
├── pom.xml                                      # Build config (shade, provided-scope)
└── src/main/
    ├── java/com/example/payment/
    │   └── PaymentJobPlugin.java                # BatchJobPlugin implementation
    └── resources/
        ├── data/
        │   └── sample-payments.csv              # Bundled input data
        └── META-INF/services/
            └── com.fronzec.api.BatchJobPlugin   # SPI registration
```

### pom.xml — Key Concepts

- **`provided` scope**: `batch-job-api`, `spring-batch-core`, `spring-context`, and `spring-tx` must
  be `provided` — the platform supplies them at runtime.
- **`maven-shade-plugin`**: Bundles job-specific dependencies (e.g., CSV parsers, HTTP clients)
  while excluding platform libraries to prevent classpath conflicts.
- **Java 21**: The platform runs on Java 21; plugins must target the same release.
- **No Spring Boot parent**: Plugins are standalone Maven projects; they don't inherit from Spring
  Boot's parent POM.

```xml
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example</groupId>
  <artifactId>example-payment-job</artifactId>
  <version>1.0.0</version>

  <properties>
    <maven.compiler.release>21</maven.compiler.release>
  </properties>

  <dependencies>
    <dependency>
      <groupId>com.fronzec</groupId>
      <artifactId>batch-job-api</artifactId>
      <version>0.0.1-SNAPSHOT</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.springframework.batch</groupId>
      <artifactId>spring-batch-core</artifactId>
      <version>6.0.0</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.springframework</groupId>
      <artifactId>spring-context</artifactId>
      <version>7.0.1</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.springframework</groupId>
      <artifactId>spring-tx</artifactId>
      <version>7.0.1</version>
      <scope>provided</scope>
    </dependency>
    <!-- Job-specific deps (e.g., payment SDK) go here WITHOUT provided -->
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.6.0</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <createDependencyReducedPom>false</createDependencyReducedPom>
              <artifactSet>
                <excludes>
                  <exclude>com.fronzec:batch-job-api</exclude>
                  <exclude>org.springframework.batch:spring-batch-core</exclude>
                  <exclude>org.springframework:spring-context</exclude>
                  <exclude>org.springframework:spring-tx</exclude>
                </excludes>
              </artifactSet>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

### SPI Registration

Create the file `src/main/resources/META-INF/services/com.fronzec.api.BatchJobPlugin` containing the
fully-qualified class name of your plugin:

```
com.example.payment.PaymentJobPlugin
```

This is how the platform discovers your plugin via `ServiceLoader` at load time.

______________________________________________________________________

## REST API Reference

All endpoints are prefixed with `/api/batch-service`. Authentication uses HTTP Basic (see Security
Roles below).

### Job Definition Management

| Method   | Path                                  | Auth             | Description                                                             |
| -------- | ------------------------------------- | ---------------- | ----------------------------------------------------------------------- |
| `POST`   | `/jobs/upload`                        | `PLUGIN_ADMIN`   | Upload a JAR and register a job definition                              |
| `GET`    | `/jobs/definitions`                   | `PLUGIN_VIEWER`+ | List all job definitions                                                |
| `GET`    | `/jobs/definitions/{id}`              | `PLUGIN_VIEWER`+ | Get a single definition                                                 |
| `PUT`    | `/jobs/definitions/{id}/enable`       | `PLUGIN_ADMIN`   | Enable a definition                                                     |
| `PUT`    | `/jobs/definitions/{id}/disable`      | `PLUGIN_ADMIN`   | Disable a definition                                                    |
| `DELETE` | `/jobs/definitions/{id}[?force=true]` | `PLUGIN_ADMIN`   | Unload, delete definition, then delete JAR file (see [Delete](#delete)) |
| `GET`    | `/jobs/plugins`                       | Public           | List merged plugin metadata                                             |

### Upload

```bash
curl -u admin:admin123 \
  -F "file=@target/example-payment-job-1.0.0.jar" \
  -F "jobName=payment-job" \
  -F "version=1.0.0" \
  -F "mainClassName=com.example.payment.PaymentJobPlugin" \
  http://localhost:8080/api/batch-service/jobs/upload
```

**Response** (`201 Created`):

```json
{
  "id": 1,
  "job_name": "payment-job",
  "version": "1.0.0",
  "main_class_name": "com.example.payment.PaymentJobPlugin",
  "jar_file_path": "./plugins/jars/payment-job-1.0.0.jar",
  "sha256_checksum": "abc123...",
  "uploaded_at": "2024-03-15T10:30:00",
  "enabled": false,
  "approval_status": "PENDING",
  "load_status": "UNLOADED"
}
```

### Approval Workflow

By default, `app.plugins.approval.auto-approve=true` in development. In production, set it to
`false` and use explicit approval:

| Method | Path                             | Auth           | Description              |
| ------ | -------------------------------- | -------------- | ------------------------ |
| `PUT`  | `/jobs/definitions/{id}/approve` | `PLUGIN_ADMIN` | Approve a job definition |
| `PUT`  | `/jobs/definitions/{id}/reject`  | `PLUGIN_ADMIN` | Reject a job definition  |

```bash
curl -u admin:admin123 \
  -X PUT \
  -H "Content-Type: application/json" \
  -d '{"approved_by": "admin"}' \
  http://localhost:8080/api/batch-service/jobs/definitions/1/approve
```

**Approval states**: `PENDING` → `APPROVED` | `REJECTED`

### Load and Unload

| Method | Path                            | Auth           | Description                          |
| ------ | ------------------------------- | -------------- | ------------------------------------ |
| `POST` | `/jobs/definitions/{id}/load`   | `PLUGIN_ADMIN` | Load plugin from JAR into registry   |
| `POST` | `/jobs/definitions/{id}/unload` | `PLUGIN_ADMIN` | Unregister plugin, close classloader |
| `POST` | `/jobs/definitions/{id}/reload` | `PLUGIN_ADMIN` | Atomic unload + load                 |
| `POST` | `/jobs/load-all`                | `PLUGIN_ADMIN` | Load all enabled definitions         |

```bash
# Load
curl -u admin:admin123 -X POST \
  http://localhost:8080/api/batch-service/jobs/definitions/1/load

# Unload (with optional force flag)
curl -u admin:admin123 -X POST \
  "http://localhost:8080/api/batch-service/jobs/definitions/1/unload?force=true"

# Reload
curl -u admin:admin123 -X POST \
  http://localhost:8080/api/batch-service/jobs/definitions/1/reload

# Load all enabled
curl -u admin:admin123 -X POST \
  http://localhost:8080/api/batch-service/jobs/load-all
```

### Delete

`DELETE /jobs/definitions/{id}` is a single operation that runs under the same per-definition lock
as load, unload, and reload, so it cannot race with them for the same ID:

1. A definition with execution history (rows in `job_executions_audit`) is rejected before anything
   is unloaded. Disable it instead (`PUT /jobs/definitions/{id}/disable`).
1. The plugin is unloaded automatically (unregistered and its classloader released). A definition
   that is not loaded is deleted without error.
1. The parameter templates and the definition row are deleted in one database transaction.
1. The JAR file is deleted only after that transaction commits.

| Outcome                                                 | Response                                                               |
| ------------------------------------------------------- | ---------------------------------------------------------------------- |
| Deleted                                                 | `204 No Content`                                                       |
| Unknown ID                                              | `404 Not Found`                                                        |
| Execution history exists (with or without `force=true`) | `409 Conflict`; nothing is unloaded or deleted; disable the definition |
| Running executions and `force=false` (default)          | `409 Conflict`; nothing is deleted                                     |
| Database delete fails                                   | Error response; the transaction is rolled back and the JAR is kept     |

`force=true` skips only the running-execution guard; it does not stop running executions and does
not bypass the execution-history guard. A missing JAR file does not fail the request. If the JAR
cannot be removed after commit, the request still returns `204`, a `WARN` is logged, and the
`JOB_DELETED` success audit names the orphan JAR path. `JOB_DELETED` is audited only once the
outcome is known: `SUCCESS` after completion, `FAILURE` (with the definition ID and reason) when the
delete is rejected or fails.

```bash
# Delete (fails with 409 while executions are running)
curl -u admin:admin123 -X DELETE \
  http://localhost:8080/api/batch-service/jobs/definitions/1

# Delete even if executions are running (they are not stopped)
curl -u admin:admin123 -X DELETE \
  "http://localhost:8080/api/batch-service/jobs/definitions/1?force=true"
```

### Job Execution

| Method | Path                       | Auth             | Description                        |
| ------ | -------------------------- | ---------------- | ---------------------------------- |
| `POST` | `/jobs/start-single/sync`  | `PLUGIN_ADMIN`   | Run a single job synchronously     |
| `POST` | `/jobs/start-single/async` | `PLUGIN_ADMIN`   | Run a single job asynchronously    |
| `POST` | `/jobs/start-all/sync`     | `PLUGIN_ADMIN`   | Run all loaded jobs synchronously  |
| `POST` | `/jobs/start-all/async`    | `PLUGIN_ADMIN`   | Run all loaded jobs asynchronously |
| `POST` | `/jobs/stop-all`           | `PLUGIN_ADMIN`   | Stop all running jobs              |
| `GET`  | `/jobs/running-all`        | `PLUGIN_VIEWER`+ | List running job executions        |

**Execute a single job synchronously**:

```bash
curl -u admin:admin123 \
  -X POST \
  -H "Content-Type: application/json" \
  -H "X-User: admin" \
  -d '{
    "job_bean_name": "payment-job",
    "date": "2024-03-15",
    "execution_attempt_number": "1",
    "description": "Manual test run"
  }' \
  http://localhost:8080/api/batch-service/jobs/start-single/sync
```

**Request body fields** (snake_case due to Jackson naming strategy):

| Field                      | Required | Description                      |
| -------------------------- | -------- | -------------------------------- |
| `job_bean_name`            | Yes      | The job name from `getJobName()` |
| `date`                     | Yes      | ISO-8601 date string             |
| `execution_attempt_number` | Yes      | Unique attempt number            |
| `description`              | No       | Non-identifying descriptor       |

**Execution audit history**: every launch path above (single or all, sync or async) writes exactly
one `job_executions_audit` row per job execution. The only writer is `JobExecutionAuditListener`,
which the platform attaches to every plugin job at registration time (alongside any listeners the
plugin attached itself). The row records the start time when the job starts, then the finish time,
duration in milliseconds, and the outcome, which is the execution's Spring Batch `BatchStatus` name
(`COMPLETED`, `FAILED`, `STOPPED`, ...). Jobs without a `job_definitions` row (for example,
classpath-only jobs) are not audited. Audit write failures are logged and never fail the job. A
plugin whose `configureJob(...)` returns a `Job` that is not a Spring Batch `AbstractJob` still
registers, but its executions are not audited and a `WARN` is logged.

______________________________________________________________________

## JAR Signing Configuration

The platform verifies JAR signatures before loading. Configure via `application.properties`:

```properties
# Signature verification mode: permissive (default) or strict
app.plugins.signature.mode=strict

# In strict mode, unsigned or invalid JARs are REJECTED at upload time.
# In permissive mode, warnings are logged but upload proceeds.
```

**JAR signing with jarsigner** (for production):

```bash
jarsigner -keystore my-keystore.jks -storepass secret \
  -keypass secret target/example-payment-job-1.0.0.jar my-key-alias
```

______________________________________________________________________

## Approval Workflow

The approval system prevents unverified plugins from being loaded into the platform.

### Modes

| Property                            | Value          | Behavior                     |
| ----------------------------------- | -------------- | ---------------------------- |
| `app.plugins.approval.auto-approve` | `true` (dev)   | Jobs auto-approved on upload |
| `app.plugins.approval.auto-approve` | `false` (prod) | Jobs require manual approval |

### Approval States

```
PENDING ──▶ APPROVED ──▶ LOADED ──▶ EXECUTED
                │
                ▼
            REJECTED (terminal)
```

### Production Approval Flow

1. Developer uploads JAR → `PENDING`
1. Approver reviews metadata and signature → `APPROVED` or `REJECTED`
1. If approved, operator loads the job → `LOADED`
1. Job is available for execution

______________________________________________________________________

## Security Roles

The platform uses HTTP Basic authentication with two roles:

| Role            | Username | Password (dev) | Permissions                                        |
| --------------- | -------- | -------------- | -------------------------------------------------- |
| `PLUGIN_ADMIN`  | `admin`  | `admin123`     | Full CRUD, load, unload, execute, approve          |
| `PLUGIN_VIEWER` | `viewer` | `viewer123`    | Read-only: list definitions, plugins, running jobs |

### Endpoint Access Matrix

| Endpoint Pattern       | Public | PLUGIN_VIEWER | PLUGIN_ADMIN |
| ---------------------- | ------ | ------------- | ------------ |
| `GET /actuator/health` | ✅     | ✅            | ✅           |
| `GET /jobs/plugins`    | ✅     | ✅            | ✅           |
| `GET /jobs/**`         | ❌     | ✅            | ✅           |
| `POST /jobs/**`        | ❌     | ❌            | ✅           |
| `PUT /jobs/**`         | ❌     | ❌            | ✅           |
| `DELETE /jobs/**`      | ❌     | ❌            | ✅           |

### Production Password Configuration

Replace plain-text passwords with BCrypt hashes before production:

```properties
app.security.admin-password={bcrypt}$2a$10$...
app.security.viewer-password={bcrypt}$2a$10$...
```

And update `SecurityConfig` to use `BCryptPasswordEncoder` instead of `NoOpPasswordEncoder`.

______________________________________________________________________

## Health and Metrics Endpoints

### Health Check

The platform exposes a dedicated plugin health endpoint:

```
GET /api/batch-service/actuator/health/plugins
```

**Response** (UP):

```json
{
  "status": "UP",
  "details": {
    "loaded": 3,
    "failed": 0,
    "jarDirAccessible": true
  }
}
```

**Response** (DOWN):

```json
{
  "status": "DOWN",
  "details": {
    "loaded": 2,
    "failed": 1,
    "jarDirAccessible": true
  }
}
```

The health indicator reports DOWN when:

- One or more job definitions have `loadStatus = FAILED`
- The JAR storage directory is not readable

### Prometheus Metrics

Metrics are exposed at:

```
GET /api/batch-service/actuator/prometheus
```

| Metric                                   | Type    | Description                    |
| ---------------------------------------- | ------- | ------------------------------ |
| `plugins_uploaded_total`                 | Counter | Total JARs uploaded            |
| `plugins_loaded_total`                   | Counter | Total plugin load operations   |
| `plugins_unloaded_total`                 | Counter | Total plugin unload operations |
| `plugins_executed_total{job_name="..."}` | Counter | Executions per job name        |
| `plugins_loaded_count`                   | Gauge   | Currently loaded plugins       |
| `plugins_load_duration_seconds`          | Timer   | Load operation duration        |
| `plugins_unload_duration_seconds`        | Timer   | Unload operation duration      |

### Prometheus Scrape Config

```yaml
scrape_configs:
  - job_name: 'fr-batch-service'
    metrics_path: '/api/batch-service/actuator/prometheus'
    static_configs:
      - targets: ['localhost:8080']
```

______________________________________________________________________

## Troubleshooting

### Q: Upload fails with "Invalid JAR: not a valid ZIP file"

**Cause**: The file is not a valid JAR (ZIP) or is corrupted.

**Fix**: Verify the file starts with ZIP magic bytes (`PK\x03\x04`). Run `mvn clean package` and
re-upload. Check the file extension is `.jar`.

### Q: Load fails with "JobLoadException: SPI file not found"

**Cause**: The `META-INF/services/com.fronzec.api.BatchJobPlugin` file is missing from the JAR or
contains an incorrect class name.

**Fix**:

```bash
# Check SPI file exists in the JAR
jar tf target/example-payment-job-1.0.0.jar | grep META-INF/services
# Verify class name
jar xf target/example-payment-job-1.0.0.jar META-INF/services/com.fronzec.api.BatchJobPlugin
cat META-INF/services/com.fronzec.api.BatchJobPlugin
```

### Q: Load fails with "NoClassDefFoundError" for platform classes

**Cause**: Platform dependencies (Spring Batch, Spring Framework) were shaded into the plugin JAR,
causing classloader conflicts.

**Fix**: Ensure `spring-batch-core`, `spring-context`, and `spring-tx` are excluded in the shade
plugin configuration and declared as `provided` scope. Verify with:

```bash
jar tf target/example-payment-job-1.0.0.jar | grep "spring"
# Should return empty or only job-specific Spring classes
```

### Q: Execute fails with "Job not found" for plugin jobs

**Cause**: The plugin was not loaded before execution, or was loaded under a different name.

**Fix**: Verify the job name in `getJobName()` matches the `job_bean_name` in the request. Check
load status via `GET /jobs/definitions/{id}` — it should show `loadStatus: LOADED`.

### Q: Unload fails with "JobUnloadConflictException"

**Cause**: The job has active running executions and `force=false` was used.

**Fix**: Either wait for executions to complete, use `force=true`, or stop the running executions
first via `POST /jobs/stop-all`.

### Q: Health endpoint shows DOWN

**Cause**: A job definition has `loadStatus = FAILED` or the JAR directory is not accessible.

**Fix**: Check `GET /jobs/definitions` for entries with `loadStatus: FAILED`. Investigate the
application logs for the failure reason. Verify the JAR directory exists and is readable:
`ls -la ./plugins/jars/`.

### Q: Metrics not appearing in Prometheus

**Cause**: The Prometheus endpoint may not be exposed or the scrape path is incorrect.

**Fix**: Verify `management.endpoints.web.exposure.include=health,info,env,prometheus` in
`application.properties`. Ensure the Prometheus scrape config uses the correct `metrics_path` with
the context path prefix (`/api/batch-service/actuator/prometheus`).
