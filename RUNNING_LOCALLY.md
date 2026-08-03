# Running the Stack Locally

Everything you need to go from a fresh clone to a fully operational local environment: MySQL running, backend migrated, seeds applied, all four plugins loaded, and the UI showing live data.

> **Toolchain:** Runtime and package-management choices, plus alternatives for
> Java, Node, and Python, are documented in [`TOOLCHAIN.md`](TOOLCHAIN.md).

## Quick Start (Task)

If you have [Task](https://taskfile.dev) installed, the root `Taskfile.yml` wraps every step below.
Each task just delegates to the same scripts and the Maven wrapper, so this is optional sugar — the
manual steps work without Task.

```bash
task doctor     # check required tools
task deps       # one-time: install batch-job-api into the local Maven cache
task up         # start MySQL (Podman); the DB auto-creates on backend boot
task backend    # run the backend (foreground — leave it running)
# then, in another terminal:
task seed       # load seed data
task plugins    # build + upload + approve + enable + load all plugins
task ui         # start the UI
```

Run `task --list` to see every target. The rest of this guide explains each step in full and is the
source of truth if you are not using Task.

### One command for everything (process-compose)

If you have [process-compose](https://github.com/F1bonacc1/process-compose) installed
(`brew install f1bonacc1/tap/process-compose`), `task dev` runs the **whole stack** —
MySQL, backend, seed, plugins and UI — in a single TUI, with start-order, health gating
and per-process logs. No more juggling terminals.

```bash
task deps     # one-time prerequisite (Maven cache)
task dev      # everything, orchestrated; quit with q / Ctrl-C
```

`mysql`, `backend` and `ui` stay running; `seed` and `plugins` run once in order after the
backend is healthy. The config lives in `process-compose.yaml` and mirrors the individual
tasks below.

## Prerequisites

| Tool | Version | Check |
|------|---------|-------|
| Java (JDK) | 21 | `java -version` |
| JAVA_HOME | points to JDK 21 | `echo $JAVA_HOME` |
| Podman | 4.0+ | `podman --version` |
| podman-compose | 1.0+ | `podman-compose --version` |
| Node.js | 22 LTS | `node -version` |
| npm | bundled with Node 22 | `npm -version` |
| Task (optional) | 3.x | `task --version` |

> **Easiest path — [mise](https://mise.jdx.dev):** run `mise install` at the repo root, then activate mise
> in your shell with `eval "$(mise activate zsh)"` (see `.mise.toml`). This selects the pinned Java 21,
> Node 22, and Python 3.11; `mise install` alone only downloads them. No manual sdkman/fnm/pyenv setup
> needed. See [`TOOLCHAIN.md`](TOOLCHAIN.md) for alternatives and package installation guidance.
>
> **No Maven install needed** — every build uses the bundled wrapper `./fr-batch-service/mvnw`.
> **No `mysql` client needed** — the seed script execs into the running DB container.
> `python3` (built into macOS) covers the JSON parsing the scripts do; `jq` is used if present.
>
> Run `task doctor` to check these tools at any time.

---

## One-Time: GitHub Packages Authentication

`batch-job-api` is published to GitHub Packages. Every plugin and `fr-batch-service` declare it as a Maven dependency. Without auth, Maven exits with HTTP 401.

### Option A — Local install (simplest for the repo owner)

Build and install `batch-job-api` into your local Maven cache. No GitHub PAT needed.

```bash
./fr-batch-service/mvnw -f batch-job-api/pom.xml clean install
```

> Or run `task deps`, which does exactly this.

### Option B — GitHub PAT in settings.xml

1. Create a [Personal Access Token](https://github.com/settings/tokens) with the **`read:packages`** scope.
2. Add a `<server>` entry to `~/.m2/settings.xml`:

```xml
<settings>
  <servers>
    <server>
      <id>github</id>
      <username>YOUR_GITHUB_USERNAME</username>
      <password>YOUR_PAT_HERE</password>
    </server>
  </servers>
</settings>
```

> Option A is the faster path if you own the repository. Option B is needed for contributors who cannot publish to the registry.

---

## Step 1 — Start MySQL

This guide runs the database with [Podman](https://podman.io/) (rootless, daemonless) and
[`podman-compose`](https://github.com/containers/podman-compose). The database container lives in
`fr-batch-service/_devenvironment/`.

On macOS, start the Podman VM once per boot before any container command:

```bash
podman machine start
```

Then bring up MySQL:

```bash
cd fr-batch-service/_devenvironment
podman-compose up -d
cd -
```

> **Why `podman-compose` and not `podman compose`?** The native `podman compose` wrapper delegates to
> whatever external provider it finds — often Docker's `docker-compose` plugin, which on a machine that
> once ran Docker Desktop tries to read credentials via `docker-credential-osxkeychain` and fails. The
> standalone `podman-compose` tool talks to Podman directly and avoids that.

The compose file reads `fr-batch-service/.env` (via `env_file: ../.env`) and declares an explicit project
name (`frbatch-local`) so Podman accepts the generated volume name — Podman rejects volume names that
don't start with an alphanumeric, and the `_devenvironment` directory name would otherwise leak a leading
underscore into it.

That `.env` configures `MYSQL_DATABASE=frbatchservicedb2` and a non-root user. The app's **local profile**
connects to a different database (`fr_batch_local`) as `root` with an empty password. You do **not** need to
create it by hand — the local JDBC URL carries `createDatabaseIfNotExist=true`, so the backend creates
`fr_batch_local` on its first connection (Step 2). The container's server default charset is `utf8mb4`.

> **Why the `.env` mismatch?** `fr-batch-service/.env` is git-tracked and sets `DB_NAME=frbatchservicedb2`,
> while the local Spring profile (`application-local.properties`) targets `fr_batch_local`. They serve
> different purposes and the `.env` value pre-dates the local profile — leave it as is.

Verify MySQL is up:

```bash
podman-compose exec db mysqladmin ping -u root
```

---

## Step 2 — Start the Backend

```bash
cd fr-batch-service
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

> Uses the bundled Maven wrapper — no system Maven needed. The first run downloads the pinned Maven version.

**The `local` profile flag is required.** Without it, the app tries to read `${DB_HOST}`, `${DB_PORT}`, `${DB_NAME}`, `${DB_USERNAME}`, and `${DB_PASSWORD}` from the environment and fails at startup.

What happens on first run with the `local` profile:

- `FlywayMigrationRunner` (active only in the `local` profile) runs migrations V1–V8, creating all tables.
- `spring.flyway.enabled=false` in `application-local.properties` keeps Spring's built-in Flyway autoconfigure out of the way.
- Uploaded plugins must be **approved** before they can load (see Step 6). Under the `local` profile `AutoApproveConfig` auto-approves each upload (toggle with `app.plugins.approval.auto-approve`); the helper script also approves explicitly, so it works either way.
- NoOp password encoder is active (plain-text credentials work).

Wait until you see a line similar to:
```
Started FrBatchServiceApplication in X.XXX seconds
```

Readiness check — the public plugin endpoint responds as soon as the API is serving:

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/api/batch-service/jobs/plugins
```

Expected: `200`.

> **Note on `/actuator/health`:** it reports `DOWN` (HTTP 503) until at least one uploaded plugin is loaded
> — the `plugins` health group is unhealthy with zero loaded plugins. That is expected on a fresh start; it
> flips to `UP` after Step 6. Use `/jobs/plugins` (above) as the "is the backend serving?" signal.

---

## Step 3 — Apply Seeds

The seed files live in `fr-batch-service/_devenvironment/db/`. Apply them **after** the backend has started (Flyway must have created the tables first).

Run the helper script from the **repository root**:

```bash
bash scripts/seed-local-db.sh
```

**No host `mysql` client required.** The script auto-detects how to reach the database: it uses a host
`mysql` client if one is in your `PATH`, otherwise it execs into the running Podman DB container (which
already ships a client). Force a mode with `DB_CLIENT=host|podman` if needed:

```bash
# Force the container client (default when no host mysql is installed)
DB_CLIENT=podman bash scripts/seed-local-db.sh

# Force a host mysql client over TCP
DB_CLIENT=host DB_NAME=fr_batch_local DB_USER=root MYSQL_HOST=127.0.0.1 bash scripts/seed-local-db.sh
```

Seed files applied in order:

| File | Requires | Inserts |
|------|----------|---------|
| `10_seed_event_tickets.sql` | V5 (event_tickets, generated_files) | sample event ticket rows |
| `20_seed_harvest_source.sql` | V7 (harvest_source, harvest_dead_letter) | harvest source configs |
| `30_seed_usage_record.sql` | V8 (usage_record, billing_charge) | usage and billing samples |

> Do **not** run `00_create_schema.sql` — that file is superseded by Flyway and will conflict with existing tables.

---

## Step 4 — Plugin Storage (automatic — no action needed)

The backend stores uploaded JARs under `${user.dir}/../plugin-jars/` — i.e.
`plugin-jars/` at the repository root when started from `fr-batch-service` — and **creates that directory
on first upload**. There is no manual `mkdir` step and no machine-specific path baked into config.

> Want a different location? Override `fr-batch-service.plugins.jar-dir` in `application-local.properties`,
> or pass `-Dspring-boot.run.arguments="--fr-batch-service.plugins.jar-dir=/your/path/"` to the backend.

---

## Step 5 — Build the Plugin JARs

Each plugin is a separate Maven module. Build all four fat JARs with the wrapper (no system Maven needed):

```bash
./fr-batch-service/mvnw -B package -f ticket-pdf-job/pom.xml -DskipTests
./fr-batch-service/mvnw -B package -f ticket-bundle-job/pom.xml -DskipTests
./fr-batch-service/mvnw -B package -f fault-tolerant-harvester-job/pom.xml -DskipTests
./fr-batch-service/mvnw -B package -f partitioned-harvester-job/pom.xml -DskipTests
```

> Or just run `task plugins`, which builds and loads everything via the helper script.

Expected output jars:

| Plugin | JAR |
|--------|-----|
| ticket-pdf-job | `ticket-pdf-job/target/ticket-pdf-job-1.0.0.jar` |
| ticket-bundle-job | `ticket-bundle-job/target/ticket-bundle-job-1.0.0.jar` |
| fault-tolerant-harvester-job | `fault-tolerant-harvester-job/target/fault-tolerant-harvester-job-1.0.0.jar` |
| partitioned-harvester-job | `partitioned-harvester-job/target/partitioned-harvester-job-1.0.0.jar` |

> If you skipped Option A for GitHub Packages auth, Maven will 401 here. Go back and run `./fr-batch-service/mvnw -f batch-job-api/pom.xml clean install` (or `task deps`) first.

---

## Step 6 — Upload, Enable, and Load Each Plugin

Plugin loading is **dynamic upload, not classpath**. At startup, zero plugins are active. You register them through the REST API:

1. **Upload** — POST the JAR to the service (returns a definition `id`)
2. **Approve** — PUT to approve the definition (the `/load` endpoint rejects `PENDING` definitions)
3. **Enable** — PUT to mark the definition active
4. **Load** — POST to instantiate the plugin and register it as a Spring Batch job

> Non-production profiles auto-approve uploads (`app.plugins.approval.auto-approve`, on by default), so a
> definition is usually already `APPROVED`. The helper script still calls approve explicitly — a second
> approve just returns `409 Already approved`, which it treats as success — so it works whether auto-approve
> is on or off.

Use the helper script (it handles all four plugins and all four steps):

```bash
bash scripts/load-plugins.sh
```

To also build the JARs before loading:

```bash
bash scripts/load-plugins.sh --build
```

### Manual reference

For a single plugin, the three calls look like:

```bash
BASE=http://localhost:8080/api/batch-service

# 1. Upload
RESPONSE=$(curl -s -u admin:admin123 -X POST "$BASE/jobs/upload" \
  -F "file=@ticket-pdf-job/target/ticket-pdf-job-1.0.0.jar" \
  -F "jobName=ticket-pdf-job" \
  -F "version=1.0.0" \
  -F "mainClassName=com.fronzec.plugins.ticketpdf.TicketPdfJobPlugin")

ID=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

# 2. Approve (required — /load rejects PENDING definitions)
curl -s -u admin:admin123 -X PUT "$BASE/jobs/definitions/$ID/approve" \
  -H 'Content-Type: application/json' -d '{"approvedBy":"admin"}'

# 3. Enable
curl -s -u admin:admin123 -X PUT "$BASE/jobs/definitions/$ID/enable"

# 4. Load
curl -s -u admin:admin123 -X POST "$BASE/jobs/definitions/$ID/load"
```

### Declarative reference (hurl)

The same four steps as a declarative HTTP file — [`scripts/load-plugins.hurl`](scripts/load-plugins.hurl).
It replaces the `curl` + JSON-parsing chain above with five readable requests and native
assertions; the definition `id` is captured from the definitions list by `job_name`, so there is
nothing to parse by hand.

```bash
# one-time: install hurl (https://hurl.dev) — e.g. brew install hurl
hurl --test --file-root . \
  --variable base=http://localhost:8080/api/batch-service \
  --variable user=admin --variable pass=admin123 \
  --variable jobName=ticket-pdf-job \
  --variable version=1.0.0 \
  --variable mainClass=com.fronzec.plugins.ticketpdf.TicketPdfJobPlugin \
  --variable jar=ticket-pdf-job/target/ticket-pdf-job-1.0.0.jar \
  scripts/load-plugins.hurl
```

> `--file-root .` is required: hurl sandboxes file reads to the `.hurl` file's directory, and the
> JAR lives outside `scripts/`. Swap the `--variable` values (see the coordinates table below) to
> load a different plugin.

The flow is idempotent — re-running returns `409`s and still passes. CI runs this file twice
(fresh load + idempotency) for `ticket-pdf-job` as a contract test on every PR; see the
`contract-test` job in `.github/workflows/maven.yml`. The coordinates table below covers all four
plugins for manual runs.

### Plugin coordinates reference

| jobName | JAR path | mainClassName |
|---------|----------|---------------|
| `ticket-pdf-job` | `ticket-pdf-job/target/ticket-pdf-job-1.0.0.jar` | `com.fronzec.plugins.ticketpdf.TicketPdfJobPlugin` |
| `ticket-bundle-job` | `ticket-bundle-job/target/ticket-bundle-job-1.0.0.jar` | `com.fronzec.plugins.ticketbundle.TicketBundleJobPlugin` |
| `fault-tolerant-harvester-job` | `fault-tolerant-harvester-job/target/fault-tolerant-harvester-job-1.0.0.jar` | `com.fronzec.plugins.harvester.FaultTolerantHarvesterJobPlugin` |
| `partitioned-harvester-job` | `partitioned-harvester-job/target/partitioned-harvester-job-1.0.0.jar` | `com.fronzec.plugins.partitionedharvester.PartitionedHarvesterJobPlugin` |

> **Security note:** `POST /jobs/upload`, `PUT /jobs/definitions/{id}/approve`, `PUT /jobs/definitions/{id}/enable`, and `POST /jobs/definitions/{id}/load` all require the `ADMIN` role. Locally, use `admin:admin123`.

---

## Step 7 — Start the UI

```bash
cd batch-ops-ui
npm install
npm run dev
```

Open http://localhost:5173. Log in with `viewer/viewer123`.

The Vite dev server proxies `/api` requests to `:8080`, so the backend must be running first.

---

## Verify It Worked

```bash
# List registered plugins (public endpoint — no auth needed)
curl -s http://localhost:8080/api/batch-service/jobs/plugins | python3 -m json.tool

# List job definitions (requires VIEWER role)
curl -s -u viewer:viewer123 http://localhost:8080/api/batch-service/jobs/definitions | python3 -m json.tool
```

Expected: four uploaded entries in `/jobs/plugins`, each showing `status: ACTIVE`.

Open the dashboard at http://localhost:5173 — you should see all four jobs listed and triggerable.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `Could not resolve placeholder 'DB_HOST'` at startup | Missing `local` profile flag | Add `-Dspring-boot.run.profiles=local` |
| `Unknown database 'fr_batch_local'` | JDBC URL lost `createDatabaseIfNotExist=true` | Restore the param in `application-local.properties`, or create the DB once: `podman-compose exec db mysql -u root -e "CREATE DATABASE IF NOT EXISTS fr_batch_local;"` |
| `mvn package` exits with HTTP 401 | GitHub Packages not authenticated | Run `./fr-batch-service/mvnw -f batch-job-api/pom.xml clean install` (Option A / `task deps`) |
| UI shows "Backend unreachable" | Backend not running on :8080 | Start backend first: Step 2 |
| `POST /jobs/upload` returns 401 | Wrong credentials | Use `admin:admin123` for write operations |
| Flyway fails with `Table already exists` | `00_create_schema.sql` was run manually | Drop all tables and let Flyway recreate, or restore a clean volume |

---

## Alternative: Full-Stack Docker Compose

A root-level `docker-compose.yml` runs MySQL, the Spring Boot app (built from the Dockerfile), Prometheus, and Grafana together. It targets the `docker` profile, not `local`.

The app image builds from source and pulls the private `batch-job-api` from GitHub Packages, so the build needs a token. Export `GITHUB_TOKEN` (a PAT with `read:packages`) — it is passed to the build as a BuildKit secret and never lands in an image layer:

```bash
export GITHUB_TOKEN=<your-pat-with-read:packages>
DB_PASSWORD=yourpassword docker compose up -d
```

If your GitHub username matters for package auth, also `export GITHUB_ACTOR=<your-username>` (defaults to `x-access-token`).

This is useful for a production-like smoke test, but not the recommended path for day-to-day development because it rebuilds the app image on every change.
