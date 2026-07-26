# Repository Guide for AI Agents

## Scope

Spring Batch Plugin Lab is a monorepo containing a shared Java API, a Spring Boot plugin host,
independent batch-job plugin JARs, and a Svelte operations UI. Make focused changes in the owning
module; do not introduce cross-module coupling outside `batch-job-api` unless the plugin contract
requires it.

## Repository Layout

- `batch-job-api/` — shared plugin contract; host and plugins depend on it.
- `fr-batch-service/` — Spring Boot 4 / Spring Batch 6 host, REST API, Flyway migrations, and
  dynamic JAR loader.
- `ticket-pdf-job/`, `ticket-bundle-job/`, `fault-tolerant-harvester-job/`,
  `partitioned-harvester-job/` — independently built plugin modules.
- `batch-ops-ui/` — Svelte + Vite dashboard.
- `scripts/`, `Taskfile.yml`, and `RUNNING_LOCALLY.md` — local-development automation and runbook.

## Toolchains and Commands

- Use Java 21 and Node 22 (pinned in `.mise.toml`).
- Install the shared API before building dependents: `task deps`.
- Run backend checks from `fr-batch-service/`: `./mvnw test` and `./mvnw verify` as appropriate.
- Run UI checks from `batch-ops-ui/`: `npm run check`, `npm test`, and `npm run build` as
  appropriate.
- Prefer `Taskfile.yml` tasks for local stack workflows (`task doctor`, `task up`, `task backend`,
  `task seed`, `task plugins`, `task ui`).

## Change Guidelines

- Preserve the plugin boundary: shared SPI changes belong in `batch-job-api`; update each affected
  plugin and the host together.
- Add or update tests with behavior changes. For database changes, add Flyway migrations rather than
  changing existing applied migrations.
- Do not commit credentials, tokens, local environment files, generated build output, or plugin
  JARs.
- Keep backend formatting compatible with the Maven formatting plugins and UI formatting consistent
  with existing Svelte/TypeScript code.
- Update `RUNNING_LOCALLY.md` or module README files when setup, ports, API behavior, or
  plugin-loading workflows change.

## Validation and Review

- Run the narrowest relevant checks first, then broader module checks when practical.
- Record commands not run and the reason in the final handoff.
