# Example Batch Service

A **learning and experimentation project** for Spring Batch and Spring Framework features. This
repository serves as a reference implementation showcasing different approaches to solve common
batch processing challenges.

## Project Philosophy

This project intentionally demonstrates **multiple ways to implement similar functionality**. You
may find:

- Different pagination strategies (offset-based vs keyset pagination)
- Various reader/writer implementations (JDBC vs JPA)
- Multiple configuration approaches (Java config, annotations)
- Different error handling patterns
- Various testing strategies

This diversity is by design - it provides an easy reference for how to implement things in different
ways with Spring and Spring Batch.

## Tech Stack

- Java 21
- Spring Boot 4.0
- Spring Batch
- Spring Data JPA
- MySQL 8.x
- Vavr (functional programming utilities)
- Micrometer + Prometheus (metrics)

## How to run locally

### Requirements

- Java 21 SDK
- Maven 3.9+
- MySQL 8.2 (via Docker/Podman — see below)
- Docker or Podman

### Run locally

```bash
cd fr-batch-service
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

The `local` profile is required. It wires a local MySQL connection (`fr_batch_local` database, root
user, empty password) and runs Flyway migrations automatically at startup.

For the full setup — starting MySQL, seeding data, building and loading plugins, and running the UI
— see the root [RUNNING_LOCALLY.md](../RUNNING_LOCALLY.md).

## How to develop

See [AGENTS.md](AGENTS.md) for coding standards and development guidelines.

## Spring Batch Jobs

### Job1 - ETL Pipeline Demo

A 3-step pipeline demonstrating a complete ETL workflow:

| Step   | Description       | Reader                          | Writer                |
| ------ | ----------------- | ------------------------------- | --------------------- |
| Step 1 | CSV to Database   | `FlatFileItemReader`            | `JdbcBatchItemWriter` |
| Step 2 | DB Transformation | Custom Keyset Pagination Reader | JPA Repository        |
| Step 3 | DB to REST API    | `JdbcPagingItemReader`          | Custom REST Writer    |

### Implementation Patterns Demonstrated

- **Keyset Pagination**: `Step2KeySetPagingItemReader` - efficient pagination for large datasets
- **Process Indicator Pattern**: Track which records have been processed
- **Chunk-oriented Processing**: Configurable chunk sizes per step
- **External API Integration**: REST client calls within batch processing

## HTTP Web Endpoints

| Endpoint                          | Method | Description           |
| --------------------------------- | ------ | --------------------- |
| `/api/batch-service/jobs/run`     | POST   | Trigger job execution |
| `/api/batch-service/jobs/stop`    | POST   | Stop running jobs     |
| `/api/batch-service/jobs/running` | GET    | List running jobs     |
| `/actuator/health`                | GET    | Health check          |
| `/actuator/prometheus`            | GET    | Prometheus metrics    |

## Roadmap for development experience

- [ ] Easy dev environment setup using docker, docker compose or podman and podman compose, using
  maven wrapper and reusing local m2 artifacs. Some limitations found here was the resources
  limitations for the DB container.

## Roadmap for features included

- [ ] Allow run list of jobs sync and async
- [ ] At least one complex job using csv to db and db to rest endpoint
- [ ] Thirdparty services mocking using mockintosh
- [ ] Metrics with prometheus and grafana
- [ ] Web admin using svelte or react

## How to set up mock service powered by mockintosh

1. Fist install `pipx` to isolate `mockintosh` environment
1. Install `mockintosh` using pipx `pipx install mockintosh`. Latest version `0.13.17`
1. Sometimetimes mockintosh venv doesn't work correctly by incompatible version of `markupsafe`,
   install a compatible version with the mockintosh version using
   `pipx inject mockintosh markupsafe==2.0.1`
1. Test using `mockintosh --version` command
