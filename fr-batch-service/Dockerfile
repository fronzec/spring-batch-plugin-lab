# Stage 1: Build fat JAR with Maven
FROM maven:3.9-eclipse-temurin-21 AS build
# GitHub username for the GitHub Packages 'github' server (overridden in CI).
ARG GITHUB_ACTOR=x-access-token
WORKDIR /build
COPY pom.xml .
# fr-batch-service depends on com.fronzec:batch-job-api hosted on GitHub
# Packages (private). The token is provided as a BuildKit secret and written to
# a throwaway settings.xml that is removed inside the same RUN, so it never
# lands in an image layer or the build cache.
# Download dependencies first for layer caching.
RUN --mount=type=secret,id=gh_token \
    printf '<settings><servers><server><id>github</id><username>%s</username><password>%s</password></server></servers></settings>' \
      "$GITHUB_ACTOR" "$(cat /run/secrets/gh_token)" > /tmp/settings.xml && \
    mvn -s /tmp/settings.xml dependency:go-offline -q; \
    status=$?; rm -f /tmp/settings.xml; exit $status
COPY src/ src/
RUN --mount=type=secret,id=gh_token \
    printf '<settings><servers><server><id>github</id><username>%s</username><password>%s</password></server></servers></settings>' \
      "$GITHUB_ACTOR" "$(cat /run/secrets/gh_token)" > /tmp/settings.xml && \
    mvn -s /tmp/settings.xml clean package -DskipTests -q; \
    status=$?; rm -f /tmp/settings.xml; exit $status

# Stage 2: Minimal JRE runtime
FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
# Install wget for HEALTHCHECK (alpine JRE image does not include it)
RUN apk add --no-cache wget
WORKDIR /app
COPY --from=build /build/target/*.jar app.jar
# Probe the plugin-listing endpoint, not /actuator/health: the aggregate health
# is DOWN (503) until at least one plugin is loaded, which would put a fresh
# container in an endless unhealthy/restart loop. /jobs/plugins returns 200 as
# soon as the app serves — the same readiness signal used by CI and process-compose.
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD wget -qO- http://localhost:8080/api/batch-service/jobs/plugins || exit 1
USER appuser
EXPOSE 8080
# -XX:-UseBASE64Intrinsics is an interim mitigation for CVE-2026-114938 (Java4Shell, #124).
# Remove it once the runtime image ships Temurin 21.0.13.1+2 or later.
ENTRYPOINT ["java", "-Xmx512m", "-XX:MaxMetaspaceSize=256m", "-XX:-UseBASE64Intrinsics", "-jar", "app.jar"]
