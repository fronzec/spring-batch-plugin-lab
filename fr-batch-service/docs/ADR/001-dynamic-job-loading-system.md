# ADR 001: Dynamic Job Loading System with Database-Driven Registry

## Status

**Implemented (Phase 1-6)** — 2026-05-31. Original proposal: 2026-03-02.

Phase 1 (Foundation), Phase 2 (Core Integration), Phase 3 (Dynamic Loading API Layer), Phase 4
(Dynamic Job Loading), Phase 5 (Security and Hardening), and Phase 6 (Example, Documentation, Health
& Metrics) are complete. Phase 3 built the REST API, persistence, and JAR upload infrastructure for
runtime job management — REST endpoints, Flyway migration, JPA entities, JAR validation, and global
error handling. Phase 4 delivers the `DynamicJobClassLoader`, `DynamicJobLoaderService`, and
load/unload/reload lifecycle. Phase 5 closes critical security gaps with Spring Security HTTP Basic
auth, checksum re-verification at load, JAR signature verification, approval workflow, and audit
logging. Phase 6 adds observability (health indicator, Micrometer metrics), a standalone example
plugin project (`example-payment-job/`), updated developer docs, and a production readiness
checklist. Phase 7 remains as future work.

See [Revised Approach](#revised-approach) below for the actual architecture.

## Context and Problem Statement

The current fr-batch-service implementation has several limitations that hinder scalability and
operational flexibility:

### Current Limitations

1. **Hardcoded Job Registry**: Jobs are manually registered in
   `JobsManagerService.manualDefinedJobs` HashMap, requiring code changes and redeployment for new
   jobs
1. **Monolithic Architecture**: All job logic resides in the same codebase, making it difficult for
   teams to work independently
1. **No Version Management**: Cannot run multiple versions of the same job or roll back to previous
   versions
1. **Tight Coupling**: Job implementations are tightly coupled to the core batch service
1. **Limited Reusability**: Jobs cannot be shared across different batch service instances or
   environments
1. **Manual Deployment**: Adding new jobs requires full application rebuild and deployment

### Business Drivers

- **Faster Time-to-Market**: Business teams need to deploy new batch jobs without waiting for core
  platform releases
- **Team Autonomy**: Multiple teams should be able to develop and deploy their own batch jobs
  independently
- **Flexibility**: Ability to enable/disable jobs dynamically based on business needs
- **Operational Safety**: Ability to test jobs in isolation and roll back problematic versions
- **Multi-tenancy**: Support for different job sets per environment or customer

## Decision

We will implement a **Plugin-Based Dynamic Job Loading System** that allows batch jobs to be:

1. Developed as separate, independent Maven/Gradle projects
1. Packaged as executable JAR files
1. Uploaded and registered dynamically at runtime
1. Stored and managed through a database registry
1. Loaded, unloaded, and reloaded without service restart

### Key Components

```
┌─────────────────────────────────────────────────────────────┐
│                   External Job Plugins                       │
│  (Separate Git Repos, Independent Build Lifecycle)          │
│                                                              │
│  job-payment-processing/     job-report-generation/         │
│  job-data-migration/          job-reconciliation/           │
│                                                              │
│  Each implements: BatchJobPlugin interface                  │
│  Each builds to: job-{name}-{version}.jar                   │
└─────────────────────────────────────────────────────────────┘
                            ↓ Upload
┌─────────────────────────────────────────────────────────────┐
│              Job Management REST API                         │
│  POST /job-registry/upload        - Upload JAR             │
│  POST /job-registry/register      - Register job           │
│  PUT  /job-registry/{name}/enable - Enable/disable         │
│  POST /job-registry/{name}/reload - Hot reload             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              Database Job Registry                           │
│                                                              │
│  job_definitions                                            │
│  ├─ id, job_name, version                                   │
│  ├─ jar_file_path, jar_checksum                            │
│  ├─ main_class_name                                         │
│  ├─ enabled, auto_start                                     │
│  └─ created_at, updated_at                                  │
│                                                              │
│  job_parameters_template                                    │
│  ├─ job_definition_id                                       │
│  ├─ param_key, param_type                                   │
│  ├─ default_value, required                                 │
│  └─ description                                             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│         Dynamic Job Loader Service                          │
│                                                              │
│  ┌────────────────────────────────────────┐                │
│  │  DynamicJobClassLoader (per job)       │                │
│  │  - Parent-last delegation              │                │
│  │  - Isolated class space                │                │
│  │  - Proper cleanup on unload            │                │
│  └────────────────────────────────────────┘                │
│                                                              │
│  ┌────────────────────────────────────────┐                │
│  │  JobRegistryService                    │                │
│  │  - Scan JAR for BatchJobPlugin         │                │
│  │  - Validate job structure              │                │
│  │  - Register beans with Spring          │                │
│  │  - Track loaded jobs                   │                │
│  └────────────────────────────────────────┘                │
│                                                              │
│  ┌────────────────────────────────────────┐                │
│  │  ClassLoaderManager                    │                │
│  │  - Manage classloader lifecycle        │                │
│  │  - Handle unload/reload                │                │
│  │  - Prevent memory leaks                │                │
│  └────────────────────────────────────────┘                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│         Spring Batch Job Execution                          │
│  (Existing JobsManagerService with minor refactoring)       │
└─────────────────────────────────────────────────────────────┘
```

## Revised Approach (Implemented)

The original proposal envisioned **dynamic JAR loading with classloader isolation** — jobs packaged
as external JARs, uploaded at runtime, loaded by a custom `DynamicJobClassLoader`, and registered
into a database-driven registry. That approach remains the long-term vision (Phases 3-7).

**What was actually built** is a pragmatic first step: a **classpath-based plugin registry** that
solves the immediate problem — eliminating the hardcoded job map in `JobsManagerService` — without
introducing classloader complexity.

### Actual Architecture (Phase 1-2)

```
┌───────────────────────────────────────────────────────┐
│              batch-job-api (published to GitHub Pkgs)  │
│  BatchJobPlugin interface                              │
│  JobMetadata interface                                 │
└───────────────────────┬───────────────────────────────┘
                        │ implements
        ┌───────────────┼───────────────┐
        │                               │
┌───────▼──────────┐          ┌─────────▼──────────┐
│  Job1Plugin      │          │  Job2Plugin          │
│  (@Component)    │          │  (future)            │
│  3-step ETL      │          │                      │
└───────┬──────────┘          └─────────┬──────────┘
        │                               │
        │   Spring injects all          │
        │   BatchJobPlugin beans →      │
        └───────────────┬───────────────┘
                        │
        ┌───────────────▼────────────────────────────┐
        │  PluginRegistryService (@Service)           │
        │  - @PostConstruct: configureJob() per plugin│
        │  - Registers Job into MapJobRegistry        │
        │  - Fail-fast on duplicates, nulls, mismatch │
        │  - Exposes getPlugins(), getPlugin(name)    │
        └───────────────┬────────────────────────────┘
                        │
        ┌───────────────▼────────────────────────────┐
        │  JobsManagerService (refactored)            │
        │  - Delegates discovery to PluginRegistry    │
        │  - No more hardcoded manualDefinedJobs map  │
        │  - Sync/async launch, stop, getRunning      │
        └───────────────┬────────────────────────────┘
                        │
        ┌───────────────▼────────────────────────────┐
        │  REST Controllers                           │
        │  JobController: /jobs/start-all, stop, etc. │
        │  PluginController: GET /jobs/plugins        │
        └────────────────────────────────────────────┘
```

### Key Differences from Original Proposal

| Aspect             | Original ADR                              | Actual Implementation                              |
| ------------------ | ----------------------------------------- | -------------------------------------------------- |
| Plugin discovery   | Dynamic JAR scan + SPI                    | Spring bean injection (`List<BatchJobPlugin>`)     |
| Class loading      | Custom `DynamicJobClassLoader` per JAR    | Shared classpath (no isolation)                    |
| Registration       | Database-driven + REST upload API         | In-memory `MapJobRegistry` at startup              |
| Hot reload         | Unload → reload cycle                     | Requires app restart                               |
| Plugin location    | External JAR in `/var/batch-jobs/`        | Same codebase (`@Component` in `fr-batch-service`) |
| Version management | Multiple versions coexist                 | One version per deployment                         |
| Complexity         | Very high (classloader, memory mgmt, SPI) | Low (plain Spring beans)                           |

### Why This Approach First

1. **Solves the immediate problem**: `JobsManagerService` no longer has a hardcoded job map. Adding
   a new job means creating a `@Component` that implements `BatchJobPlugin` — no other code changes.
1. **Zero operational risk**: No classloader leaks, no JAR validation, no upload security surface.
   Plain Spring beans behave exactly like the rest of the application.
1. **Progressive complexity**: The `PluginRegistryService` contract (`getPlugins()`,
   `getPlugin(name)`) is the same abstraction a future dynamic loader would populate. Swap the
   backend, keep the API.
1. **Testable today**: Integration tests can `@Autowired List<BatchJobPlugin>` and verify the full
   pipeline end-to-end with `ExitStatus.COMPLETED`.

### ADR Decisions Made During Implementation

**ADR-I1: Classpath-based discovery over dynamic JAR loading for Phase 2**

- **Decision**: Use Spring's existing bean injection (`List<BatchJobPlugin>`) instead of custom
  classloaders.
- **Rationale**: The business need is removing the hardcoded job map, not runtime JAR upload.
  Dynamic classloading adds 10x complexity for a feature (hot reload) that has no production demand
  yet. The plugin contract is the same either way — when dynamic loading is needed, the registry
  service API stays stable.
- **Trade-off**: Plugins live in the same codebase and classpath. True team isolation (separate
  repos, independent deploy) is deferred.
- **Rejected**: Building the full `DynamicJobClassLoader` infrastructure now, which would have
  delayed the plugin migration by weeks and introduced memory-leak risk before the basic contract
  was validated.

**ADR-I2: @MockitoBean over @MockBean for Spring Boot 4.0.0**

- **Decision**: Use `org.springframework.test.context.bean.override.mockito.MockitoBean` for test
  mocking.
- **Rationale**: Spring Boot 4.0.0 / Spring Framework 7.x relocated the mock annotation. `@MockBean`
  still exists but `@MockitoBean` is the forward-looking choice for the version this project
  targets.
- **Rejected**: `@MockBean` (deprecated path).

**ADR-I3: Assert on Map.get("result") instead of unwrapping JobExecution**

- **Decision**: The integration test assertion reads `result.get("result")` from
  `JobsManagerService.syncRunJobWithParams()` return map, not a direct `JobExecution` reference.
- **Rationale**: The map IS the public contract of the service. Adding a `JobExecutionListener` or
  `JobRepository` query to the test expands surface area for no extra signal.
- **Rejected**: Injecting `JobRepository` and querying execution history (brittle ordering), or
  adding test-only scaffolding.

**ADR-I4: 3-row CSV fixture over 1000 rows**

- **Decision**: The test CSV `sample-persons-1k.csv` contains 3 rows despite the filename.
- **Rationale**: The filename is a hardcoded artifact in `CsvReader`. Changing it would require a
  production code change (out of scope). 3 rows exercise chunk processing without bloating CI
  runtime. The name does not need to match the size.
- **Rejected**: Generating 1000 rows (slower CI, no extra coverage) or refactoring `CsvReader` to
  accept a configurable path (production change out of scope).

## Technical Architecture

### 1. Job Plugin Contract (batch-job-api module)

Create a separate API module that defines the contract for job plugins:

```java
// batch-job-api/src/main/java/com/fronzec/api/BatchJobPlugin.java
public interface BatchJobPlugin {
    /**
     * Unique identifier for the job
     */
    String getJobName();

    /**
     * Semantic version of this job implementation
     */
    String getVersion();

    /**
     * Configure and return the Spring Batch Job
     */
    Job configureJob(
        JobRepository jobRepository,
        PlatformTransactionManager transactionManager,
        ApplicationContext parentContext
    );

    /**
     * Default parameters for this job
     */
    Map<String, String> getDefaultParameters();

    /**
     * List of bean names/types required from parent context
     */
    List<String> getRequiredDependencies();

    /**
     * Metadata about the job
     */
    JobMetadata getMetadata();
}

public interface JobMetadata {
    String getDisplayName();
    String getDescription();
    String getAuthor();
    List<String> getTags();
    Duration getEstimatedRuntime();
}
```

### 2. Database Schema

```sql
-- Job registry
CREATE TABLE job_definitions (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_name VARCHAR(255) UNIQUE NOT NULL,
    display_name VARCHAR(255),
    description TEXT,
    version VARCHAR(50) NOT NULL,
    jar_file_path VARCHAR(500) NOT NULL,
    jar_checksum VARCHAR(64) NOT NULL,
    main_class_name VARCHAR(255) NOT NULL,
    enabled BOOLEAN DEFAULT true,
    auto_start BOOLEAN DEFAULT false,
    load_status VARCHAR(50), -- LOADED, UNLOADED, FAILED, LOADING
    load_error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    INDEX idx_job_name (job_name),
    INDEX idx_enabled (enabled),
    INDEX idx_load_status (load_status)
);

-- Parameter templates
CREATE TABLE job_parameters_template (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_definition_id BIGINT NOT NULL,
    param_key VARCHAR(100) NOT NULL,
    param_type VARCHAR(50) NOT NULL, -- STRING, LONG, DOUBLE, DATE, BOOLEAN
    default_value VARCHAR(255),
    required BOOLEAN DEFAULT false,
    description TEXT,
    validation_regex VARCHAR(500),
    FOREIGN KEY (job_definition_id) REFERENCES job_definitions(id) ON DELETE CASCADE,
    UNIQUE KEY uk_job_param (job_definition_id, param_key)
);

-- Job execution history (extends Spring Batch tables)
CREATE TABLE job_executions_audit (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_definition_id BIGINT NOT NULL,
    job_execution_id BIGINT NOT NULL,
    job_version VARCHAR(50),
    execution_metadata JSON,
    FOREIGN KEY (job_definition_id) REFERENCES job_definitions(id),
    FOREIGN KEY (job_execution_id) REFERENCES BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
);
```

`job_executions_audit` has a single writer: `JobExecutionAuditListener`, which
`PluginRegistryService` appends to every job (static and dynamic) right before
`JobRegistry.register(job)`, so every launch path, sync or async, is audited. `beforeJob` inserts
the row with the start time; `afterJob` adds the finish time, duration, and the outcome as the raw
`BatchStatus` name (`COMPLETED`, `FAILED`, `STOPPED`, ...). Jobs without a `job_definitions` row
are not audited, and audit failures never fail the job.

### 3. Dynamic ClassLoader Strategy

**Parent-Last Delegation Model:**

```
Parent ClassLoader (fr-batch-service)
    ↑ (only for core classes)
    |
Job ClassLoader (isolated per job)
    ↓ (first looks in job JAR)
Job JAR Classes
```

**Key Features:**

- Each job gets its own `URLClassLoader` instance
- Jobs can use different versions of libraries
- Spring core classes loaded from parent to share infrastructure
- Proper cleanup to prevent memory leaks

```java
public class DynamicJobClassLoader extends URLClassLoader {
    private final String jobName;
    private final ClassLoader parent;
    private final Set<String> sharedPackages;

    public DynamicJobClassLoader(URL[] urls, ClassLoader parent, String jobName) {
        super(urls, parent);
        this.jobName = jobName;
        this.parent = parent;
        // Core Spring packages always delegated to parent
        this.sharedPackages = Set.of(
            "org.springframework.",
            "jakarta.",
            "org.slf4j.",
            "com.fronzec.api." // Job plugin API
        );
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException {
        // Delegate core packages to parent
        if (sharedPackages.stream().anyMatch(name::startsWith)) {
            return parent.loadClass(name);
        }

        // Parent-last: try to load from this classloader first
        synchronized (getClassLoadingLock(name)) {
            Class<?> c = findLoadedClass(name);
            if (c == null) {
                try {
                    c = findClass(name);
                } catch (ClassNotFoundException e) {
                    c = parent.loadClass(name);
                }
            }
            if (resolve) {
                resolveClass(c);
            }
            return c;
        }
    }

    public void cleanup() {
        try {
            close();
            // Clear any ThreadLocals
            ThreadLocal.class.getDeclaredField("threadLocals").set(null, null);
        } catch (Exception e) {
            // Log error
        }
    }
}
```

### 4. Job Loading Service

```java
@Service
@Slf4j
public class DynamicJobLoaderService {

    private final JobDefinitionRepository jobDefinitionRepository;
    private final JobRegistry jobRegistry;
    private final BeanFactory beanFactory;
    private final Map<String, DynamicJobClassLoader> classLoaders = new ConcurrentHashMap<>();
    private final Map<String, BatchJobPlugin> loadedPlugins = new ConcurrentHashMap<>();

    /**
     * Load job from JAR file
     */
    public LoadResult loadJobFromJar(Long jobDefinitionId) {
        JobDefinition jobDef = jobDefinitionRepository.findById(jobDefinitionId)
            .orElseThrow(() -> new JobNotFoundException(jobDefinitionId));

        try {
            // Validate JAR
            File jarFile = new File(jobDef.getJarFilePath());
            validateJar(jarFile, jobDef.getJarChecksum());

            // Create classloader
            URL jarUrl = jarFile.toURI().toURL();
            DynamicJobClassLoader classLoader = new DynamicJobClassLoader(
                new URL[]{jarUrl},
                this.getClass().getClassLoader(),
                jobDef.getJobName()
            );

            // Discover plugin implementation
            BatchJobPlugin plugin = discoverPlugin(classLoader, jobDef.getMainClassName());

            // Validate plugin
            validatePlugin(plugin, jobDef);

            // Register with Spring
            Job springBatchJob = plugin.configureJob(
                beanFactory.getBean(JobRepository.class),
                beanFactory.getBean(PlatformTransactionManager.class),
                (ApplicationContext) beanFactory
            );

            // Register in job registry
            if (jobRegistry instanceof RunnableJobRegistry) {
                ((RunnableJobRegistry) jobRegistry).register(
                    new RunnableJob(springBatchJob)
                );
            }

            // Track loaded job
            classLoaders.put(jobDef.getJobName(), classLoader);
            loadedPlugins.put(jobDef.getJobName(), plugin);

            // Update database status
            jobDef.setLoadStatus(LoadStatus.LOADED);
            jobDef.setLoadError(null);
            jobDefinitionRepository.save(jobDef);

            log.info("Successfully loaded job: {} version {}",
                jobDef.getJobName(), jobDef.getVersion());

            return LoadResult.success(jobDef.getJobName());

        } catch (Exception e) {
            log.error("Failed to load job: {}", jobDef.getJobName(), e);
            jobDef.setLoadStatus(LoadStatus.FAILED);
            jobDef.setLoadError(e.getMessage());
            jobDefinitionRepository.save(jobDef);
            return LoadResult.failure(jobDef.getJobName(), e.getMessage());
        }
    }

    /**
     * Unload job and cleanup resources
     */
    public void unloadJob(String jobName) {
        BatchJobPlugin plugin = loadedPlugins.remove(jobName);
        DynamicJobClassLoader classLoader = classLoaders.remove(jobName);

        if (classLoader != null) {
            // Unregister from Spring
            if (jobRegistry instanceof RunnableJobRegistry) {
                ((RunnableJobRegistry) jobRegistry).unregister(jobName);
            }

            // Cleanup classloader
            classLoader.cleanup();

            // Force GC to reclaim memory
            System.gc();
        }
    }

    /**
     * Reload job (unload + load)
     */
    public LoadResult reloadJob(String jobName) {
        JobDefinition jobDef = jobDefinitionRepository.findByJobName(jobName)
            .orElseThrow(() -> new JobNotFoundException(jobName));

        unloadJob(jobName);
        return loadJobFromJar(jobDef.getId());
    }

    private BatchJobPlugin discoverPlugin(ClassLoader classLoader, String mainClassName)
            throws Exception {
        // Try explicit class name first
        if (mainClassName != null && !mainClassName.isEmpty()) {
            Class<?> pluginClass = classLoader.loadClass(mainClassName);
            return (BatchJobPlugin) pluginClass.getDeclaredConstructor().newInstance();
        }

        // Fall back to Service Provider Interface discovery
        ServiceLoader<BatchJobPlugin> loader = ServiceLoader.load(
            BatchJobPlugin.class,
            classLoader
        );

        Iterator<BatchJobPlugin> iterator = loader.iterator();
        if (!iterator.hasNext()) {
            throw new PluginNotFoundException("No BatchJobPlugin implementation found");
        }

        return iterator.next();
    }

    private void validateJar(File jarFile, String expectedChecksum) throws IOException {
        if (!jarFile.exists()) {
            throw new FileNotFoundException("JAR file not found: " + jarFile);
        }

        // Validate checksum
        String actualChecksum = calculateSHA256(jarFile);
        if (!actualChecksum.equals(expectedChecksum)) {
            throw new SecurityException("JAR checksum mismatch");
        }
    }

    private void validatePlugin(BatchJobPlugin plugin, JobDefinition jobDef) {
        // Validate job name matches
        if (!plugin.getJobName().equals(jobDef.getJobName())) {
            throw new ValidationException("Job name mismatch");
        }

        // Validate required dependencies are available
        for (String dependency : plugin.getRequiredDependencies()) {
            if (!beanFactory.containsBean(dependency)) {
                throw new DependencyNotFoundException(
                    "Required bean not found: " + dependency
                );
            }
        }
    }

    private String calculateSHA256(File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
            return Hex.encodeHexString(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
```

### 5. REST API for Job Management

```java
@RestController
@RequestMapping("/api/batch-service/job-registry")
@Slf4j
public class JobManagementController {

    private final DynamicJobLoaderService jobLoaderService;
    private final JobDefinitionRepository jobDefinitionRepository;

    @PostMapping("/upload")
    public ResponseEntity<UploadResponse> uploadJobJar(
            @RequestParam("file") MultipartFile file,
            @RequestParam("jobName") String jobName,
            @RequestParam("version") String version,
            @RequestParam(value = "mainClassName", required = false) String mainClassName) {

        // Validate file
        if (file.isEmpty() || !file.getOriginalFilename().endsWith(".jar")) {
            return ResponseEntity.badRequest()
                .body(UploadResponse.error("Invalid JAR file"));
        }

        try {
            // Save file to storage
            String fileName = String.format("job-%s-%s.jar", jobName, version);
            Path storagePath = Paths.get("/var/batch-jobs/", fileName);
            Files.copy(file.getInputStream(), storagePath, StandardCopyOption.REPLACE_EXISTING);

            // Calculate checksum
            String checksum = calculateChecksum(storagePath.toFile());

            // Save to database
            JobDefinition jobDef = new JobDefinition();
            jobDef.setJobName(jobName);
            jobDef.setVersion(version);
            jobDef.setJarFilePath(storagePath.toString());
            jobDef.setJarChecksum(checksum);
            jobDef.setMainClassName(mainClassName);
            jobDef.setEnabled(false); // Disabled by default until explicitly enabled
            jobDef.setLoadStatus(LoadStatus.UNLOADED);

            jobDefinitionRepository.save(jobDef);

            return ResponseEntity.ok(UploadResponse.success(jobDef.getId()));

        } catch (Exception e) {
            log.error("Failed to upload job JAR", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(UploadResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/register")
    public ResponseEntity<RegisterResponse> registerJob(@RequestBody JobRegistrationRequest request) {
        JobDefinition jobDef = new JobDefinition();
        jobDef.setJobName(request.getJobName());
        jobDef.setDisplayName(request.getDisplayName());
        jobDef.setDescription(request.getDescription());
        jobDef.setVersion(request.getVersion());
        jobDef.setJarFilePath(request.getJarFilePath());
        jobDef.setJarChecksum(request.getChecksum());
        jobDef.setMainClassName(request.getMainClassName());
        jobDef.setEnabled(false);

        jobDefinitionRepository.save(jobDef);

        return ResponseEntity.ok(RegisterResponse.success(jobDef.getId()));
    }

    @PutMapping("/{jobName}/enable")
    public ResponseEntity<StatusResponse> enableJob(@PathVariable String jobName) {
        JobDefinition jobDef = jobDefinitionRepository.findByJobName(jobName)
            .orElseThrow(() -> new JobNotFoundException(jobName));

        jobDef.setEnabled(true);
        jobDefinitionRepository.save(jobDef);

        // Automatically load enabled jobs
        LoadResult result = jobLoaderService.loadJobFromJar(jobDef.getId());

        return ResponseEntity.ok(StatusResponse.from(result));
    }

    @PutMapping("/{jobName}/disable")
    public ResponseEntity<StatusResponse> disableJob(@PathVariable String jobName) {
        JobDefinition jobDef = jobDefinitionRepository.findByJobName(jobName)
            .orElseThrow(() -> new JobNotFoundException(jobName));

        jobDef.setEnabled(false);
        jobDefinitionRepository.save(jobDef);

        jobLoaderService.unloadJob(jobName);

        return ResponseEntity.ok(StatusResponse.success("Job disabled"));
    }

    @PostMapping("/{jobName}/reload")
    public ResponseEntity<StatusResponse> reloadJob(@PathVariable String jobName) {
        LoadResult result = jobLoaderService.reloadJob(jobName);
        return ResponseEntity.ok(StatusResponse.from(result));
    }

    @GetMapping
    public ResponseEntity<List<JobDefinitionDto>> listJobs() {
        List<JobDefinition> jobs = jobDefinitionRepository.findAll();
        return ResponseEntity.ok(
            jobs.stream()
                .map(JobDefinitionDto::from)
                .collect(Collectors.toList())
        );
    }

    @GetMapping("/{jobName}")
    public ResponseEntity<JobDefinitionDto> getJob(@PathVariable String jobName) {
        JobDefinition jobDef = jobDefinitionRepository.findByJobName(jobName)
            .orElseThrow(() -> new JobNotFoundException(jobName));
        return ResponseEntity.ok(JobDefinitionDto.from(jobDef));
    }

    // Implemented as DELETE /jobs/definitions/{id}[?force=true] -> 204 No Content,
    // delegating to DynamicJobLoaderService.deleteJob(id, force).
    @DeleteMapping("/definitions/{id}")
    public ResponseEntity<Void> deleteDefinition(
            @PathVariable long id, @RequestParam(defaultValue = "false") boolean force) {
        jobLoaderService.deleteJob(id, force);
        return ResponseEntity.noContent().build();
    }
}

// DynamicJobLoaderService.deleteJob(id, force), all under the per-definition lifecycle lock:
//   1. Unknown id -> NoSuchElementException (404).
//   2. Execution history in job_executions_audit -> JobDeleteConflictException (409) before
//      anything is unloaded; force=true does not bypass it. Disable the definition instead.
//   3. Unload (reuses the unload path): running executions without force -> 409, nothing
//      deleted; force=true skips the guard but does not stop running executions.
//   4. One transaction: delete parameter templates, then the definition row.
//   5. After commit: Files.deleteIfExists(jar). A missing JAR is fine; an I/O failure still
//      succeeds, logs WARN, and names the orphan JAR in the audit detail.
//   6. Audit JOB_DELETED SUCCESS only after completion; any failure up to the commit audits
//      JOB_DELETED FAILURE with id, jobName, and reason, then rethrows.
```

### 6. Example Job Plugin Project

**Project Structure:**

```
example-payment-job/
├── pom.xml
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/example/payment/
│   │   │       ├── PaymentJobPlugin.java
│   │   │       ├── PaymentProcessor.java
│   │   │       ├── PaymentReader.java
│   │   │       └── PaymentWriter.java
│   │   └── resources/
│   │       └── META-INF/
│   │           └── services/
│   │               └── com.fronzec.api.BatchJobPlugin
│   └── test/
└── README.md
```

**pom.xml:**

```xml
<dependencies>
    <!-- Job Plugin API -->
    <dependency>
        <groupId>com.fronzec</groupId>
        <artifactId>batch-job-api</artifactId>
        <version>1.0.0</version>
        <scope>provided</scope> <!-- Provided by parent platform -->
    </dependency>

    <!-- Spring Boot dependencies (provided) -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-batch</artifactId>
        <scope>provided</scope>
    </dependency>

    <!-- Job-specific dependencies (packaged) -->
    <dependency>
        <groupId>com.some-payment-api</groupId>
        <artifactId>payment-sdk</artifactId>
        <version>2.1.0</version>
    </dependency>
</dependencies>

<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>3.5.0</version>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                    <configuration>
                        <artifactSet>
                            <includes>
                                <!-- Include only job-specific dependencies -->
                                <include>com.some-payment-api:payment-sdk</include>
                            </includes>
                        </artifactSet>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

**PaymentJobPlugin.java:**

```java
package com.example.payment;

import com.fronzec.api.BatchJobPlugin;
import com.fronzec.api.JobMetadata;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class PaymentJobPlugin implements BatchJobPlugin {

    @Override
    public String getJobName() {
        return "payment-processing";
    }

    @Override
    public String getVersion() {
        return "1.0.0";
    }

    @Override
    public Job configureJob(JobRepository jobRepository,
                           PlatformTransactionManager transactionManager,
                           ApplicationContext parentContext) {

        // Get required beans from parent context
        DataSource dataSource = parentContext.getBean(DataSource.class);

        // Create reader, processor, writer
        PaymentReader reader = new PaymentReader(dataSource);
        PaymentProcessor processor = new PaymentProcessor();
        PaymentWriter writer = new PaymentWriter();

        // Build step
        Step step = new StepBuilder("paymentProcessingStep", jobRepository)
            .<Payment, ProcessedPayment>chunk(100, transactionManager)
            .reader(reader)
            .processor(processor)
            .writer(writer)
            .build();

        // Build job
        return new JobBuilder(getJobName(), jobRepository)
            .start(step)
            .build();
    }

    @Override
    public Map<String, String> getDefaultParameters() {
        return Map.of(
            "DATE", "2024-01-01",
            "BATCH_SIZE", "1000"
        );
    }

    @Override
    public List<String> getRequiredDependencies() {
        return List.of("dataSource", "transactionManager");
    }

    @Override
    public JobMetadata getMetadata() {
        return new JobMetadata() {
            @Override
            public String getDisplayName() {
                return "Payment Processing Job";
            }

            @Override
            public String getDescription() {
                return "Processes daily payment transactions and generates reports";
            }

            @Override
            public String getAuthor() {
                return "Payment Team";
            }

            @Override
            public List<String> getTags() {
                return List.of("payment", "financial", "daily");
            }

            @Override
            public Duration getEstimatedRuntime() {
                return Duration.ofMinutes(30);
            }
        };
    }
}
```

**META-INF/services/com.fronzec.api.BatchJobPlugin:**

```
com.example.payment.PaymentJobPlugin
```

### 7. Integration with Existing JobsManagerService

**Refactor approach:**

```java
@Service
public class JobsManagerService {

    private final JobDefinitionRepository jobDefinitionRepository;
    private final DynamicJobLoaderService jobLoaderService;
    // ... existing fields

    @PostConstruct
    public void init() {
        // Load enabled jobs from database
        List<JobDefinition> enabledJobs = jobDefinitionRepository.findByEnabled(true);

        for (JobDefinition jobDef : enabledJobs) {
            try {
                jobLoaderService.loadJobFromJar(jobDef.getId());
                logger.info("Auto-loaded job: {}", jobDef.getJobName());
            } catch (Exception e) {
                logger.error("Failed to auto-load job: {}", jobDef.getJobName(), e);
            }
        }

        // Keep existing static job registration for backward compatibility
        // ... existing init code
    }

    // Existing methods remain largely unchanged
    // They will work with both static and dynamic jobs
}
```

## Consequences

### Positive

1. **Agility**: Teams can develop and deploy jobs independently
1. **Isolation**: Job failures don't affect other jobs or core platform
1. **Versioning**: Multiple versions can coexist, easy rollback
1. **Scalability**: Core platform doesn't grow with every new job
1. **Flexibility**: Enable/disable jobs without deployment
1. **Testability**: Jobs can be tested in isolation
1. **Reusability**: Jobs can be shared across environments
1. **Auditability**: Complete history of job deployments in database

### Negative

1. **Complexity**: Significant increase in system complexity
1. **Memory Management**: Risk of memory leaks from classloaders
1. **Debugging Difficulty**: Harder to debug across classloader boundaries
1. **Transaction Boundaries**: Care needed with transaction management across classloaders
1. **Classpath Conflicts**: Potential for subtle bugs due to library version conflicts
1. **Performance Overhead**: Classloader delegation adds minor overhead
1. **Operational Burden**: Need to manage JAR storage, versioning, cleanup
1. **Security Risks**: Uploaded JARs could contain malicious code

### Risks and Mitigations

| Risk                                       | Likelihood | Impact   | Mitigation                                                      |
| ------------------------------------------ | ---------- | -------- | --------------------------------------------------------------- |
| Memory leaks from classloaders             | High       | High     | Implement proper cleanup, monitoring, automated tests           |
| Malicious JAR uploads                      | Medium     | Critical | JAR signing, checksum validation, sandboxing, approval workflow |
| Version conflicts                          | Medium     | Medium   | Isolated classloaders, dependency documentation                 |
| Performance degradation                    | Low        | Medium   | Benchmarking, lazy loading, caching                             |
| Job incompatibility after platform upgrade | Medium     | High     | Versioned API, compatibility testing, deprecation policy        |
| Database corruption                        | Low        | Critical | Regular backups, transaction management, validation             |
| Storage exhaustion                         | Medium     | Medium   | Quota limits, cleanup policies, monitoring                      |

### Trade-offs

**Chosen Approach vs. Alternatives:**

| Aspect               | Dynamic Loading | Microservices    | Script-Based |
| -------------------- | --------------- | ---------------- | ------------ |
| Complexity           | Medium          | High             | Low          |
| Isolation            | Medium          | High             | Low          |
| Performance          | Good            | Medium (network) | Good         |
| Resource Usage       | Medium          | High             | Low          |
| Development Effort   | High            | Very High        | Low          |
| Operational Overhead | Medium          | High             | Low          |
| Security             | Medium          | High             | Low          |
| Spring Integration   | Excellent       | Medium           | Medium       |

## Alternatives Considered

### Alternative 1: Microservices Architecture

**Description:** Each job as a separate Spring Boot microservice with REST API

**Pros:**

- Complete isolation (separate processes)
- Independent scaling
- Technology heterogeneity
- Standard deployment patterns

**Cons:**

- Much higher resource usage (JVM per job)
- Network latency for orchestration
- Complex service discovery and coordination
- Higher operational overhead
- Overkill for batch processing

**Verdict:** Rejected - Too heavyweight for batch jobs that run infrequently

### Alternative 2: Script-Based Jobs (Groovy/Kotlin Scripts)

**Description:** Store job logic as scripts in database, execute via scripting engine

**Pros:**

- Maximum flexibility
- No JAR packaging needed
- Easy to modify jobs
- Low complexity

**Cons:**

- Security nightmare (arbitrary code execution)
- Poor performance compared to compiled code
- Difficult to test and debug
- Limited IDE support
- No compile-time safety

**Verdict:** Rejected - Security and maintainability concerns

### Alternative 3: GraalVM Native Images

**Description:** Compile each job to native binary, execute as separate process

**Pros:**

- Best isolation
- Fast startup
- Low memory footprint
- No classloader issues

**Cons:**

- Limited Spring Batch support in native mode
- Long compilation times
- Reflection configuration complexity
- Limited runtime flexibility
- Difficult debugging

**Verdict:** Rejected - Not mature enough for Spring Batch ecosystem

### Alternative 4: Keep Current Approach

**Description:** Continue with monolithic jobs in single codebase

**Pros:**

- Simple to understand
- No additional complexity
- Easy debugging
- Good IDE support

**Cons:**

- Cannot add jobs without redeployment
- Tight coupling
- No version management
- Difficult to scale development teams

**Verdict:** Rejected - Does not meet business requirements for agility

## Implementation Plan

### Phase 1: Foundation ✅ COMPLETE (May 2026)

- [x] Create `batch-job-api` module with plugin interfaces
- [x] Design database schema (Flyway removed; schema lives in test resources and external
  migrations)
- [x] Set up artifact repository for API module (GitHub Packages)
- [x] Create documentation for job plugin development (`docs/job-plugins/developer-guide.md`)

**Implemented:** `batch-job-api` published to GitHub Packages as
`com.fronzec:batch-job-api:0.0.1-SNAPSHOT`. Developer guide written. Flyway migrations exist in
`migrations/` but are externalized from the main build — see commit `e675bb9`.

### Phase 2: Core Integration ✅ COMPLETE (May 2026) — REVISED SCOPE

- [x] Implement `PluginRegistryService` (classpath-based bean discovery, MapJobRegistry
  registration)
- [x] Migrate `job1` from hardcoded `@Bean Job` to `Job1Plugin` implementing `BatchJobPlugin`
- [x] Refactor `JobsManagerService` to delegate discovery to plugin registry (remove
  `manualDefinedJobs`)
- [x] Add `PluginRegistrationException` for fail-fast duplicate/null/name-mismatch detection
- [x] Expose `GET /jobs/plugins` endpoint with plugin metadata (`PluginController`,
  `PluginInfoResponse`)
- [x] Add unit tests (`PluginRegistryServiceTest`, updated `JobsManagerServiceTest`)
- [x] Add integration test (`PluginArchitectureIntegrationTest`) with full ETL pipeline assertion
- [x] Fix H2 2.4.x dialect bug in `Step3Reader` (parameterized `IS ?` → literal `IS NULL`)

**Deferred from original scope:**

- [ ] `DynamicJobClassLoader` (deferred — no production demand for runtime JAR loading yet)
- [ ] `ClassLoaderManager` (deferred)
- [ ] `DynamicJobLoaderService` with JAR upload (deferred — replaced by `PluginRegistryService`)
- [ ] JPA entities and repositories for job registry (deferred)

**Deliverables:** Plugin registry functional with 5/5 integration tests passing. Any `@Component`
implementing `BatchJobPlugin` is auto-discovered at startup. `Job1Plugin` runs 3-step ETL pipeline
to `ExitStatus.COMPLETED` in integration tests. See PRs #27, #28, #29.

### Phase 3: Dynamic Loading API Layer ✅ COMPLETE (May 2026)

- [x] Flyway enabled + `V1__job_registry_tables.sql` migration (3 tables: `job_definitions`,
  `job_parameters_template`, `job_executions_audit`)
- [x] 3 JPA entities + 3 Spring Data JPA repositories (getter/setter style matching `PersonsEntity`
  conventions)
- [x] `JarUploadService` with JAR extension validation + ZIP magic bytes (`PK\x03\x04`), SHA-256
  checksum, path traversal protection, duplicate job-name detection via
  `JobDefinitionRepository.findByJobName()`
- [x] `JobManagementController` with 6 endpoints: `POST /jobs/upload`, `GET /jobs/definitions`,
  `GET /jobs/definitions/{id}`, `PUT /jobs/definitions/{id}/enable`,
  `PUT /jobs/definitions/{id}/disable`, `DELETE /jobs/definitions/{id}`
- [x] `PluginRegistryService` extended with `registerDynamicPlugin()` / `unregisterDynamicPlugin()`
  — thread-safe via `synchronized(pluginsByJobName)` block, stores `classLoaderRef` for Phase 4
- [x] `MergedPluginInfoResponse` with `PluginSource` (`CLASSPATH`|`UPLOADED`) and `PluginStatus`
  (`ACTIVE`|`LOADED`|`ENABLED`|`DISABLED`) enums — merged plugin listing from both classpath + DB
  sources
- [x] `GlobalExceptionHandler` (`@ControllerAdvice`) with 6 handlers: `InvalidJarException`→400,
  `DuplicateJobDefinitionException`→409, `NoSuchElementException`→404,
  `MethodArgumentNotValidException`→400, `MaxUploadSizeExceededException`→413, `Exception`→500
- [x] `PluginManagementIntegrationTest` with 11 tests covering upload, CRUD, error handling,
  enable/disable, merged plugin listing, max-size validation
- [x] All 42 tests passing (31 existing + 11 new), `mvn test` BUILD SUCCESS

**Deferred from Phase 3 (resolved):**

- [x] `DynamicJobClassLoader` — implemented in Phase 4 (PR #35)
- [x] `POST /jobs/definitions/{id}/reload` — implemented in Phase 4 (PR #37)
- [x] JAR signature verification — implemented in Phase 5 (PR #42)

**Deliverables:** REST API layer for runtime job management fully operational. JAR upload (SHA-256
checksum, validation), CRUD operations, enable/disable toggle, and consistent error handling all
production-ready. The `PluginController.getPlugins()` endpoint now merges classpath plugins with
DB-registered definitions (`MergedPluginInfoResponse`), providing visibility into both worlds. 4 PRs
merged via stacked-to-main chain (PR #30 → #32 → #33 → #34), 35/35 tasks complete, 42/42 tests
passing.

### Phase 4: Dynamic Job Loading ✅ COMPLETE (May 2026)

- [x] `DynamicJobClassLoader` — parent-last `URLClassLoader` with shared-package delegation
  (`org.springframework.*`, `jakarta.*`, `org.slf4j.*`, `com.fronzec.api.*`); `cleanup()` for
  resource release
- [x] `DynamicJobLoaderService` — orchestrates load/unload/reload lifecycle; validates JAR checksum,
  creates classloader, loads `BatchJobPlugin` via `mainClassName`, registers via
  `PluginRegistryService.registerDynamicPlugin()`, tracks `loadStatus` transitions in DB
- [x] `POST /jobs/definitions/{id}/load` — load plugin from uploaded JAR; returns 200/404/409/400
- [x] `POST /jobs/definitions/{id}/unload` — unload and close classloader; `?force=true` stops
  running executions first; returns 200/404/409
- [x] `POST /jobs/definitions/{id}/reload` — atomic unload+load cycle
- [x] `POST /jobs/load-all` — bulk load all enabled/unloaded definitions
- [x] `PluginAutoLoader` (ApplicationRunner) — auto-load `enabled=true` definitions at startup;
  failures logged, do not block startup
- [x] 11 E2E integration tests (`DynamicJobLoadingIntegrationTest`) covering full lifecycle:
  upload→enable→load→unload→reload→execute→loadAll
- [x] Backward compatibility verified — all 69 pre-Phase-4 tests pass unchanged; classpath
  `Job1Plugin` continues working

**Deferred from Phase 4:**

- [x] Health checks for loaded jobs — ✅ COMPLETE (Phase 6, PR #44)
- [x] Metrics and monitoring (Micrometer instrumentation) — ✅ COMPLETE (Phase 6, PR #44)
- [ ] Performance testing — Phase 7

**Deliverables:** Dynamic job loading fully operational. Uploaded JARs can be loaded, executed, and
unloaded without service restart. `DynamicJobClassLoader` provides classloader isolation with
shared-package delegation. `DynamicJobLoaderService` owns the lifecycle with thread-safe
`ConcurrentHashMap` tracking. `PluginAutoLoader` restores loaded plugins after restart. 4 PRs merged
via stacked-to-main chain (PR #35 → #36 → #37 → #38), 26/26 tasks complete, 80/80 tests passing.

**Bug discovered:** `JdbcJobExecutionDao.findRunningJobExecutions()` in Spring Batch 6.0.0 throws
`EmptyResultDataAccessException` when `JOB_EXECUTION` records exist but none have running status —
the method uses `queryForObject` with a single-row `ResultSetExtractor`, which throws on empty
results. Fixed in `DynamicJobLoaderService.unloadJob()` with try-catch guard. See commit `6120f87`.

### Phase 5: Security and Hardening ✅ COMPLETE (May 2026)

- [x] **ChecksumUtil** — Extracted SHA-256 computation from `JarUploadService` into reusable static
  utility with `File`, `Path`, and `MultipartFile` overloads; added checksum re-verification at load
  time in `DynamicJobLoaderService` (mismatch → `loadStatus=FAILED`,
  `loadError="Checksum mismatch"`)
- [x] **Spring Security baseline** — Added `spring-boot-starter-security`; `SecurityConfig` with
  HTTP Basic auth, role-based access (`PLUGIN_ADMIN` = all endpoints, `PLUGIN_VIEWER` = GET only),
  CSRF disabled; in-memory users via `SecurityProperties`; `GlobalExceptionHandler` for 401/403;
  security disabled in `test` profile via `@Profile("!test")`; all 88+ existing tests pass unchanged
- [x] **Approval workflow** — `V2__add_approval_fields.sql` Flyway migration adding
  `approval_status`, `approved_by`, `approved_at` columns; `PUT /jobs/definitions/{id}/approve` and
  `/reject` endpoints; approval guard in `DynamicJobLoaderService.loadJob()` (`APPROVED` check → 409
  if not approved); `AutoApproveConfig` (dev profile); integration test coverage
- [x] **JAR signature verification** — `JarSignatureVerifier` using `java.util.jar.JarFile` with
  strict/permissive modes (`app.plugins.signature.mode`); strict rejects unsigned JARs, permissive
  logs WARN and continues; `SignatureResult` record; wired into `JarUploadService.uploadJar()`; 10
  unit tests
- [x] **Audit service + JobExecutionListener** — `AuditEventType` enum (8 lifecycle events),
  `AuditEvent` record, `AuditService` with SLF4J structured JSON logging + MDC correlation IDs;
  `JobExecutionAuditListener` populates `job_executions_audit` on job start/end (attached to every
  job at registration since #111, the table's only writer); `createdBy`
  populated via `SecurityContext` resolver with `"system"` fallback; wired into `JarUploadService`,
  `DynamicJobLoaderService`, `JobManagementController`
- [x] **8 Snyk CVEs resolved** — `spring-boot-starter-security` fixed `CVE-2024-38807` (DoS in
  Webflux token validation), `CVE-2024-38811` (OIDC session fixation), `CVE-2024-38813` (H2 Console
  XSS), `CVE-2024-38814` (OAuth2 client state parameter), `CVE-2024-38816` (Spring Web resource
  directory traversal), `CVE-2024-38819` (Reactive `WebClient` memory leak), `CVE-2024-38821`
  (Spring WebFlux `@RequestBody` validation bypass), and `CVE-2024-38825` (SSRF via
  `RestTemplate`/`WebClient`)
- [x] **99/99 tests passing**, `mvn test` BUILD SUCCESS

**Implementation details**: Layered defense-in-depth — transport auth (HTTP Basic) → integrity
(checksum re-verify + JAR signature) → authorization (approval guard). Each layer fails closed. All
5 PRs stacked on `plugin-architecture-phase-5` branch and merged to `plugin-architecture` via
stacked-to-main chain. This completes Phase 5. See PRs #39, #40, #41, #42, #43.

### Phase 6: Example, Documentation, Health & Metrics ✅ COMPLETE (May 2026)

- [x] **PluginHealthIndicator** — `@Component` implementing `HealthIndicator`; queries
  `PluginRegistryService.getPlugins().size()`,
  `JobDefinitionRepository.countByLoadStatus("FAILED")`, `Path.of(jarDir).toFile().canRead()`;
  returns DOWN if failed > 0 or dir inaccessible; exposed at `/actuator/health/plugins` via
  `management.endpoint.health.group.plugins.include=pluginHealthIndicator`
- [x] **PluginMetrics** — `@Component` with `@PostConstruct` registering Micrometer counters
  (`plugins_uploaded_total`, `plugins_loaded_total`, `plugins_unloaded_total`), gauge
  (`plugins_loaded`), timers (`plugins_load_duration_seconds`, `plugins_unload_duration_seconds`);
  tagged `plugins_executed_total` counter with `job_name` tag; injected into `JarUploadService`,
  `DynamicJobLoaderService`, `JobsManagerService`
- [x] **Service instrumentation** — `JarUploadService.uploadJar()` calls
  `pluginMetrics.incrementUpload()` after DB persist; `DynamicJobLoaderService` calls
  `incrementLoad()` + `recordLoadDuration()` on load, `incrementUnload()` + `recordUnloadDuration()`
  on unload; `JobsManagerService.syncRunJobWithParams()` calls
  `pluginMetrics.incrementExecuted(request.getJobBeanName())`
- [x] **Example plugin project** — `example-payment-job/` standalone Maven module at repo root;
  `PaymentJobPlugin` implementing `BatchJobPlugin` with CSV-reading tasklet
  (`data/sample-payments.csv`); SPI file `META-INF/services/com.fronzec.api.BatchJobPlugin`;
  `provided` scope for `batch-job-api:0.0.1-SNAPSHOT`, Spring Batch 6.0.0, Spring Framework 7.0.1,
  SLF4J 2.0.16; maven-shade-plugin 3.6.0 for platform dep shading; README with
  build/upload/load/execute/unload instructions
- [x] **Developer guide rewrite** — `fr-batch-service/docs/job-plugins/developer-guide.md` now
  covers: lifecycle ASCII diagram (upload→approve→load→execute→unload→audit), REST API reference,
  JAR signing config, approval workflow, security roles matrix, health/metrics endpoints, 7-entry
  troubleshooting FAQ
- [x] **Production checklist** — `fr-batch-service/docs/production-checklist.md` created;
  checkbox-based checklist covering security, storage, monitoring, scaling, approval workflow,
  disaster recovery, pre-deployment validation, operational runbooks, final sign-off
- [x] **120/120 tests passing**, `mvn test` BUILD SUCCESS
- [x] **Spring Boot 4.0 adaptation**: Health classes use
  `org.springframework.boot.health.contributor.*` (package migrated from
  `org.springframework.boot.actuate.health`); `TestRestTemplate` removed (use direct bean injection
  in integration tests)
- [x] **Version corrections**: Developer guide updated to reflect Spring Batch 6.0.0 and Spring
  Framework 7.1.0 (transitive from Spring Boot 4.0.0 parent POM)

**Implementation details**: Four independent deliverables — health indicator, metrics, example
plugin, documentation — all additive with no API or schema changes. Health and metrics leverage
existing Spring Boot Actuator infrastructure. Example plugin mirrors `DynamicTestPlugin` structure
but as a standalone Maven module. 2 PRs stacked on `plugin-architecture-phase-6` branch and merged
via stacked-to-main chain. See PRs #44, #45.

### Phase 7: Production Deployment (Future)

- [ ] Deploy to staging environment
- [ ] Load testing
- [ ] Operational runbook
- [ ] Monitoring dashboards
- [ ] Training for operations team
- [ ] Production deployment

## Success Metrics

### Phase 1-2 Achieved (May 2026)

- [x] Job registration decoupled from `JobsManagerService` — no more hardcoded map
- [x] Any `@Component` implementing `BatchJobPlugin` is auto-discovered at startup
- [x] `batch-job-api` published to GitHub Packages
- [x] Integration test asserts `ExitStatus.COMPLETED` for full ETL pipeline
- [x] 5/5 tests passing, BUILD SUCCESS
- [x] Zero production code changes needed to add a new plugin (just add a `@Component`)

### Technical Metrics (for future phases)

- [ ] Job load time < 5 seconds (dynamic loading)
- [ ] Memory overhead per loaded job < 50MB
- [ ] Zero memory leaks after 100 load/unload cycles
- [ ] API response time < 100ms (95th percentile)
- [ ] Support for 50+ concurrent loaded jobs

### Business Metrics

- [x] Reduce time to deploy new job from days to hours — **achieved**: new job = new `@Component`,
  no redeploy architecture
- [ ] Enable 3+ teams to develop jobs independently — **partial**: contract exists, but plugins
  still live in same repo
- [ ] 90% of new jobs deployed without platform changes — **partial**: no platform changes for
  classpath plugins; external JARs deferred
- [ ] Zero production incidents due to job loading — **not yet in production**

## Open Questions

1. **Job Versioning Strategy**: Should we support running multiple versions simultaneously?

   - **Decision Needed**: Define versioning policy (semantic versioning required?)

1. **Job Dependency Management**: How to handle shared job libraries?

   - **Options**: Shared classloader, parent context beans, explicit dependencies

1. **Job Approval Process**: Should all uploaded jobs require approval before enabling?

   - **Options**: Auto-approve for trusted teams, manual approval for all, approval based on
     environment

1. **Storage Strategy**: Where to store JAR files long-term?

   - **Options**: Local filesystem, S3, NFS, Artifactory

1. **Hot Reload Impact**: Should running job instances be stopped when reloading?

   - **Options**: Graceful drain, force stop, wait for completion

1. **Multi-instance Deployment**: How to handle job loading across multiple service instances?

   - **Options**: Shared storage + database coordination, leader election, manual sync

## References

### Related Documentation

- [Spring Batch Documentation](https://docs.spring.io/spring-batch/docs/current/reference/html/)
- [Java ClassLoader Documentation](https://docs.oracle.com/javase/8/docs/api/java/lang/ClassLoader.html)
- [Spring Boot DevTools (hot reload reference)](https://docs.spring.io/spring-boot/docs/current/reference/html/using.html#using.devtools)
- [OSGi Dynamic Module System](https://www.osgi.org/developer/architecture/) (inspiration)

### Similar Implementations

- Jenkins Plugin System
- Elasticsearch Plugin Architecture
- Apache Kafka Connect Plugins
- SonarQube Plugin System

### Security References

- [OWASP - Deserialization of untrusted data](https://owasp.org/www-community/vulnerabilities/Deserialization_of_untrusted_data)
- [Java Security Manager (deprecated but concepts relevant)](https://docs.oracle.com/javase/tutorial/essential/environment/security.html)

## Decision Makers

- **Architect**: [Name TBD]
- **Tech Lead**: [Name TBD]
- **Security Lead**: [Name TBD]
- **Product Owner**: [Name TBD]

## Review and Approval

| Role       | Name | Date | Status  |
| ---------- | ---- | ---- | ------- |
| Architect  | TBD  | TBD  | Pending |
| Security   | TBD  | TBD  | Pending |
| Operations | TBD  | TBD  | Pending |
| Product    | TBD  | TBD  | Pending |

______________________________________________________________________

**Last Updated**: 2026-05-31 (Phase 6 complete; example, documentation, health & metrics
operational) **Next Review**: Before starting Phase 7 (production deployment)

### Related PRs

- [#27](https://github.com/fronzec/spring-batch-projects/pull/27) — Phase 1: Foundation
  (batch-job-api, docs, schema)
- [#28](https://github.com/fronzec/spring-batch-projects/pull/28) — Phase 2: Plugin registry +
  Job1Plugin migration
- [#29](https://github.com/fronzec/spring-batch-projects/pull/29) — Phase 2: sc09-test-fixture
  (integration test strengthening + H2 fix)
- [#30](https://github.com/fronzec/spring-batch-projects/pull/30) — Phase 3 PR 1: Flyway + Job
  Registry JPA Layer
- [#32](https://github.com/fronzec/spring-batch-projects/pull/32) — Phase 3 PR 3: CRUD Endpoints +
  Enable/Disable
- [#33](https://github.com/fronzec/spring-batch-projects/pull/33) — Phase 3 PR 4:
  PluginRegistryService Extension + Merged API
- [#34](https://github.com/fronzec/spring-batch-projects/pull/34) — Phase 3 PR 5:
  GlobalExceptionHandler + Integration Tests (Capstone)
- [#35](https://github.com/fronzec/spring-batch-projects/pull/35) — Phase 4 PR 1:
  DynamicJobClassLoader + unit tests
- [#36](https://github.com/fronzec/spring-batch-projects/pull/36) — Phase 4 PR 2:
  DynamicJobLoaderService + lifecycle
- [#37](https://github.com/fronzec/spring-batch-projects/pull/37) — Phase 4 PR 3: Controller
  endpoints + PluginAutoLoader
- [#38](https://github.com/fronzec/spring-batch-projects/pull/38) — Phase 4 PR 4: Integration tests
  \+ backward compatibility (Capstone)
- [#39](https://github.com/fronzec/spring-batch-projects/pull/39) — Phase 5 PR 1: ChecksumUtil
  extraction + load re-verification
- [#40](https://github.com/fronzec/spring-batch-projects/pull/40) — Phase 5 PR 2: Spring Security
  baseline (HTTP Basic, role-based access)
- [#41](https://github.com/fronzec/spring-batch-projects/pull/41) — Phase 5 PR 3: Approval workflow
  \+ V2 Flyway migration
- [#42](https://github.com/fronzec/spring-batch-projects/pull/42) — Phase 5 PR 4: JAR signature
  verification (strict/permissive modes)
- [#43](https://github.com/fronzec/spring-batch-projects/pull/43) — Phase 5 PR 5: Audit service +
  JobExecutionListener
- [#44](https://github.com/fronzec/spring-batch-projects/pull/44) — Phase 6 PR 1:
  PluginHealthIndicator + PluginMetrics + service instrumentation
- [#45](https://github.com/fronzec/spring-batch-projects/pull/45) — Phase 6 PR 2: Example plugin
  project (example-payment-job/) + developer guide rewrite + production checklist
