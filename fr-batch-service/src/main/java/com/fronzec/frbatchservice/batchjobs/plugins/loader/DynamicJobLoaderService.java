/* 2024-2026 */
package com.fronzec.frbatchservice.batchjobs.plugins.loader;

import com.fronzec.api.BatchJobPlugin;
import com.fronzec.frbatchservice.batchjobs.plugins.PluginRegistryService;
import com.fronzec.frbatchservice.batchjobs.plugins.audit.AuditEvent;
import com.fronzec.frbatchservice.batchjobs.plugins.audit.AuditEventType;
import com.fronzec.frbatchservice.batchjobs.plugins.audit.AuditService;
import com.fronzec.frbatchservice.batchjobs.plugins.entity.JobDefinitionEntity;
import com.fronzec.frbatchservice.batchjobs.plugins.metrics.PluginMetrics;
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobDefinitionRepository;
import com.fronzec.frbatchservice.batchjobs.plugins.util.ChecksumUtil;
import java.io.File;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.ApplicationContext;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Orchestrates the full lifecycle of dynamically-loaded batch job plugins: load from JAR,
 * instantiate, configure, register, and unload.
 *
 * <p>Owns the {@link DynamicJobClassLoader} lifecycle via a {@link ConcurrentHashMap} keyed by
 * definition ID. Delegates registry operations to {@link PluginRegistryService}.
 */
@Service
public class DynamicJobLoaderService {

  private static final Logger log = LoggerFactory.getLogger(DynamicJobLoaderService.class);

  private final JobDefinitionRepository jobDefinitionRepository;
  private final PluginRegistryService pluginRegistryService;
  private final ApplicationContext applicationContext;
  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;
  private final AuditService auditService;
  private final PluginMetrics pluginMetrics;

  /** Tracks active classloaders by definition ID for cleanup on unload. */
  private final Map<Long, DynamicJobClassLoader> classLoaders = new ConcurrentHashMap<>();

  /**
   * Per-definition lifecycle locks. Serialize concurrent load, unload, and reload calls for the
   * same definition ID so registry and classloader transitions execute atomically. A holder is
   * allocated only after confirming the definition exists, so unknown IDs cannot grow this map
   * without bound. This coordination is limited to calls in this service instance; it does not
   * provide distributed lifecycle locking.
   */
  private final Map<Long, LifecycleLockHolder> lifecycleLocks = new ConcurrentHashMap<>();

  public DynamicJobLoaderService(
      JobDefinitionRepository jobDefinitionRepository,
      PluginRegistryService pluginRegistryService,
      ApplicationContext applicationContext,
      JobRepository jobRepository,
      PlatformTransactionManager transactionManager,
      AuditService auditService,
      PluginMetrics pluginMetrics) {
    this.jobDefinitionRepository = jobDefinitionRepository;
    this.pluginRegistryService = pluginRegistryService;
    this.applicationContext = applicationContext;
    this.jobRepository = jobRepository;
    this.transactionManager = transactionManager;
    this.auditService = auditService;
    this.pluginMetrics = pluginMetrics;
  }

  /**
   * Loads a job definition from its JAR, instantiates the plugin, configures the Spring Batch
   * {@link org.springframework.batch.core.job.Job}, and registers it with the registry.
   *
   * @param definitionId the database ID of the job definition to load
   * @return a {@link LoadResult} describing the outcome
   * @throws NoSuchElementException if the definition does not exist
   * @throws IllegalStateException if the definition is disabled or already loaded
   * @throws JobLoadException if the load fails at any stage (bad JAR, class missing, configure
   *     failure, etc.)
   */
  public LoadResult loadJob(Long definitionId) {
    // Gate lock allocation on existence so unknown IDs (e.g. POST /definitions/{id}/load with a
    // bogus id) cannot grow lifecycleLocks without bound. The authoritative existence check still runs
    // inside doLoadJob under the lock, guarding against a definition deleted between the two reads.
    if (jobDefinitionRepository.findById(definitionId).isEmpty()) {
      throw new NoSuchElementException("Job definition not found for id: " + definitionId);
    }
    return withDefinitionLock(definitionId, () -> doLoadJob(definitionId));
  }

  <T> T withDefinitionLock(Long definitionId, Supplier<T> action) {
    LifecycleLockHolder holder =
        lifecycleLocks.compute(
            definitionId,
            (id, current) -> {
              LifecycleLockHolder selected = current == null ? new LifecycleLockHolder() : current;
              selected.users++;
              return selected;
            });
    holder.lock.lock();
    try {
      return action.get();
    } finally {
      holder.lock.unlock();
      lifecycleLocks.computeIfPresent(
          definitionId,
          (id, current) -> --current.users == 0 ? null : current);
    }
  }

  void withDefinitionLock(Long definitionId, Runnable action) {
    withDefinitionLock(
        definitionId,
        () -> {
          action.run();
          return null;
        });
  }

  private static final class LifecycleLockHolder {
    private final ReentrantLock lock = new ReentrantLock();
    private int users;
  }

  /**
   * Performs the actual load while the caller owns the per-definition lifecycle lock.
   */
  private LoadResult doLoadJob(Long definitionId) {
    JobDefinitionEntity entity =
        jobDefinitionRepository
            .findById(definitionId)
            .orElseThrow(
                () ->
                    new NoSuchElementException(
                        "Job definition not found for id: " + definitionId));

    // Pre-condition: must be enabled
    if (!Boolean.TRUE.equals(entity.getEnabled())) {
      throw new IllegalStateException(
          "Job definition \"" + entity.getJobName() + "\" (id=" + definitionId + ") is disabled");
    }

    // Approval guard: only APPROVED definitions can be loaded
    if (!"APPROVED".equals(entity.getApprovalStatus())) {
      throw new IllegalStateException(
          "Job definition \""
              + entity.getJobName()
              + "\" (id="
              + definitionId
              + ") must be approved before loading. Current status: "
              + entity.getApprovalStatus());
    }

    // Optimistic guard: already loaded in THIS JVM's registry. The persisted
    // loadStatus can be a stale LOADED after a restart (the registry is
    // in-memory and resets), so consult the live registry, not the DB column.
    if (pluginRegistryService.getRegisteredJobNames().contains(entity.getJobName())) {
      throw new IllegalStateException(
          "Job \"" + entity.getJobName() + "\" (id=" + definitionId + ") is already loaded");
    }

    long loadStartNanos = System.nanoTime();
    DynamicJobClassLoader classLoader = null;
    boolean registered = false;
    try {
      // Mark LOADING so concurrent attempts see in-flight state. Persisting it inside the
      // failure boundary guarantees a failed save still ends in FAILED with its original cause.
      entity.setLoadStatus("LOADING");
      entity.setLoadError(null);
      jobDefinitionRepository.save(entity);

      // Validate JAR file exists
      File jarFile = new File(entity.getJarFilePath());
      if (!jarFile.exists()) {
        throw new JobLoadException(
            "JAR file not found: " + entity.getJarFilePath());
      }

      // Re-verify checksum: detect tampering that occurred after upload
      String storedChecksum = entity.getJarChecksum();
      String actualChecksum = ChecksumUtil.computeSha256(jarFile.toPath());
      if (!actualChecksum.equals(storedChecksum)) {
        entity.setLoadStatus(LoadResult.FAILED);
        entity.setLoadError(
            "Checksum mismatch: stored=" + storedChecksum + " actual=" + actualChecksum);
        jobDefinitionRepository.save(entity);
        throw new JobLoadException(
            "Checksum mismatch for job \""
                + entity.getJobName()
                + "\": stored="
                + storedChecksum
                + " actual="
                + actualChecksum);
      }

      // Create parent-last classloader
      URL[] jarUrls = new URL[] {jarFile.toURI().toURL()};
      classLoader =
          new DynamicJobClassLoader(jarUrls, getClass().getClassLoader(), entity.getJobName());

      // Load the plugin main class
      Class<?> pluginClass = classLoader.loadClass(entity.getMainClassName());

      // Validate it implements BatchJobPlugin
      if (!BatchJobPlugin.class.isAssignableFrom(pluginClass)) {
        throw new JobLoadException(
            "Main class "
                + entity.getMainClassName()
                + " does not implement BatchJobPlugin");
      }

      // Instantiate and verify
      BatchJobPlugin plugin = (BatchJobPlugin) pluginClass.getDeclaredConstructor().newInstance();

      if (!plugin.getJobName().equals(entity.getJobName())) {
        throw new JobLoadException(
            "Plugin declares jobName=\""
                + plugin.getJobName()
                + "\" but definition expects \""
                + entity.getJobName()
                + "\"");
      }

      // Register through existing hook — registerDynamicPlugin() internally calls
      // plugin.configureJob(...), validates the Job, and registers in JobRegistry
      pluginRegistryService.registerDynamicPlugin(plugin);
      registered = true;

      // Track classloader for future unload
      classLoaders.put(definitionId, classLoader);

      // Persist success
      entity.setLoadStatus(LoadResult.LOADED);
      entity.setLoadError(null);
      jobDefinitionRepository.save(entity);

      log.info(
          "Loaded plugin '{}' (id={}) from {}",
          entity.getJobName(),
          definitionId,
          entity.getJarFilePath());

      auditService.logEvent(
          new AuditEvent(
              AuditEventType.JOB_LOADED,
              entity.getJobName(),
              AuditService.currentUserId(),
              "Job loaded from " + entity.getJarFilePath(),
              AuditEvent.SUCCESS,
              LocalDateTime.now()));

      long durationMs =
          java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
              System.nanoTime() - loadStartNanos);
      pluginMetrics.incrementLoad();
      pluginMetrics.recordLoadDuration(durationMs);

      return new LoadResult(
          entity.getJobName(), LoadResult.LOADED, "Successfully loaded");

    } catch (Exception e) {
      boolean canReleaseClassLoader = true;
      if (registered) {
        try {
          pluginRegistryService.unregisterDynamicPlugin(entity.getJobName());
        } catch (Exception rollbackEx) {
          canReleaseClassLoader = false;
          e.addSuppressed(rollbackEx);
          log.error(
              "Failed to roll back plugin registration for definition {}: {}",
              definitionId,
              rollbackEx.getMessage(),
              rollbackEx);
        }
      }

      if (canReleaseClassLoader) {
        DynamicJobClassLoader ownedClassLoader = classLoaders.remove(definitionId);
        DynamicJobClassLoader classLoaderToClean =
            ownedClassLoader != null ? ownedClassLoader : classLoader;
        if (classLoaderToClean != null) {
          try {
            classLoaderToClean.cleanup();
          } catch (Exception cleanupEx) {
            e.addSuppressed(cleanupEx);
            log.warn(
                "Failed to clean up classloader for definition {}: {}",
                definitionId,
                cleanupEx.getMessage(),
                cleanupEx);
          }
        }
      }

      entity.setLoadStatus(LoadResult.FAILED);
      entity.setLoadError(e.getMessage());
      try {
        jobDefinitionRepository.save(entity);
      } catch (Exception persistenceEx) {
        e.addSuppressed(persistenceEx);
        log.error(
            "Failed to persist FAILED status for definition {}: {}",
            definitionId,
            persistenceEx.getMessage(),
            persistenceEx);
      }

      log.error("Failed to load plugin for definition {}: {}", definitionId, e.getMessage(), e);

      auditService.logEvent(
          new AuditEvent(
              AuditEventType.JOB_LOADED,
              entity.getJobName(),
              AuditService.currentUserId(),
              "Load failed: " + e.getMessage(),
              AuditEvent.FAILURE,
              LocalDateTime.now()));

      if (e instanceof JobLoadException) {
        throw (JobLoadException) e;
      }
      throw new JobLoadException(
          "Failed to load job \"" + entity.getJobName() + "\": " + e.getMessage(), e);
    }
  }

  /**
   * Unloads a previously-loaded job: unregisters from the registry, closes the classloader, and
   * updates the database status.
   *
   * @param definitionId the database ID of the job definition to unload
   * @param force if {@code true}, skip the running-execution guard
   * @return a {@link LoadResult} describing the outcome
   * @throws NoSuchElementException if the definition does not exist
   * @throws JobUnloadConflictException if the job has running executions and {@code force} is
   *     {@code false}
   */
  public LoadResult unloadJob(Long definitionId, boolean force) {
    if (jobDefinitionRepository.findById(definitionId).isEmpty()) {
      throw new NoSuchElementException("Job definition not found for id: " + definitionId);
    }
    return withDefinitionLock(definitionId, () -> doUnloadJob(definitionId, force));
  }

  /** Performs the actual unload while the caller owns the per-definition lifecycle lock. */
  private LoadResult doUnloadJob(Long definitionId, boolean force) {
    long unloadStartNanos = System.nanoTime();

    JobDefinitionEntity entity =
        jobDefinitionRepository
            .findById(definitionId)
            .orElseThrow(
                () ->
                    new NoSuchElementException(
                        "Job definition not found for id: " + definitionId));

    // Guard: check for running executions unless forced
    if (!force) {
      // TODO: integrate JobExplorer for precise running-execution check.
      // Currently uses JobRepository.findRunningJobExecutions() which may not be exhaustive.
      java.util.Set<?> runningExecutions;
      try {
        runningExecutions =
            jobRepository.findRunningJobExecutions(entity.getJobName());
      } catch (EmptyResultDataAccessException e) {
        // Spring Batch 6.0.0 JdbcJobExecutionDao uses queryForObject which throws
        // when there are non-running executions but zero running ones. Treat as
        // "no running executions".
        log.debug(
            "findRunningJobExecutions returned empty for '{}': {}",
            entity.getJobName(),
            e.getMessage());
        runningExecutions = Collections.emptySet();
      }
      if (runningExecutions != null && !runningExecutions.isEmpty()) {
        throw new JobUnloadConflictException(
            "Job \""
                + entity.getJobName()
                + "\" has "
                + runningExecutions.size()
                + " running execution(s). Use force=true to override.");
      }
    }

    // Unregister from the plugin registry and JobRegistry
    pluginRegistryService.unregisterDynamicPlugin(entity.getJobName());

    // Close and remove classloader
    DynamicJobClassLoader classLoader = classLoaders.remove(definitionId);
    if (classLoader != null) {
      classLoader.cleanup();
    } else {
      log.warn(
          "No classloader found for definition {} — may not have been loaded through this service",
          definitionId);
    }

    // Persist unloaded state
    entity.setLoadStatus(LoadResult.UNLOADED);
    entity.setLoadError(null);
    jobDefinitionRepository.save(entity);

    log.info(
        "Unloaded plugin '{}' (id={})", entity.getJobName(), definitionId);

    auditService.logEvent(
        new AuditEvent(
            AuditEventType.JOB_UNLOADED,
            entity.getJobName(),
            AuditService.currentUserId(),
            "Job unloaded (force=" + force + ")",
            AuditEvent.SUCCESS,
            LocalDateTime.now()));

    long durationMs =
        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
            System.nanoTime() - unloadStartNanos);
    pluginMetrics.incrementUnload();
    pluginMetrics.recordUnloadDuration(durationMs);

    return new LoadResult(
        entity.getJobName(), LoadResult.UNLOADED, "Successfully unloaded");
  }

  /**
   * Reloads a job by unloading (forced) then loading sequentially. The operation is sequential, so
   * the status never appears partially ready.
   *
   * @param definitionId the database ID of the job definition to reload
   * @return a {@link LoadResult} describing the outcome (always LOADED on success)
   * @throws NoSuchElementException if the definition does not exist
   * @throws JobLoadException if the load step fails
   */
  public LoadResult reloadJob(Long definitionId) {
    if (jobDefinitionRepository.findById(definitionId).isEmpty()) {
      throw new NoSuchElementException("Job definition not found for id: " + definitionId);
    }
    return withDefinitionLock(
        definitionId,
        () -> {
          doUnloadJob(definitionId, true);
          return doLoadJob(definitionId);
        });
  }

  /**
   * Loads all enabled job definitions whose {@code loadStatus} is not {@code LOADED}. Each
   * definition is loaded independently; one failure does not block the rest.
   *
   * @return a map from job name to the per-definition {@link LoadResult}
   */
  public Map<String, LoadResult> loadAllEnabled() {
    List<JobDefinitionEntity> enabledDefs = jobDefinitionRepository.findByEnabled(true);

    Map<String, LoadResult> results = new LinkedHashMap<>();

    for (JobDefinitionEntity def : enabledDefs) {
      // Skip definitions already loaded in THIS JVM's registry. After a restart
      // the persisted loadStatus may still say LOADED while the in-memory registry
      // is empty, so check the live registry — not the DB column — to avoid
      // skipping definitions that need re-instantiating.
      if (pluginRegistryService.getRegisteredJobNames().contains(def.getJobName())) {
        log.debug(
            "Skipping already-loaded definition '{}' (id={})",
            def.getJobName(),
            def.getId());
        results.put(
            def.getJobName(),
            new LoadResult(
                def.getJobName(), LoadResult.LOADED, "Already loaded"));
        continue;
      }

      try {
        LoadResult result = loadJob(def.getId());
        results.put(def.getJobName(), result);
      } catch (Exception e) {
        log.error(
            "Failed to auto-load definition '{}' (id={}): {}",
            def.getJobName(),
            def.getId(),
            e.getMessage());
        results.put(
            def.getJobName(),
            new LoadResult(
                def.getJobName(), LoadResult.FAILED, e.getMessage()));
      }
    }

    return results;
  }

}
