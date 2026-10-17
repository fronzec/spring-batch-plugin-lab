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
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobExecutionAuditRepository;
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobParameterTemplateRepository;
import com.fronzec.frbatchservice.batchjobs.plugins.service.InvalidJarException;
import com.fronzec.frbatchservice.batchjobs.plugins.service.JarUploadService;
import com.fronzec.frbatchservice.batchjobs.plugins.service.JarUploadService.StoredJar;
import com.fronzec.frbatchservice.batchjobs.plugins.util.ChecksumUtil;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
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
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.multipart.MultipartFile;

/**
 * Orchestrates the full lifecycle of dynamically-loaded batch job plugins: load from JAR,
 * instantiate, configure, register, unload, replace the JAR, and delete.
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
  private final JobParameterTemplateRepository jobParameterTemplateRepository;
  private final JobExecutionAuditRepository jobExecutionAuditRepository;
  private final JarUploadService jarUploadService;

  /**
   * Runs the database part of {@link #deleteJob} (templates + definition) and of {@link
   * #replaceJar} (definition update), each as one transaction.
   */
  private final TransactionTemplate lifecycleTransaction;

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
      PluginMetrics pluginMetrics,
      JobParameterTemplateRepository jobParameterTemplateRepository,
      JobExecutionAuditRepository jobExecutionAuditRepository,
      JarUploadService jarUploadService) {
    this.jobDefinitionRepository = jobDefinitionRepository;
    this.pluginRegistryService = pluginRegistryService;
    this.applicationContext = applicationContext;
    this.jobRepository = jobRepository;
    this.transactionManager = transactionManager;
    this.auditService = auditService;
    this.pluginMetrics = pluginMetrics;
    this.jobParameterTemplateRepository = jobParameterTemplateRepository;
    this.jobExecutionAuditRepository = jobExecutionAuditRepository;
    this.jarUploadService = jarUploadService;
    this.lifecycleTransaction = new TransactionTemplate(transactionManager);
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
   * Deletes a job definition as one lifecycle-coordinated operation: unloads the plugin, deletes
   * its parameter templates and definition row in a single database transaction, and only after
   * that transaction commits removes the JAR file from disk.
   *
   * <p>The whole operation runs under the per-definition lifecycle lock, so it cannot interleave
   * with {@link #loadJob}, {@link #unloadJob}, or {@link #reloadJob} for the same ID. A definition
   * that is not currently loaded is deleted without error.
   *
   * <p>Before anything is unloaded, a definition with execution history in {@code
   * job_executions_audit} is rejected with {@link JobDeleteConflictException}; such definitions
   * should be disabled instead. {@code force=true} does not bypass this guard: it only skips the
   * running-execution guard, and it does not stop running executions.
   *
   * <p>A missing JAR file is not an error. If the JAR cannot be removed after commit, the delete
   * still succeeds, a warning is logged, and the {@code JOB_DELETED} success audit names the
   * orphan JAR path. Any failure up to and including the commit emits a {@code JOB_DELETED}
   * failure audit and propagates the original exception.
   *
   * @param definitionId the database ID of the job definition to delete
   * @param force if {@code true}, skip the running-execution guard
   * @throws NoSuchElementException if the definition does not exist
   * @throws JobDeleteConflictException if the definition has execution history, regardless of
   *     {@code force}; nothing is unloaded or deleted
   * @throws JobUnloadConflictException if the job has running executions and {@code force} is
   *     {@code false}; nothing is deleted
   * @throws org.springframework.dao.DataAccessException if the database delete fails; the
   *     transaction is rolled back and the JAR file is kept
   */
  public void deleteJob(Long definitionId, boolean force) {
    if (jobDefinitionRepository.findById(definitionId).isEmpty()) {
      throw new NoSuchElementException("Job definition not found for id: " + definitionId);
    }
    withDefinitionLock(definitionId, () -> doDeleteJob(definitionId, force));
  }

  /** Performs the actual delete while the caller owns the per-definition lifecycle lock. */
  private void doDeleteJob(Long definitionId, boolean force) {
    JobDefinitionEntity entity =
        jobDefinitionRepository
            .findById(definitionId)
            .orElseThrow(
                () ->
                    new NoSuchElementException(
                        "Job definition not found for id: " + definitionId));
    String jobName = entity.getJobName();
    String jarFilePath = entity.getJarFilePath();

    boolean unloaded = false;
    try {
      // History guard runs before unload so a rejected delete leaves the plugin untouched.
      // force only bypasses the running-execution guard, never this one.
      if (jobExecutionAuditRepository.existsByJobDefinitionId(definitionId)) {
        throw new JobDeleteConflictException(
            "Job definition \""
                + jobName
                + "\" (id="
                + definitionId
                + ") has execution history and cannot be deleted; disable it instead"
                + " (PUT /jobs/definitions/"
                + definitionId
                + "/disable). force=true does not bypass this check.");
      }
      doUnloadJob(definitionId, force);
      unloaded = true;
      // doUnloadJob persisted the entity as UNLOADED, so delete by id inside the transaction
      // instead of deleting the now-stale instance read above.
      lifecycleTransaction.executeWithoutResult(
          status -> {
            jobParameterTemplateRepository.deleteByJobDefinitionId(definitionId);
            jobDefinitionRepository.deleteById(definitionId);
          });
    } catch (RuntimeException e) {
      String outcome =
          unloaded
              ? "The plugin was unloaded but the definition, its parameter templates, and its"
                  + " JAR were kept."
              : "Nothing was deleted.";
      log.error(
          "Failed to delete job definition id={} jobName='{}' force={}: {}",
          definitionId,
          jobName,
          force,
          e.getMessage(),
          e);
      auditService.logEvent(
          new AuditEvent(
              AuditEventType.JOB_DELETED,
              jobName,
              AuditService.currentUserId(),
              "Delete failed for job definition id="
                  + definitionId
                  + " (force="
                  + force
                  + "): "
                  + e.getMessage()
                  + ". "
                  + outcome,
              AuditEvent.FAILURE,
              LocalDateTime.now()));
      throw e;
    }

    String jarOutcome = deleteJarAfterCommit(definitionId, jarFilePath);
    log.info("Job definition deleted: id={}, jobName='{}', {}", definitionId, jobName, jarOutcome);
    auditService.logEvent(
        new AuditEvent(
            AuditEventType.JOB_DELETED,
            jobName,
            AuditService.currentUserId(),
            "Job definition deleted: id=" + definitionId + " (force=" + force + "); " + jarOutcome,
            AuditEvent.SUCCESS,
            LocalDateTime.now()));
  }

  /**
   * Removes a JAR file that no definition references any more, after the delete or replace
   * transaction committed. File-system problems never fail the operation: the database change is
   * already committed, so the JAR is reported as an orphan instead.
   *
   * @return a human-readable JAR outcome for the success audit detail
   */
  private String deleteJarAfterCommit(Long definitionId, String jarFilePath) {
    if (jarFilePath == null || jarFilePath.isBlank()) {
      log.warn("Job definition id={} had no JAR path; nothing to remove from disk", definitionId);
      return "no JAR path recorded";
    }
    try {
      if (Files.deleteIfExists(Path.of(jarFilePath))) {
        log.info("JAR file deleted from disk: {}", jarFilePath);
        return "JAR deleted: " + jarFilePath;
      }
      log.info("JAR file already absent from disk: {}", jarFilePath);
      return "JAR already absent: " + jarFilePath;
    } catch (IOException | InvalidPathException | SecurityException e) {
      log.warn(
          "Job definition id={} committed but its unreferenced JAR could not be removed; orphan"
              + " JAR left on disk: {} ({})",
          definitionId,
          jarFilePath,
          e.toString());
      return "orphan JAR left on disk: " + jarFilePath + " (" + e + ")";
    }
  }

  /**
   * Replaces the JAR of an existing job definition in place, keeping its ID so execution history
   * and parameter templates stay linked.
   *
   * <p>The whole operation runs under the per-definition lifecycle lock. It first unloads the
   * plugin without force, so running executions reject the replacement with {@link
   * JobUnloadConflictException} and nothing changes. It then validates and stores the new JAR
   * through {@link JarUploadService#storeReplacementJar} under a unique file name, so the current
   * JAR is never overwritten. One database transaction updates the JAR path, checksum, version,
   * and main class, resets the approval to {@code PENDING} (clearing the approver), and keeps the
   * definition {@code UNLOADED}; {@code enabled} is unchanged. After commit, the previous JAR is
   * deleted and then the non-production auto-approval is applied as on upload.
   *
   * <p>A missing previous JAR is not an error. If it cannot be removed after commit, the
   * replacement still succeeds, a warning is logged, and the {@code JAR_REPLACED} success audit
   * names the orphan JAR path. Any failure before or at commit deletes the newly stored JAR (best
   * effort), emits a {@code JAR_REPLACED} failure audit, and propagates the original exception;
   * the definition keeps its previous JAR and metadata but stays unloaded when the failure
   * happened after the unload.
   *
   * @param definitionId the database ID of the job definition whose JAR is replaced
   * @param file the replacement JAR
   * @param version the version of the replacement
   * @param mainClassName fully-qualified class name of the {@code BatchJobPlugin} implementation
   * @return the updated definition
   * @throws NoSuchElementException if the definition does not exist
   * @throws JobUnloadConflictException if the job has running executions; nothing is changed
   * @throws InvalidJarException if the replacement JAR fails validation; the definition is kept
   * @throws org.springframework.dao.DataAccessException if the database update fails; the
   *     transaction is rolled back, the new JAR is deleted, and the previous JAR is kept
   */
  public JobDefinitionEntity replaceJar(
      Long definitionId, MultipartFile file, String version, String mainClassName) {
    if (jobDefinitionRepository.findById(definitionId).isEmpty()) {
      throw new NoSuchElementException("Job definition not found for id: " + definitionId);
    }
    return withDefinitionLock(
        definitionId, () -> doReplaceJar(definitionId, file, version, mainClassName));
  }

  /** Performs the actual replacement while the caller owns the per-definition lifecycle lock. */
  private JobDefinitionEntity doReplaceJar(
      Long definitionId, MultipartFile file, String version, String mainClassName) {
    JobDefinitionEntity entity =
        jobDefinitionRepository
            .findById(definitionId)
            .orElseThrow(
                () ->
                    new NoSuchElementException(
                        "Job definition not found for id: " + definitionId));
    String jobName = entity.getJobName();
    String previousJarPath = entity.getJarFilePath();
    String previousVersion = entity.getVersion();

    boolean unloaded = false;
    StoredJar storedJar = null;
    JobDefinitionEntity updated;
    try {
      doUnloadJob(definitionId, false);
      unloaded = true;
      storedJar = jarUploadService.storeReplacementJar(file, jobName, version, mainClassName);
      StoredJar newJar = storedJar;
      updated =
          lifecycleTransaction.execute(
              status -> applyReplacement(definitionId, newJar, version, mainClassName));
    } catch (RuntimeException e) {
      String outcome;
      if (!unloaded) {
        outcome = "Nothing was changed.";
      } else {
        String newJarOutcome =
            storedJar == null ? "no new JAR was stored" : discardStoredJar(storedJar.path(), e);
        outcome =
            "The plugin was unloaded and stays unloaded; the definition and its current JAR "
                + previousJarPath
                + " were kept; "
                + newJarOutcome
                + ".";
      }
      log.error(
          "Failed to replace JAR of job definition id={} jobName='{}' version={}: {}",
          definitionId,
          jobName,
          version,
          e.getMessage(),
          e);
      auditService.logEvent(
          new AuditEvent(
              AuditEventType.JAR_REPLACED,
              jobName,
              AuditService.currentUserId(),
              "Replace JAR failed for job definition id="
                  + definitionId
                  + " (version "
                  + version
                  + "): "
                  + e.getMessage()
                  + ". "
                  + outcome,
              AuditEvent.FAILURE,
              LocalDateTime.now()));
      throw e;
    }

    String previousJarOutcome =
        storedJar.path().toString().equals(previousJarPath)
            ? "previous JAR path unchanged"
            : deleteJarAfterCommit(definitionId, previousJarPath);
    String approvalOutcome = autoApproveAfterCommit(updated);
    log.info(
        "JAR replaced: id={}, jobName='{}', version {} -> {}, jar={}, {}; previous {}",
        definitionId,
        jobName,
        previousVersion,
        version,
        updated.getJarFilePath(),
        approvalOutcome,
        previousJarOutcome);
    auditService.logEvent(
        new AuditEvent(
            AuditEventType.JAR_REPLACED,
            jobName,
            AuditService.currentUserId(),
            "JAR replaced for job definition id="
                + definitionId
                + ": version "
                + previousVersion
                + " -> "
                + version
                + ", jar "
                + updated.getJarFilePath()
                + " (checksum "
                + updated.getJarChecksum()
                + "), "
                + approvalOutcome
                + "; previous "
                + previousJarOutcome,
            AuditEvent.SUCCESS,
            LocalDateTime.now()));
    return updated;
  }

  /**
   * Points the definition at the replacement JAR and resets its approval. Runs inside the replace
   * transaction; re-reads the definition because {@link #doUnloadJob} already saved it.
   */
  private JobDefinitionEntity applyReplacement(
      Long definitionId, StoredJar newJar, String version, String mainClassName) {
    JobDefinitionEntity current =
        jobDefinitionRepository
            .findById(definitionId)
            .orElseThrow(
                () ->
                    new NoSuchElementException(
                        "Job definition not found for id: " + definitionId));
    current.setJarFilePath(newJar.path().toString());
    current.setJarChecksum(newJar.checksum());
    current.setVersion(version);
    current.setMainClassName(mainClassName);
    current.setApprovalStatus("PENDING");
    current.setApprovedBy(null);
    current.setApprovedAt(null);
    current.setLoadStatus(LoadResult.UNLOADED);
    current.setLoadError(null);
    return jobDefinitionRepository.save(current);
  }

  /**
   * Applies the non-production auto-approval after the replace transaction committed. A failure
   * does not fail the replacement: the definition keeps its committed {@code PENDING} approval.
   *
   * @return a human-readable approval outcome for the success audit detail
   */
  private String autoApproveAfterCommit(JobDefinitionEntity updated) {
    try {
      return jarUploadService.autoApproveIfEnabled(updated)
          ? "auto-approved (non-production)"
          : "approval reset to PENDING";
    } catch (RuntimeException e) {
      // The in-memory entity may already carry the approval; mirror the committed state.
      updated.setApprovalStatus("PENDING");
      updated.setApprovedBy(null);
      updated.setApprovedAt(null);
      log.warn(
          "Job definition id={} JAR replaced but auto-approval failed; approval stays PENDING: {}",
          updated.getId(),
          e.toString());
      return "auto-approval failed, approval stays PENDING (" + e + ")";
    }
  }

  /**
   * Deletes a replacement JAR that was stored but never committed as the definition's JAR.
   *
   * @return a human-readable outcome for the failure audit detail
   */
  private String discardStoredJar(Path storedJar, RuntimeException failure) {
    try {
      Files.deleteIfExists(storedJar);
      return "the new JAR " + storedJar + " was deleted";
    } catch (IOException | SecurityException e) {
      failure.addSuppressed(e);
      log.warn(
          "Could not delete uncommitted replacement JAR; orphan JAR left on disk: {} ({})",
          storedJar,
          e.toString());
      return "the new JAR could not be deleted; orphan JAR left on disk: " + storedJar;
    }
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
