/* 2024-2026 */
package com.fronzec.frbatchservice.batchjobs.plugins.loader;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.fronzec.api.BatchJobPlugin;
import com.fronzec.api.JobMetadata;
import com.fronzec.frbatchservice.batchjobs.plugins.PluginRegistrationException;
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
import com.fronzec.frbatchservice.batchjobs.plugins.util.ChecksumUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.context.ApplicationContext;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.transaction.PlatformTransactionManager;

@ExtendWith(MockitoExtension.class)
class DynamicJobLoaderServiceTest {

  @Mock private JobDefinitionRepository jobDefinitionRepository;
  @Mock private PluginRegistryService pluginRegistryService;
  @Mock private ApplicationContext applicationContext;
  @Mock private JobRepository jobRepository;
  @Mock private PlatformTransactionManager transactionManager;
  @Mock private AuditService auditService;
  @Mock private PluginMetrics pluginMetrics;
  @Mock private JobParameterTemplateRepository jobParameterTemplateRepository;
  @Mock private JobExecutionAuditRepository jobExecutionAuditRepository;
  @Mock private JarUploadService jarUploadService;

  @InjectMocks private DynamicJobLoaderService service;

  private Path tempDir;
  private String validJarPath;
  private String validJarChecksum;

  // ── Setup / teardown ──────────────────────────────────────────────────────

  @BeforeEach
  void setUp() throws Exception {
    tempDir = Files.createTempDirectory("loader-service-test-");
    // Create a real (empty) file so JAR existence validation passes
    Path jarFile = tempDir.resolve("test-plugin.jar");
    Files.createFile(jarFile);
    validJarPath = jarFile.toString();
    validJarChecksum = ChecksumUtil.computeSha256(jarFile);
  }

  @AfterEach
  void tearDown() {
    if (tempDir != null) {
      try (var walk = Files.walk(tempDir)) {
        walk
            .sorted(Comparator.reverseOrder())
            .forEach(
                p -> {
                  try {
                    Files.deleteIfExists(p);
                  } catch (IOException ignored) {
                    // best-effort cleanup
                  }
                });
      } catch (IOException ignored) {
        // best-effort cleanup
      }
    }
  }

  // ── Helper: entity stub ───────────────────────────────────────────────────

  private JobDefinitionEntity createEntity(
      long id, String jobName, boolean enabled, String loadStatus) {
    JobDefinitionEntity entity = new JobDefinitionEntity();
    entity.setId(id);
    entity.setJobName(jobName);
    entity.setJarFilePath(validJarPath);
    entity.setJarChecksum(validJarChecksum);
    entity.setMainClassName("com.test.TestPlugin");
    entity.setEnabled(enabled);
    entity.setLoadStatus(loadStatus);
    entity.setApprovalStatus("APPROVED");
    return entity;
  }

  /** Reads the reference-counted lifecycle holder map for white-box assertions. */
  @SuppressWarnings("unchecked")
  private static Map<Long, ?> lifecycleLocksOf(DynamicJobLoaderService target) throws Exception {
    var field = DynamicJobLoaderService.class.getDeclaredField("lifecycleLocks");
    field.setAccessible(true);
    return (Map<Long, ?>) field.get(target);
  }

  @SuppressWarnings("unchecked")
  private static Map<Long, DynamicJobClassLoader> classLoadersOf(
      DynamicJobLoaderService target) throws Exception {
    var field = DynamicJobLoaderService.class.getDeclaredField("classLoaders");
    field.setAccessible(true);
    return (Map<Long, DynamicJobClassLoader>) field.get(target);
  }

  // ── Successful load ───────────────────────────────────────────────────────

  @Test
  void loadJob_validPlugin_registersAndReturnsLoaded() throws Exception {
    JobDefinitionEntity entity = createEntity(1L, "test-job", true, null);

    when(jobDefinitionRepository.findById(1L)).thenReturn(Optional.of(entity));
    doNothing()
        .when(pluginRegistryService)
        .registerDynamicPlugin(any(BatchJobPlugin.class));

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, ctx) -> {
              when(mock.loadClass("com.test.TestPlugin"))
                  .thenAnswer(inv -> TestBatchJobPlugin.class);
            })) {

      LoadResult result = service.loadJob(1L);

      assertEquals("test-job", result.jobName());
      assertEquals(LoadResult.LOADED, result.status());
      assertTrue(result.message().contains("Successfully loaded"));
      assertEquals(LoadResult.LOADED, entity.getLoadStatus());
      assertNull(entity.getLoadError());

      verify(pluginRegistryService)
          .registerDynamicPlugin(any(BatchJobPlugin.class));
    }
  }

  // ── Load failures ─────────────────────────────────────────────────────────

  @Test
  void loadJob_missingJarFile_setsFailed() {
    JobDefinitionEntity entity = createEntity(2L, "missing-jar-job", true, null);
    entity.setJarFilePath("/nonexistent/path.jar");

    when(jobDefinitionRepository.findById(2L)).thenReturn(Optional.of(entity));

    JobLoadException ex = assertThrows(JobLoadException.class, () -> service.loadJob(2L));
    assertTrue(ex.getMessage().contains("JAR file not found"));
    assertEquals(LoadResult.FAILED, entity.getLoadStatus());
    assertNotNull(entity.getLoadError());
  }

  @Test
  void loadJob_checksumMismatch_setsFailed() {
    JobDefinitionEntity entity = createEntity(18L, "tampered-job", true, null);
    // Use a known-wrong checksum — the actual file on disk will have a different hash
    entity.setJarChecksum(
        "0000000000000000000000000000000000000000000000000000000000000000");

    when(jobDefinitionRepository.findById(18L)).thenReturn(Optional.of(entity));

    JobLoadException ex =
        assertThrows(JobLoadException.class, () -> service.loadJob(18L));
    assertTrue(
        ex.getMessage().contains("Checksum mismatch"),
        "Expected 'Checksum mismatch' but got: " + ex.getMessage());
    assertEquals(LoadResult.FAILED, entity.getLoadStatus());
    assertNotNull(entity.getLoadError());
    assertTrue(
        entity.getLoadError().contains("Checksum mismatch"),
        "Expected loadError to contain 'Checksum mismatch' but got: " + entity.getLoadError());
  }

  @Test
  void loadJob_classNotImplementingBatchJobPlugin_setsFailed() throws Exception {
    JobDefinitionEntity entity = createEntity(3L, "bad-plugin-job", true, null);

    when(jobDefinitionRepository.findById(3L)).thenReturn(Optional.of(entity));

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, ctx) -> {
              when(mock.loadClass("com.test.TestPlugin")).thenAnswer(inv -> String.class);
            })) {

      JobLoadException ex = assertThrows(JobLoadException.class, () -> service.loadJob(3L));
      assertTrue(
          ex.getMessage().contains("does not implement BatchJobPlugin"),
          "Expected 'does not implement' but got: " + ex.getMessage());
      assertEquals(LoadResult.FAILED, entity.getLoadStatus());
      assertNotNull(entity.getLoadError());
    }
  }

  @Test
  void loadJob_nameMismatch_setsFailed() throws Exception {
    JobDefinitionEntity entity = createEntity(4L, "other-name", true, null);

    when(jobDefinitionRepository.findById(4L)).thenReturn(Optional.of(entity));

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, ctx) -> {
              when(mock.loadClass("com.test.TestPlugin"))
                  .thenAnswer(inv -> TestBatchJobPlugin.class);
            })) {

      JobLoadException ex = assertThrows(JobLoadException.class, () -> service.loadJob(4L));
      assertTrue(
          ex.getMessage().contains("declares jobName="),
          "Expected 'declares jobName=' but got: " + ex.getMessage());
      assertEquals(LoadResult.FAILED, entity.getLoadStatus());
    }
  }

  @Test
  void loadJob_classNotFound_setsFailed() throws Exception {
    JobDefinitionEntity entity = createEntity(5L, "bad-class-job", true, null);

    when(jobDefinitionRepository.findById(5L)).thenReturn(Optional.of(entity));

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, ctx) -> {
              when(mock.loadClass("com.test.TestPlugin"))
                  .thenThrow(new ClassNotFoundException("com.test.TestPlugin"));
            })) {

      JobLoadException ex = assertThrows(JobLoadException.class, () -> service.loadJob(5L));
      assertTrue(
          ex.getMessage().contains("Failed to load job"),
          "Expected 'Failed to load job' but got: " + ex.getMessage());
      assertEquals(LoadResult.FAILED, entity.getLoadStatus());
      assertNotNull(entity.getLoadError());
    }
  }

  @Test
  void loadJob_configureJobFails_setsFailed() throws Exception {
    JobDefinitionEntity entity = createEntity(17L, "test-job", true, null);

    when(jobDefinitionRepository.findById(17L)).thenReturn(Optional.of(entity));
    doThrow(new PluginRegistrationException("configureJob failed"))
        .when(pluginRegistryService)
        .registerDynamicPlugin(any(BatchJobPlugin.class));

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, ctx) -> {
              when(mock.loadClass("com.test.TestPlugin"))
                  .thenAnswer(inv -> TestBatchJobPlugin.class);
            })) {

      JobLoadException ex = assertThrows(JobLoadException.class, () -> service.loadJob(17L));
      assertTrue(
          ex.getMessage().contains("configureJob failed"),
          "Expected 'configureJob failed' but got: " + ex.getMessage());
      assertEquals(LoadResult.FAILED, entity.getLoadStatus());
      assertNotNull(entity.getLoadError());
    }
  }

  @Test
  void loadJob_lateFailureAfterRegistration_unregistersBeforeClassLoaderCleanup() throws Exception {
    JobDefinitionEntity entity = createEntity(19L, "test-job", true, null);
    RuntimeException loadFailure = new RuntimeException("persisting LOADED failed");

    when(jobDefinitionRepository.findById(19L)).thenReturn(Optional.of(entity));
    when(jobDefinitionRepository.save(entity))
        .thenAnswer(
            invocation -> {
              if (LoadResult.LOADED.equals(entity.getLoadStatus())) {
                throw loadFailure;
              }
              return entity;
            });

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, ctx) ->
                when(mock.loadClass("com.test.TestPlugin"))
                    .thenAnswer(inv -> TestBatchJobPlugin.class))) {
      JobLoadException thrown =
          assertThrows(JobLoadException.class, () -> service.loadJob(19L));
      DynamicJobClassLoader candidate = mocked.constructed().getFirst();
      InOrder rollbackOrder = inOrder(pluginRegistryService, candidate);

      assertSame(loadFailure, thrown.getCause());
      rollbackOrder.verify(pluginRegistryService).unregisterDynamicPlugin("test-job");
      rollbackOrder.verify(candidate).cleanup();
      assertFalse(classLoadersOf(service).containsKey(19L));
    }
  }

  @Test
  void loadJob_rollbackUnregisterFails_retainsClassLoaderOwnership() throws Exception {
    JobDefinitionEntity entity = createEntity(20L, "test-job", true, null);
    RuntimeException loadFailure = new RuntimeException("persisting LOADED failed");
    RuntimeException rollbackFailure = new RuntimeException("unregister failed");

    when(jobDefinitionRepository.findById(20L)).thenReturn(Optional.of(entity));
    when(jobDefinitionRepository.save(entity))
        .thenAnswer(
            invocation -> {
              if (LoadResult.LOADED.equals(entity.getLoadStatus())) {
                throw loadFailure;
              }
              return entity;
            });
    doThrow(rollbackFailure).when(pluginRegistryService).unregisterDynamicPlugin("test-job");

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, ctx) ->
                when(mock.loadClass("com.test.TestPlugin"))
                    .thenAnswer(inv -> TestBatchJobPlugin.class))) {
      JobLoadException thrown =
          assertThrows(JobLoadException.class, () -> service.loadJob(20L));
      DynamicJobClassLoader candidate = mocked.constructed().getFirst();

      assertSame(loadFailure, thrown.getCause());
      assertSame(rollbackFailure, loadFailure.getSuppressed()[0]);
      assertSame(candidate, classLoadersOf(service).get(20L));
      verify(candidate, never()).cleanup();
    }
  }

  // ── Initial LOADING persistence failures ──────────────────────────────────

  @Test
  void loadJob_initialLoadingSaveFails_marksFailedWithOriginalCause() throws Exception {
    JobDefinitionEntity entity = createEntity(21L, "test-job", true, null);
    RuntimeException initialFailure = new RuntimeException("persisting LOADING failed");

    when(jobDefinitionRepository.findById(21L)).thenReturn(Optional.of(entity));
    when(jobDefinitionRepository.save(entity))
        .thenAnswer(
            invocation -> {
              if ("LOADING".equals(entity.getLoadStatus())) {
                throw initialFailure;
              }
              return entity;
            });

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(DynamicJobClassLoader.class)) {
      JobLoadException thrown =
          assertThrows(JobLoadException.class, () -> service.loadJob(21L));

      assertSame(initialFailure, thrown.getCause());
      assertEquals(LoadResult.FAILED, entity.getLoadStatus());
      assertEquals("persisting LOADING failed", entity.getLoadError());
      assertTrue(mocked.constructed().isEmpty(), "no classloader may be created");
    }

    verify(pluginRegistryService, never()).registerDynamicPlugin(any());
    verify(auditService)
        .logEvent(argThat(event -> AuditEvent.FAILURE.equals(event.outcome())));
    assertTrue(classLoadersOf(service).isEmpty());
    assertTrue(lifecycleLocksOf(service).isEmpty(), "lifecycle lock must be released");
  }

  @Test
  void loadJob_initialAndFailedSavesFail_keepsOriginalCauseAndSuppressesSecond() {
    JobDefinitionEntity entity = createEntity(22L, "test-job", true, null);
    RuntimeException initialFailure = new RuntimeException("persisting LOADING failed");
    RuntimeException failedSaveFailure = new RuntimeException("persisting FAILED failed");

    when(jobDefinitionRepository.findById(22L)).thenReturn(Optional.of(entity));
    when(jobDefinitionRepository.save(entity))
        .thenThrow(initialFailure)
        .thenThrow(failedSaveFailure);

    JobLoadException thrown = assertThrows(JobLoadException.class, () -> service.loadJob(22L));

    assertSame(initialFailure, thrown.getCause());
    assertArrayEquals(new Throwable[] {failedSaveFailure}, initialFailure.getSuppressed());
    assertEquals(LoadResult.FAILED, entity.getLoadStatus());
  }

  @Test
  void loadJob_afterInitialLoadingSaveFailure_retrySucceeds() throws Exception {
    JobDefinitionEntity entity = createEntity(23L, "test-job", true, null);

    when(jobDefinitionRepository.findById(23L)).thenReturn(Optional.of(entity));
    when(jobDefinitionRepository.save(entity))
        .thenThrow(new RuntimeException("persisting LOADING failed"))
        .thenReturn(entity);

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, ctx) ->
                when(mock.loadClass("com.test.TestPlugin"))
                    .thenAnswer(inv -> TestBatchJobPlugin.class))) {
      assertThrows(JobLoadException.class, () -> service.loadJob(23L));

      LoadResult result = service.loadJob(23L);

      assertEquals(LoadResult.LOADED, result.status());
      assertEquals(LoadResult.LOADED, entity.getLoadStatus());
      assertEquals(1, mocked.constructed().size());
      verify(pluginRegistryService, times(1)).registerDynamicPlugin(any());
    }
  }

  // ── Pre-condition validations ─────────────────────────────────────────────

  @Test
  void loadJob_definitionNotFound_throwsNoSuchElementException() {
    when(jobDefinitionRepository.findById(999L)).thenReturn(Optional.empty());

    assertThrows(NoSuchElementException.class, () -> service.loadJob(999L));
  }

  @Test
  void loadJob_nonexistentId_doesNotRetainLock() throws Exception {
    when(jobDefinitionRepository.findById(999L)).thenReturn(Optional.empty());

    assertThrows(NoSuchElementException.class, () -> service.loadJob(999L));

    assertTrue(
        lifecycleLocksOf(service).isEmpty(),
        "loadJob must not retain a per-definition lock for a nonexistent id");
  }

  @Test
  void loadJob_disabledDefinition_throwsIllegalStateException() {
    JobDefinitionEntity entity = createEntity(6L, "disabled-job", false, null);

    when(jobDefinitionRepository.findById(6L)).thenReturn(Optional.of(entity));

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> service.loadJob(6L));
    assertTrue(ex.getMessage().contains("disabled"));
  }

  @Test
  void loadJob_alreadyLoaded_throwsIllegalStateException() {
    // Persisted status is intentionally NOT LOADED: the guard must fire purely
    // because the job is in the live registry, proving it consults the registry
    // and not the DB column.
    JobDefinitionEntity entity = createEntity(7L, "already-loaded", true, LoadResult.UNLOADED);

    when(jobDefinitionRepository.findById(7L)).thenReturn(Optional.of(entity));
    when(pluginRegistryService.getRegisteredJobNames()).thenReturn(Set.of("already-loaded"));

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> service.loadJob(7L));
    assertTrue(ex.getMessage().contains("already loaded"));
  }

  // ── Unload ────────────────────────────────────────────────────────────────

  @Test
  void unloadJob_success_releasesClassLoaderOwnershipAndUpdatesStatus() throws Exception {
    JobDefinitionEntity entity =
        createEntity(8L, "unloadable-job", true, LoadResult.LOADED);
    DynamicJobClassLoader classLoader = mock(DynamicJobClassLoader.class);
    classLoadersOf(service).put(8L, classLoader);

    when(jobDefinitionRepository.findById(8L)).thenReturn(Optional.of(entity));
    when(jobRepository.findRunningJobExecutions("unloadable-job"))
        .thenReturn(Collections.emptySet());
    doNothing().when(pluginRegistryService).unregisterDynamicPlugin("unloadable-job");

    LoadResult result = service.unloadJob(8L, false);

    assertEquals("unloadable-job", result.jobName());
    assertEquals(LoadResult.UNLOADED, result.status());
    assertEquals(LoadResult.UNLOADED, entity.getLoadStatus());
    verify(pluginRegistryService).unregisterDynamicPlugin("unloadable-job");
    verify(classLoader).cleanup();
    assertFalse(classLoadersOf(service).containsKey(8L));
  }

  @Test
  void unloadJob_runningExecutions_notForced_throwsConflict() {
    JobDefinitionEntity entity =
        createEntity(9L, "running-job", true, LoadResult.LOADED);

    when(jobDefinitionRepository.findById(9L)).thenReturn(Optional.of(entity));
    when(jobRepository.findRunningJobExecutions("running-job"))
        .thenReturn(Set.of(mock(JobExecution.class)));

    JobUnloadConflictException ex =
        assertThrows(JobUnloadConflictException.class, () -> service.unloadJob(9L, false));
    assertTrue(ex.getMessage().contains("running execution"));
    verify(pluginRegistryService, never()).unregisterDynamicPlugin(any());
  }

  @Test
  void unloadJob_runningExecutions_forced_succeeds() {
    JobDefinitionEntity entity =
        createEntity(10L, "force-unload-job", true, LoadResult.LOADED);

    when(jobDefinitionRepository.findById(10L)).thenReturn(Optional.of(entity));
    doNothing().when(pluginRegistryService).unregisterDynamicPlugin("force-unload-job");

    LoadResult result = service.unloadJob(10L, true);

    assertEquals(LoadResult.UNLOADED, result.status());
    verify(pluginRegistryService).unregisterDynamicPlugin("force-unload-job");
  }

  @Test
  void unloadJob_definitionNotFound_throwsNoSuchElementException() {
    when(jobDefinitionRepository.findById(999L)).thenReturn(Optional.empty());

    assertThrows(NoSuchElementException.class, () -> service.unloadJob(999L, false));
  }

  // ── Delete ────────────────────────────────────────────────────────────────

  /** Returns the single {@code JOB_DELETED} audit event emitted by the service. */
  private AuditEvent singleDeletedAuditEvent() {
    ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
    verify(auditService, atLeastOnce()).logEvent(captor.capture());
    List<AuditEvent> deleted =
        captor.getAllValues().stream()
            .filter(event -> event.type() == AuditEventType.JOB_DELETED)
            .toList();
    assertEquals(1, deleted.size(), "Expected exactly one JOB_DELETED audit event: " + deleted);
    return deleted.get(0);
  }

  @Test
  void deleteJob_loadedPlugin_unloadsDeletesRowsInOneTransactionThenDeletesJar() throws Exception {
    JobDefinitionEntity entity = createEntity(40L, "delete-loaded-job", true, LoadResult.LOADED);
    DynamicJobClassLoader classLoader = mock(DynamicJobClassLoader.class);
    classLoadersOf(service).put(40L, classLoader);
    Path jar = Path.of(validJarPath);

    when(jobDefinitionRepository.findById(40L)).thenReturn(Optional.of(entity));
    when(jobExecutionAuditRepository.existsByJobDefinitionId(40L)).thenReturn(false);
    when(jobRepository.findRunningJobExecutions("delete-loaded-job"))
        .thenReturn(Collections.emptySet());
    doAnswer(
            invocation -> {
              assertTrue(Files.exists(jar), "JAR must still exist when the transaction commits");
              return null;
            })
        .when(transactionManager)
        .commit(any());

    service.deleteJob(40L, false);

    InOrder order =
        inOrder(pluginRegistryService, jobParameterTemplateRepository, jobDefinitionRepository,
            transactionManager);
    order.verify(pluginRegistryService).unregisterDynamicPlugin("delete-loaded-job");
    order.verify(jobParameterTemplateRepository).deleteByJobDefinitionId(40L);
    order.verify(jobDefinitionRepository).deleteById(40L);
    order.verify(transactionManager).commit(any());
    verify(classLoader).cleanup();
    assertFalse(classLoadersOf(service).containsKey(40L));
    assertFalse(Files.exists(jar), "JAR must be deleted after commit");
    assertTrue(lifecycleLocksOf(service).isEmpty());

    AuditEvent deleted = singleDeletedAuditEvent();
    assertEquals(AuditEvent.SUCCESS, deleted.outcome());
    assertEquals("delete-loaded-job", deleted.jobName());
  }

  @Test
  void deleteJob_notLoadedDefinition_succeeds() {
    JobDefinitionEntity entity = createEntity(41L, "delete-unloaded-job", true, LoadResult.UNLOADED);

    when(jobDefinitionRepository.findById(41L)).thenReturn(Optional.of(entity));
    when(jobExecutionAuditRepository.existsByJobDefinitionId(41L)).thenReturn(false);
    when(jobRepository.findRunningJobExecutions("delete-unloaded-job"))
        .thenReturn(Collections.emptySet());

    service.deleteJob(41L, false);

    verify(jobParameterTemplateRepository).deleteByJobDefinitionId(41L);
    verify(jobDefinitionRepository).deleteById(41L);
    assertFalse(Files.exists(Path.of(validJarPath)));
    assertEquals(AuditEvent.SUCCESS, singleDeletedAuditEvent().outcome());
  }

  @Test
  void deleteJob_runningExecutionsWithoutForce_throwsConflictAndDeletesNothing() throws Exception {
    JobDefinitionEntity entity = createEntity(42L, "delete-running-job", true, LoadResult.LOADED);
    DynamicJobClassLoader classLoader = mock(DynamicJobClassLoader.class);
    classLoadersOf(service).put(42L, classLoader);

    when(jobDefinitionRepository.findById(42L)).thenReturn(Optional.of(entity));
    when(jobExecutionAuditRepository.existsByJobDefinitionId(42L)).thenReturn(false);
    when(jobRepository.findRunningJobExecutions("delete-running-job"))
        .thenReturn(Set.of(mock(JobExecution.class)));

    assertThrows(JobUnloadConflictException.class, () -> service.deleteJob(42L, false));

    verify(pluginRegistryService, never()).unregisterDynamicPlugin(any());
    verify(classLoader, never()).cleanup();
    assertSame(classLoader, classLoadersOf(service).get(42L));
    verifyNoInteractions(jobParameterTemplateRepository, transactionManager);
    verify(jobDefinitionRepository, never()).deleteById(any());
    assertTrue(Files.exists(Path.of(validJarPath)), "JAR must be kept on conflict");

    AuditEvent deleted = singleDeletedAuditEvent();
    assertEquals(AuditEvent.FAILURE, deleted.outcome());
    assertTrue(deleted.details().contains("id=42"), deleted.details());
    assertTrue(deleted.details().contains("running execution"), deleted.details());
  }

  @ParameterizedTest(name = "force={0}")
  @ValueSource(booleans = {false, true})
  void deleteJob_executionHistory_throwsDeleteConflictBeforeUnloadEvenWhenForced(boolean force)
      throws Exception {
    JobDefinitionEntity entity = createEntity(48L, "delete-history-job", true, LoadResult.LOADED);
    DynamicJobClassLoader classLoader = mock(DynamicJobClassLoader.class);
    classLoadersOf(service).put(48L, classLoader);

    when(jobDefinitionRepository.findById(48L)).thenReturn(Optional.of(entity));
    when(jobExecutionAuditRepository.existsByJobDefinitionId(48L)).thenReturn(true);

    JobDeleteConflictException ex =
        assertThrows(JobDeleteConflictException.class, () -> service.deleteJob(48L, force));
    assertTrue(ex.getMessage().contains("execution history"), ex.getMessage());
    assertTrue(ex.getMessage().contains("disable"), ex.getMessage());

    verifyNoInteractions(
        pluginRegistryService, jobRepository, jobParameterTemplateRepository, transactionManager);
    verify(classLoader, never()).cleanup();
    assertSame(classLoader, classLoadersOf(service).get(48L));
    verify(jobDefinitionRepository, never()).save(any());
    verify(jobDefinitionRepository, never()).deleteById(any());
    assertEquals(LoadResult.LOADED, entity.getLoadStatus());
    assertTrue(Files.exists(Path.of(validJarPath)), "JAR must be kept on a history conflict");
    assertTrue(lifecycleLocksOf(service).isEmpty());

    AuditEvent deleted = singleDeletedAuditEvent();
    assertEquals(AuditEvent.FAILURE, deleted.outcome());
    assertTrue(deleted.details().contains("id=48"), deleted.details());
    assertTrue(deleted.details().contains("execution history"), deleted.details());
    verify(auditService, never())
        .logEvent(argThat(event -> event.type() == AuditEventType.JOB_UNLOADED));
  }

  @Test
  void deleteJob_runningExecutionsWithForce_deletesWithoutTouchingExecutions() {
    JobDefinitionEntity entity = createEntity(43L, "delete-forced-job", true, LoadResult.LOADED);

    when(jobDefinitionRepository.findById(43L)).thenReturn(Optional.of(entity));
    when(jobExecutionAuditRepository.existsByJobDefinitionId(43L)).thenReturn(false);

    service.deleteJob(43L, true);

    verify(pluginRegistryService).unregisterDynamicPlugin("delete-forced-job");
    verify(jobDefinitionRepository).deleteById(43L);
    verifyNoInteractions(jobRepository);
    assertEquals(AuditEvent.SUCCESS, singleDeletedAuditEvent().outcome());
  }

  @Test
  void deleteJob_missingJar_succeeds() {
    JobDefinitionEntity entity = createEntity(44L, "delete-missing-jar-job", true, null);
    String missingJar = tempDir.resolve("already-gone.jar").toString();
    entity.setJarFilePath(missingJar);

    when(jobDefinitionRepository.findById(44L)).thenReturn(Optional.of(entity));
    when(jobExecutionAuditRepository.existsByJobDefinitionId(44L)).thenReturn(false);

    service.deleteJob(44L, true);

    verify(jobDefinitionRepository).deleteById(44L);
    AuditEvent deleted = singleDeletedAuditEvent();
    assertEquals(AuditEvent.SUCCESS, deleted.outcome());
    assertTrue(deleted.details().contains(missingJar), deleted.details());
  }

  @Test
  void deleteJob_jarDeleteFails_stillSucceedsAndNamesOrphanJarInAudit() throws Exception {
    // A non-empty directory cannot be removed by Files.deleteIfExists, which simulates an
    // I/O failure on the JAR path after the database transaction committed.
    Path undeletable = tempDir.resolve("undeletable.jar");
    Files.createDirectory(undeletable);
    Files.createFile(undeletable.resolve("blocker"));
    JobDefinitionEntity entity = createEntity(45L, "delete-orphan-job", true, null);
    entity.setJarFilePath(undeletable.toString());

    when(jobDefinitionRepository.findById(45L)).thenReturn(Optional.of(entity));
    when(jobExecutionAuditRepository.existsByJobDefinitionId(45L)).thenReturn(false);

    assertDoesNotThrow(() -> service.deleteJob(45L, true));

    verify(jobDefinitionRepository).deleteById(45L);
    assertTrue(Files.exists(undeletable));
    AuditEvent deleted = singleDeletedAuditEvent();
    assertEquals(AuditEvent.SUCCESS, deleted.outcome());
    assertTrue(deleted.details().contains("orphan"), deleted.details());
    assertTrue(deleted.details().contains(undeletable.toString()), deleted.details());
  }

  @Test
  void deleteJob_databaseDeleteFails_auditsFailureRollsBackAndKeepsJar() {
    JobDefinitionEntity entity = createEntity(46L, "delete-db-failure-job", true, null);
    DataIntegrityViolationException failure = new DataIntegrityViolationException("db down");

    when(jobDefinitionRepository.findById(46L)).thenReturn(Optional.of(entity));
    when(jobExecutionAuditRepository.existsByJobDefinitionId(46L)).thenReturn(false);
    doThrow(failure).when(jobDefinitionRepository).deleteById(46L);

    DataIntegrityViolationException thrown =
        assertThrows(DataIntegrityViolationException.class, () -> service.deleteJob(46L, true));

    assertSame(failure, thrown);
    verify(transactionManager).rollback(any());
    verify(transactionManager, never()).commit(any());
    assertTrue(Files.exists(Path.of(validJarPath)), "JAR must be kept when the DB delete fails");

    AuditEvent deleted = singleDeletedAuditEvent();
    assertEquals(AuditEvent.FAILURE, deleted.outcome());
    assertEquals("delete-db-failure-job", deleted.jobName());
    assertTrue(deleted.details().contains("id=46"), deleted.details());
    assertTrue(deleted.details().contains("db down"), deleted.details());
  }

  @Test
  void deleteJob_nonexistentId_throwsNotFoundAndRetainsNoLock() throws Exception {
    when(jobDefinitionRepository.findById(999L)).thenReturn(Optional.empty());

    assertThrows(NoSuchElementException.class, () -> service.deleteJob(999L, false));

    assertTrue(lifecycleLocksOf(service).isEmpty());
    verify(jobDefinitionRepository, never()).deleteById(any());
    verifyNoInteractions(pluginRegistryService, jobParameterTemplateRepository);
  }

  @Test
  void deleteJob_holdsDefinitionLockThroughDatabaseDelete() throws Exception {
    JobDefinitionEntity entity = createEntity(47L, "delete-lock-job", true, LoadResult.LOADED);
    when(jobDefinitionRepository.findById(47L)).thenReturn(Optional.of(entity));
    when(jobExecutionAuditRepository.existsByJobDefinitionId(47L)).thenReturn(false);

    AtomicInteger unregisterCalls = new AtomicInteger();
    CountDownLatch secondUnregisterEntered = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              if (unregisterCalls.incrementAndGet() == 2) {
                secondUnregisterEntered.countDown();
              }
              return null;
            })
        .when(pluginRegistryService)
        .unregisterDynamicPlugin("delete-lock-job");

    CountDownLatch dbDeleteEntered = new CountDownLatch(1);
    CountDownLatch releaseDbDelete = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              dbDeleteEntered.countDown();
              assertTrue(releaseDbDelete.await(5, TimeUnit.SECONDS));
              return null;
            })
        .when(jobDefinitionRepository)
        .deleteById(47L);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<?> delete = pool.submit(() -> service.deleteJob(47L, true));
      assertTrue(dbDeleteEntered.await(5, TimeUnit.SECONDS));

      Future<?> concurrentUnload = pool.submit(() -> service.unloadJob(47L, true));
      assertFalse(
          secondUnregisterEntered.await(250, TimeUnit.MILLISECONDS),
          "Concurrent unload must not enter while delete owns the lifecycle lock");

      releaseDbDelete.countDown();
      delete.get(5, TimeUnit.SECONDS);
      concurrentUnload.get(5, TimeUnit.SECONDS);
      assertEquals(2, unregisterCalls.get());
      assertTrue(lifecycleLocksOf(service).isEmpty());
    } finally {
      releaseDbDelete.countDown();
      pool.shutdownNow();
    }
  }

  // ── Replace JAR ───────────────────────────────────────────────────────────

  private static final String NEW_VERSION = "2.0.0";
  private static final String NEW_MAIN_CLASS = "com.test.ReplacementPlugin";

  private final MockMultipartFile replacementUpload =
      new MockMultipartFile("file", "replacement.jar", "application/java-archive", new byte[] {1});

  /** Returns the single {@code JAR_REPLACED} audit event emitted by the service. */
  private AuditEvent singleReplacedAuditEvent() {
    ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
    verify(auditService, atLeastOnce()).logEvent(captor.capture());
    List<AuditEvent> replaced =
        captor.getAllValues().stream()
            .filter(event -> event.type() == AuditEventType.JAR_REPLACED)
            .toList();
    assertEquals(1, replaced.size(), "Expected exactly one JAR_REPLACED audit event: " + replaced);
    return replaced.get(0);
  }

  /** Creates a stored replacement JAR on disk, as {@link JarUploadService} would. */
  private JarUploadService.StoredJar storedReplacement(String jobName) throws IOException {
    Path stored = tempDir.resolve(jobName + "-" + NEW_VERSION + "-0a1b2c3d.jar");
    Files.write(stored, new byte[] {1});
    return new JarUploadService.StoredJar(stored, "new-checksum");
  }

  private void stubSaveReturnsArgument() {
    when(jobDefinitionRepository.save(any(JobDefinitionEntity.class)))
        .thenAnswer(invocation -> invocation.getArgument(0));
  }

  @Test
  void replaceJar_loadedPlugin_unloadsUpdatesDefinitionResetsApprovalThenDeletesOldJar()
      throws Exception {
    JobDefinitionEntity entity = createEntity(60L, "replace-job", true, LoadResult.LOADED);
    entity.setVersion("1.0.0");
    entity.setApprovedBy("alice");
    entity.setApprovedAt(java.time.LocalDateTime.now());
    DynamicJobClassLoader classLoader = mock(DynamicJobClassLoader.class);
    classLoadersOf(service).put(60L, classLoader);
    Path oldJar = Path.of(validJarPath);
    JarUploadService.StoredJar newJar = storedReplacement("replace-job");

    when(jobDefinitionRepository.findById(60L)).thenReturn(Optional.of(entity));
    when(jobRepository.findRunningJobExecutions("replace-job")).thenReturn(Collections.emptySet());
    when(jarUploadService.storeReplacementJar(
            replacementUpload, "replace-job", NEW_VERSION, NEW_MAIN_CLASS))
        .thenReturn(newJar);
    stubSaveReturnsArgument();
    doAnswer(
            invocation -> {
              assertTrue(Files.exists(oldJar), "Old JAR must still exist when the update commits");
              return null;
            })
        .when(transactionManager)
        .commit(any());

    JobDefinitionEntity result =
        service.replaceJar(60L, replacementUpload, NEW_VERSION, NEW_MAIN_CLASS);

    InOrder order = inOrder(pluginRegistryService, jarUploadService, transactionManager);
    order.verify(pluginRegistryService).unregisterDynamicPlugin("replace-job");
    order.verify(jarUploadService)
        .storeReplacementJar(replacementUpload, "replace-job", NEW_VERSION, NEW_MAIN_CLASS);
    order.verify(transactionManager).commit(any());
    order.verify(jarUploadService).autoApproveIfEnabled(result);
    verify(classLoader).cleanup();
    assertFalse(classLoadersOf(service).containsKey(60L));

    assertEquals(60L, result.getId());
    assertEquals("replace-job", result.getJobName());
    assertEquals(newJar.path().toString(), result.getJarFilePath());
    assertEquals("new-checksum", result.getJarChecksum());
    assertEquals(NEW_VERSION, result.getVersion());
    assertEquals(NEW_MAIN_CLASS, result.getMainClassName());
    assertEquals("PENDING", result.getApprovalStatus());
    assertNull(result.getApprovedBy());
    assertNull(result.getApprovedAt());
    assertEquals(LoadResult.UNLOADED, result.getLoadStatus());
    assertTrue(result.getEnabled(), "enabled must be unchanged");

    assertFalse(Files.exists(oldJar), "Old JAR must be deleted after commit");
    assertTrue(Files.exists(newJar.path()));
    assertTrue(lifecycleLocksOf(service).isEmpty());

    AuditEvent replaced = singleReplacedAuditEvent();
    assertEquals(AuditEvent.SUCCESS, replaced.outcome());
    assertEquals("replace-job", replaced.jobName());
    assertTrue(replaced.details().contains("id=60"), replaced.details());
    assertTrue(replaced.details().contains(NEW_VERSION), replaced.details());
    assertTrue(replaced.details().contains("PENDING"), replaced.details());
    assertTrue(replaced.details().contains(validJarPath), replaced.details());
  }

  @Test
  void replaceJar_autoApproveFails_stillSucceedsWithPendingApproval() throws Exception {
    JobDefinitionEntity entity = createEntity(61L, "replace-approve-job", false, null);
    JarUploadService.StoredJar newJar = storedReplacement("replace-approve-job");

    when(jobDefinitionRepository.findById(61L)).thenReturn(Optional.of(entity));
    when(jobRepository.findRunningJobExecutions("replace-approve-job"))
        .thenReturn(Collections.emptySet());
    when(jarUploadService.storeReplacementJar(any(), any(), any(), any())).thenReturn(newJar);
    stubSaveReturnsArgument();
    when(jarUploadService.autoApproveIfEnabled(any()))
        .thenAnswer(
            invocation -> {
              JobDefinitionEntity target = invocation.getArgument(0);
              target.setApprovalStatus("APPROVED");
              target.setApprovedBy("system");
              throw new DataIntegrityViolationException("approve save failed");
            });

    JobDefinitionEntity result =
        service.replaceJar(61L, replacementUpload, NEW_VERSION, NEW_MAIN_CLASS);

    assertEquals("PENDING", result.getApprovalStatus());
    assertNull(result.getApprovedBy());
    assertFalse(result.getEnabled(), "enabled must be unchanged");
    AuditEvent replaced = singleReplacedAuditEvent();
    assertEquals(AuditEvent.SUCCESS, replaced.outcome());
    assertTrue(replaced.details().contains("auto-approval failed"), replaced.details());
  }

  @Test
  void replaceJar_runningExecutions_throwsConflictAndChangesNothing() throws Exception {
    JobDefinitionEntity entity = createEntity(62L, "replace-running-job", true, LoadResult.LOADED);
    DynamicJobClassLoader classLoader = mock(DynamicJobClassLoader.class);
    classLoadersOf(service).put(62L, classLoader);

    when(jobDefinitionRepository.findById(62L)).thenReturn(Optional.of(entity));
    when(jobRepository.findRunningJobExecutions("replace-running-job"))
        .thenReturn(Set.of(mock(JobExecution.class)));

    assertThrows(
        JobUnloadConflictException.class,
        () -> service.replaceJar(62L, replacementUpload, NEW_VERSION, NEW_MAIN_CLASS));

    verifyNoInteractions(jarUploadService, transactionManager);
    verify(pluginRegistryService, never()).unregisterDynamicPlugin(any());
    verify(classLoader, never()).cleanup();
    assertSame(classLoader, classLoadersOf(service).get(62L));
    verify(jobDefinitionRepository, never()).save(any());
    assertEquals(validJarPath, entity.getJarFilePath());
    assertTrue(Files.exists(Path.of(validJarPath)), "Current JAR must be kept on conflict");
    try (var files = Files.list(tempDir)) {
      assertEquals(1, files.count(), "No new JAR may be left behind");
    }
    assertTrue(lifecycleLocksOf(service).isEmpty());

    AuditEvent replaced = singleReplacedAuditEvent();
    assertEquals(AuditEvent.FAILURE, replaced.outcome());
    assertTrue(replaced.details().contains("id=62"), replaced.details());
    assertTrue(replaced.details().contains("running execution"), replaced.details());
    assertTrue(replaced.details().contains("Nothing was changed"), replaced.details());
  }

  @Test
  void replaceJar_invalidJar_throwsAndKeepsDefinitionAndCurrentJar() throws Exception {
    JobDefinitionEntity entity = createEntity(63L, "replace-invalid-job", true, null);
    entity.setVersion("1.0.0");
    InvalidJarException invalid = new InvalidJarException("Main class not found in JAR");

    when(jobDefinitionRepository.findById(63L)).thenReturn(Optional.of(entity));
    when(jobRepository.findRunningJobExecutions("replace-invalid-job"))
        .thenReturn(Collections.emptySet());
    when(jarUploadService.storeReplacementJar(any(), any(), any(), any())).thenThrow(invalid);

    InvalidJarException thrown =
        assertThrows(
            InvalidJarException.class,
            () -> service.replaceJar(63L, replacementUpload, NEW_VERSION, NEW_MAIN_CLASS));

    assertSame(invalid, thrown);
    verifyNoInteractions(transactionManager);
    verify(jarUploadService, never()).autoApproveIfEnabled(any());
    assertEquals(validJarPath, entity.getJarFilePath());
    assertEquals(validJarChecksum, entity.getJarChecksum());
    assertEquals("1.0.0", entity.getVersion());
    assertEquals("APPROVED", entity.getApprovalStatus());
    assertEquals(LoadResult.UNLOADED, entity.getLoadStatus(), "the definition stays unloaded");
    assertTrue(Files.exists(Path.of(validJarPath)), "Current JAR must be kept");
    try (var files = Files.list(tempDir)) {
      assertEquals(1, files.count(), "No new JAR may be left behind");
    }

    AuditEvent replaced = singleReplacedAuditEvent();
    assertEquals(AuditEvent.FAILURE, replaced.outcome());
    assertTrue(replaced.details().contains("id=63"), replaced.details());
    assertTrue(replaced.details().contains("Main class not found"), replaced.details());
    assertTrue(replaced.details().contains("stays unloaded"), replaced.details());
    assertTrue(replaced.details().contains(validJarPath), replaced.details());
  }

  @Test
  void replaceJar_databaseUpdateFails_deletesNewJarKeepsOldJarAndAuditsFailure() throws Exception {
    JobDefinitionEntity entity = createEntity(64L, "replace-db-failure-job", true, null);
    JarUploadService.StoredJar newJar = storedReplacement("replace-db-failure-job");
    DataIntegrityViolationException failure = new DataIntegrityViolationException("db down");

    when(jobDefinitionRepository.findById(64L)).thenReturn(Optional.of(entity));
    when(jobRepository.findRunningJobExecutions("replace-db-failure-job"))
        .thenReturn(Collections.emptySet());
    when(jarUploadService.storeReplacementJar(any(), any(), any(), any())).thenReturn(newJar);
    // First save persists the unload; the second one is the replacement update.
    when(jobDefinitionRepository.save(any(JobDefinitionEntity.class)))
        .thenAnswer(invocation -> invocation.getArgument(0))
        .thenThrow(failure);

    DataIntegrityViolationException thrown =
        assertThrows(
            DataIntegrityViolationException.class,
            () -> service.replaceJar(64L, replacementUpload, NEW_VERSION, NEW_MAIN_CLASS));

    assertSame(failure, thrown);
    verify(transactionManager).rollback(any());
    verify(transactionManager, never()).commit(any());
    verify(jarUploadService, never()).autoApproveIfEnabled(any());
    assertFalse(Files.exists(newJar.path()), "New JAR must be deleted when the update fails");
    assertTrue(Files.exists(Path.of(validJarPath)), "Current JAR must be kept");

    AuditEvent replaced = singleReplacedAuditEvent();
    assertEquals(AuditEvent.FAILURE, replaced.outcome());
    assertEquals("replace-db-failure-job", replaced.jobName());
    assertTrue(replaced.details().contains("id=64"), replaced.details());
    assertTrue(replaced.details().contains("db down"), replaced.details());
    assertTrue(replaced.details().contains(newJar.path().toString()), replaced.details());
  }

  @Test
  void replaceJar_oldJarDeleteFails_stillSucceedsAndNamesOrphanJarInAudit() throws Exception {
    // A non-empty directory cannot be removed by Files.deleteIfExists, which simulates an
    // I/O failure on the previous JAR path after the update committed.
    Path undeletable = tempDir.resolve("undeletable-old.jar");
    Files.createDirectory(undeletable);
    Files.createFile(undeletable.resolve("blocker"));
    JobDefinitionEntity entity = createEntity(65L, "replace-orphan-job", true, null);
    entity.setJarFilePath(undeletable.toString());
    JarUploadService.StoredJar newJar = storedReplacement("replace-orphan-job");

    when(jobDefinitionRepository.findById(65L)).thenReturn(Optional.of(entity));
    when(jobRepository.findRunningJobExecutions("replace-orphan-job"))
        .thenReturn(Collections.emptySet());
    when(jarUploadService.storeReplacementJar(any(), any(), any(), any())).thenReturn(newJar);
    stubSaveReturnsArgument();

    JobDefinitionEntity result =
        assertDoesNotThrow(
            () -> service.replaceJar(65L, replacementUpload, NEW_VERSION, NEW_MAIN_CLASS));

    assertEquals(newJar.path().toString(), result.getJarFilePath());
    assertTrue(Files.exists(undeletable));
    AuditEvent replaced = singleReplacedAuditEvent();
    assertEquals(AuditEvent.SUCCESS, replaced.outcome());
    assertTrue(replaced.details().contains("orphan"), replaced.details());
    assertTrue(replaced.details().contains(undeletable.toString()), replaced.details());
  }

  @Test
  void replaceJar_oldJarAlreadyMissing_succeeds() throws Exception {
    JobDefinitionEntity entity = createEntity(66L, "replace-missing-job", true, null);
    String missingJar = tempDir.resolve("already-gone.jar").toString();
    entity.setJarFilePath(missingJar);
    JarUploadService.StoredJar newJar = storedReplacement("replace-missing-job");

    when(jobDefinitionRepository.findById(66L)).thenReturn(Optional.of(entity));
    when(jobRepository.findRunningJobExecutions("replace-missing-job"))
        .thenReturn(Collections.emptySet());
    when(jarUploadService.storeReplacementJar(any(), any(), any(), any())).thenReturn(newJar);
    stubSaveReturnsArgument();

    service.replaceJar(66L, replacementUpload, NEW_VERSION, NEW_MAIN_CLASS);

    AuditEvent replaced = singleReplacedAuditEvent();
    assertEquals(AuditEvent.SUCCESS, replaced.outcome());
    assertTrue(replaced.details().contains("already absent"), replaced.details());
  }

  @Test
  void replaceJar_nonexistentId_throwsNotFoundAndRetainsNoLock() throws Exception {
    when(jobDefinitionRepository.findById(999L)).thenReturn(Optional.empty());

    assertThrows(
        NoSuchElementException.class,
        () -> service.replaceJar(999L, replacementUpload, NEW_VERSION, NEW_MAIN_CLASS));

    assertTrue(lifecycleLocksOf(service).isEmpty());
    verifyNoInteractions(pluginRegistryService, jarUploadService, transactionManager);
  }

  @Test
  void replaceJar_holdsDefinitionLockWhileStoringAndUpdating() throws Exception {
    JobDefinitionEntity entity = createEntity(67L, "replace-lock-job", true, LoadResult.LOADED);
    JarUploadService.StoredJar newJar = storedReplacement("replace-lock-job");
    when(jobDefinitionRepository.findById(67L)).thenReturn(Optional.of(entity));
    when(jobRepository.findRunningJobExecutions("replace-lock-job"))
        .thenReturn(Collections.emptySet());
    stubSaveReturnsArgument();

    AtomicInteger unregisterCalls = new AtomicInteger();
    CountDownLatch secondUnregisterEntered = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              if (unregisterCalls.incrementAndGet() == 2) {
                secondUnregisterEntered.countDown();
              }
              return null;
            })
        .when(pluginRegistryService)
        .unregisterDynamicPlugin("replace-lock-job");

    CountDownLatch storeEntered = new CountDownLatch(1);
    CountDownLatch releaseStore = new CountDownLatch(1);
    when(jarUploadService.storeReplacementJar(any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              storeEntered.countDown();
              assertTrue(releaseStore.await(5, TimeUnit.SECONDS));
              return newJar;
            });

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<?> replace =
          pool.submit(() -> service.replaceJar(67L, replacementUpload, NEW_VERSION, NEW_MAIN_CLASS));
      assertTrue(storeEntered.await(5, TimeUnit.SECONDS));

      Future<?> concurrentUnload = pool.submit(() -> service.unloadJob(67L, true));
      assertFalse(
          secondUnregisterEntered.await(250, TimeUnit.MILLISECONDS),
          "Concurrent unload must not enter while replace owns the lifecycle lock");

      releaseStore.countDown();
      replace.get(5, TimeUnit.SECONDS);
      concurrentUnload.get(5, TimeUnit.SECONDS);
      assertEquals(2, unregisterCalls.get());
      assertTrue(lifecycleLocksOf(service).isEmpty());
    } finally {
      releaseStore.countDown();
      pool.shutdownNow();
    }
  }

  // ── Reload ────────────────────────────────────────────────────────────────

  @Test
  void reloadJob_unloadsThenLoads_atomic() throws Exception {
    JobDefinitionEntity entity =
        createEntity(11L, "test-job", true, LoadResult.LOADED);

    when(jobDefinitionRepository.findById(11L)).thenReturn(Optional.of(entity));
    doNothing().when(pluginRegistryService).unregisterDynamicPlugin("test-job");
    doNothing()
        .when(pluginRegistryService)
        .registerDynamicPlugin(any(BatchJobPlugin.class));

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, ctx) -> {
              when(mock.loadClass("com.test.TestPlugin"))
                  .thenAnswer(inv -> TestBatchJobPlugin.class);
            })) {

      LoadResult result = service.reloadJob(11L);

      assertEquals(LoadResult.LOADED, result.status());
      verify(pluginRegistryService).unregisterDynamicPlugin("test-job");
      verify(pluginRegistryService)
          .registerDynamicPlugin(any(BatchJobPlugin.class));
    }
  }

  @Test
  void reloadJob_holdsOneLockAcrossUnloadAndReplacementLoad() throws Exception {
    JobDefinitionEntity entity = createEntity(21L, "test-job", true, LoadResult.LOADED);
    entity.setMainClassName(TestBatchJobPlugin.class.getName());
    when(jobDefinitionRepository.findById(21L)).thenReturn(Optional.of(entity));

    AtomicInteger unregisterCalls = new AtomicInteger();
    CountDownLatch secondUnregisterEntered = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              if (unregisterCalls.incrementAndGet() == 2) {
                secondUnregisterEntered.countDown();
              }
              return null;
            })
        .when(pluginRegistryService)
        .unregisterDynamicPlugin("test-job");

    CountDownLatch replacementRegisterEntered = new CountDownLatch(1);
    CountDownLatch releaseReplacementRegister = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              replacementRegisterEntered.countDown();
              assertTrue(releaseReplacementRegister.await(5, TimeUnit.SECONDS));
              return null;
            })
        .when(pluginRegistryService)
        .registerDynamicPlugin(any(BatchJobPlugin.class));

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<?> reload = pool.submit(() -> service.reloadJob(21L));
      assertTrue(replacementRegisterEntered.await(5, TimeUnit.SECONDS));

      Future<?> concurrentUnload = pool.submit(() -> service.unloadJob(21L, true));
      assertFalse(
          secondUnregisterEntered.await(250, TimeUnit.MILLISECONDS),
          "Concurrent unload must not enter while reload owns the lifecycle lock");

      releaseReplacementRegister.countDown();
      reload.get(5, TimeUnit.SECONDS);
      concurrentUnload.get(5, TimeUnit.SECONDS);
      assertEquals(2, unregisterCalls.get());
    } finally {
      releaseReplacementRegister.countDown();
      pool.shutdownNow();
    }
  }

  @Test
  void reloadJob_unregisterFailureAbortsReplacementAndPreservesLoadedState() throws Exception {
    JobDefinitionEntity entity = createEntity(22L, "test-job", true, LoadResult.LOADED);
    DynamicJobClassLoader oldLoader = mock(DynamicJobClassLoader.class);
    classLoadersOf(service).put(22L, oldLoader);
    when(jobDefinitionRepository.findById(22L)).thenReturn(Optional.of(entity));
    doThrow(new RuntimeException("unregister failed"))
        .when(pluginRegistryService)
        .unregisterDynamicPlugin("test-job");

    assertThrows(RuntimeException.class, () -> service.reloadJob(22L));

    verify(pluginRegistryService, never()).registerDynamicPlugin(any(BatchJobPlugin.class));
    verify(oldLoader, never()).cleanup();
    verify(jobDefinitionRepository, never()).save(entity);
    assertSame(oldLoader, classLoadersOf(service).get(22L));
    assertEquals(LoadResult.LOADED, entity.getLoadStatus());
  }

  @Test
  void reloadJob_replacementFailureCleansCandidateAndPersistsFailed() throws Exception {
    JobDefinitionEntity entity = createEntity(23L, "test-job", true, LoadResult.LOADED);
    DynamicJobClassLoader oldLoader = mock(DynamicJobClassLoader.class);
    classLoadersOf(service).put(23L, oldLoader);
    when(jobDefinitionRepository.findById(23L)).thenReturn(Optional.of(entity));

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, context) ->
                when(mock.loadClass("com.test.TestPlugin"))
                    .thenThrow(new ClassNotFoundException("replacement missing")))) {
      assertThrows(JobLoadException.class, () -> service.reloadJob(23L));
      verify(oldLoader).cleanup();
      verify(mocked.constructed().getFirst()).cleanup();
    }

    verify(pluginRegistryService).unregisterDynamicPlugin("test-job");
    verify(pluginRegistryService, never()).registerDynamicPlugin(any(BatchJobPlugin.class));
    verify(jobDefinitionRepository, atLeastOnce()).save(entity);
    assertTrue(classLoadersOf(service).isEmpty());
    assertEquals(LoadResult.FAILED, entity.getLoadStatus());
  }

  @Test
  void loadJob_recoversFailedDefinitionWithOneRegistrationAndClassLoader() throws Exception {
    JobDefinitionEntity entity = createEntity(24L, "test-job", true, LoadResult.FAILED);
    when(jobDefinitionRepository.findById(24L)).thenReturn(Optional.of(entity));

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, context) ->
                when(mock.loadClass("com.test.TestPlugin"))
                    .thenAnswer(invocation -> TestBatchJobPlugin.class))) {
      assertEquals(LoadResult.LOADED, service.loadJob(24L).status());
      assertEquals(1, mocked.constructed().size());
    }

    verify(pluginRegistryService, times(1)).registerDynamicPlugin(any(BatchJobPlugin.class));
    assertEquals(1, classLoadersOf(service).size());
    assertEquals(LoadResult.LOADED, entity.getLoadStatus());
  }

  @Test
  void loadJob_failedRecoveryLeavesFailedWithoutLoaderOrRegistration() throws Exception {
    JobDefinitionEntity entity = createEntity(25L, "test-job", true, LoadResult.FAILED);
    when(jobDefinitionRepository.findById(25L)).thenReturn(Optional.of(entity));

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, context) ->
                when(mock.loadClass("com.test.TestPlugin"))
                    .thenThrow(new ClassNotFoundException("recovery missing")))) {
      assertThrows(JobLoadException.class, () -> service.loadJob(25L));
      verify(mocked.constructed().getFirst()).cleanup();
    }

    verify(pluginRegistryService, never()).registerDynamicPlugin(any(BatchJobPlugin.class));
    assertTrue(classLoadersOf(service).isEmpty());
    assertEquals(LoadResult.FAILED, entity.getLoadStatus());
  }

  // ── loadAllEnabled ────────────────────────────────────────────────────────

  @Test
  void loadAllEnabled_skipsAlreadyLoaded_loadsOthers() throws Exception {
    JobDefinitionEntity loaded =
        createEntity(12L, "already-loaded", true, LoadResult.LOADED);
    JobDefinitionEntity pending = createEntity(13L, "test-job", true, null);

    when(jobDefinitionRepository.findByEnabled(true)).thenReturn(List.of(loaded, pending));
    when(jobDefinitionRepository.findById(13L)).thenReturn(Optional.of(pending));
    // The "already-loaded" definition is the one currently in the live registry;
    // "test-job" is not, so it must actually load.
    when(pluginRegistryService.getRegisteredJobNames()).thenReturn(Set.of("already-loaded"));
    doNothing()
        .when(pluginRegistryService)
        .registerDynamicPlugin(any(BatchJobPlugin.class));

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, ctx) -> {
              when(mock.loadClass("com.test.TestPlugin"))
                  .thenAnswer(inv -> TestBatchJobPlugin.class);
            })) {

      Map<String, LoadResult> results = service.loadAllEnabled();

      assertEquals(2, results.size());
      assertTrue(results.containsKey("already-loaded"));
      assertEquals(LoadResult.LOADED, results.get("already-loaded").status());
      assertTrue(results.get("already-loaded").message().contains("Already loaded"));

      assertTrue(results.containsKey("test-job"));
      assertEquals(LoadResult.LOADED, results.get("test-job").status());
    }
  }

  @Test
  void loadAllEnabled_oneFails_othersStillLoaded() throws Exception {
    JobDefinitionEntity good = createEntity(14L, "test-job", true, null);
    JobDefinitionEntity bad = createEntity(15L, "bad-job", true, null);
    bad.setJarFilePath("/nonexistent/bad.jar");

    when(jobDefinitionRepository.findByEnabled(true)).thenReturn(List.of(good, bad));
    when(jobDefinitionRepository.findById(14L)).thenReturn(Optional.of(good));
    when(jobDefinitionRepository.findById(15L)).thenReturn(Optional.of(bad));
    doNothing()
        .when(pluginRegistryService)
        .registerDynamicPlugin(any(BatchJobPlugin.class));

    try (MockedConstruction<DynamicJobClassLoader> mocked =
        mockConstruction(
            DynamicJobClassLoader.class,
            (mock, ctx) -> {
              when(mock.loadClass("com.test.TestPlugin"))
                  .thenAnswer(inv -> TestBatchJobPlugin.class);
            })) {

      Map<String, LoadResult> results = service.loadAllEnabled();

      assertEquals(2, results.size());
      assertEquals(LoadResult.LOADED, results.get("test-job").status());
      assertEquals(LoadResult.FAILED, results.get("bad-job").status());
      assertTrue(
          results.get("bad-job").message().contains("JAR file not found"),
          "Expected 'JAR file not found' but got: " + results.get("bad-job").message());
    }
  }

  // ── Concurrency ───────────────────────────────────────────────────────────

  @Test
  void withDefinitionLock_sameIdExcludesConcurrentCallers() throws Exception {
    CountDownLatch firstEntered = new CountDownLatch(1);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    CountDownLatch secondEntered = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<?> first =
          pool.submit(
              () ->
                  service.withDefinitionLock(
                      30L,
                      () -> {
                        firstEntered.countDown();
                        await(releaseFirst);
                      }));
      assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
      Future<?> second = pool.submit(() -> service.withDefinitionLock(30L, secondEntered::countDown));

      assertFalse(secondEntered.await(200, TimeUnit.MILLISECONDS));
      releaseFirst.countDown();
      first.get(5, TimeUnit.SECONDS);
      second.get(5, TimeUnit.SECONDS);
      assertEquals(0, lifecycleLocksOf(service).size());
    } finally {
      releaseFirst.countDown();
      pool.shutdownNow();
    }
  }

  @Test
  void withDefinitionLock_differentIdsProceedIndependently() throws Exception {
    CountDownLatch bothEntered = new CountDownLatch(2);
    CountDownLatch releaseBoth = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<?> first =
          pool.submit(
              () ->
                  service.withDefinitionLock(
                      31L,
                      () -> {
                        bothEntered.countDown();
                        await(releaseBoth);
                      }));
      Future<?> second =
          pool.submit(
              () ->
                  service.withDefinitionLock(
                      32L,
                      () -> {
                        bothEntered.countDown();
                        await(releaseBoth);
                      }));

      assertTrue(bothEntered.await(5, TimeUnit.SECONDS));
      releaseBoth.countDown();
      first.get(5, TimeUnit.SECONDS);
      second.get(5, TimeUnit.SECONDS);
      assertTrue(lifecycleLocksOf(service).isEmpty());
    } finally {
      releaseBoth.countDown();
      pool.shutdownNow();
    }
  }

  @Test
  void withDefinitionLock_queuedCallersShareHolderUntilAllComplete() throws Exception {
    CountDownLatch firstEntered = new CountDownLatch(1);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    CountDownLatch queuedEntered = new CountDownLatch(2);
    ExecutorService pool = Executors.newFixedThreadPool(3);
    try {
      Future<?> first =
          pool.submit(
              () ->
                  service.withDefinitionLock(
                      33L,
                      () -> {
                        firstEntered.countDown();
                        await(releaseFirst);
                      }));
      assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
      Future<?> second = pool.submit(() -> service.withDefinitionLock(33L, queuedEntered::countDown));
      Future<?> third = pool.submit(() -> service.withDefinitionLock(33L, queuedEntered::countDown));

      assertEquals(1, lifecycleLocksOf(service).size());
      releaseFirst.countDown();
      assertTrue(queuedEntered.await(5, TimeUnit.SECONDS));
      first.get(5, TimeUnit.SECONDS);
      second.get(5, TimeUnit.SECONDS);
      third.get(5, TimeUnit.SECONDS);
      assertTrue(lifecycleLocksOf(service).isEmpty());
    } finally {
      releaseFirst.countDown();
      pool.shutdownNow();
    }
  }

  @Test
  void withDefinitionLock_reclaimsHolderAfterFailure() throws Exception {
    assertThrows(
        IllegalStateException.class,
        () -> service.withDefinitionLock(34L, () -> { throw new IllegalStateException("failure"); }));

    assertTrue(lifecycleLocksOf(service).isEmpty());
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted while awaiting test synchronization", exception);
    }
  }

  /**
   * Two concurrent {@code loadJob} calls for the SAME definition id must be serialized: only one may
   * proceed through the load body (opening a classloader / JAR file-handle and registering); the
   * loser must observe the winner's registration and be rejected, never opening a second handle.
   *
   * <p>Uses a REAL {@link DynamicJobClassLoader} rather than {@code mockConstruction}, because
   * Mockito's construction mocking is thread-confined and would not intercept the pool threads. The
   * parent-last classloader resolves {@link TestBatchJobPlugin} from the application classloader, so
   * the load succeeds on both threads. The winner is parked inside {@code registerDynamicPlugin}
   * (holding the per-id lock under the fix); the registration only becomes visible once it returns,
   * modelling the real race window. Without serialization the contender sails past the already-loaded
   * guard and reaches {@code registerDynamicPlugin} a second time; serialized, it parks on the lock
   * and is rejected by the guard. The bounded {@link CountDownLatch} awaits assert this
   * deterministically, without timing-dependent sleeps.
   */
  @Test
  void loadJob_concurrentSameId_serializesAndOpensSingleClassloader() throws Exception {
    JobDefinitionEntity entity = createEntity(20L, "test-job", true, null);
    // Real parent-last classloader resolves this class from the application classloader.
    entity.setMainClassName(TestBatchJobPlugin.class.getName());
    when(jobDefinitionRepository.findById(20L)).thenReturn(Optional.of(entity));

    // Stateful registry: a registration only becomes visible once register() returns, modelling the
    // real race window between the already-loaded guard and the registry write.
    Set<String> registeredNames = ConcurrentHashMap.newKeySet();
    AtomicInteger guardCalls = new AtomicInteger();
    CountDownLatch secondGuardReached = new CountDownLatch(1);
    when(pluginRegistryService.getRegisteredJobNames())
        .thenAnswer(
            inv -> {
              if (guardCalls.incrementAndGet() == 2) {
                secondGuardReached.countDown();
              }
              return new HashSet<>(registeredNames);
            });

    CountDownLatch registerEntered = new CountDownLatch(1);
    CountDownLatch releaseRegister = new CountDownLatch(1);
    doAnswer(
            inv -> {
              registerEntered.countDown();
              releaseRegister.await();
              BatchJobPlugin plugin = inv.getArgument(0);
              registeredNames.add(plugin.getJobName());
              return null;
            })
        .when(pluginRegistryService)
        .registerDynamicPlugin(any(BatchJobPlugin.class));

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      // Winner: blocks inside registerDynamicPlugin while holding the per-id lock (under the fix).
      Future<?> winner = pool.submit(() -> service.loadJob(20L));
      assertTrue(
          registerEntered.await(5, TimeUnit.SECONDS),
          "Winning thread never reached registerDynamicPlugin");

      // Contender: same id. Serialized, it parks on the lock and never reaches the guard again;
      // unserialized, it passes the guard and reaches registerDynamicPlugin a second time.
      Future<?> contender =
          pool.submit(
              () -> {
                try {
                  service.loadJob(20L);
                } catch (IllegalStateException alreadyLoaded) {
                  // Expected once serialized: the contender sees the winner's registration.
                }
              });

      boolean contenderPassedGuard = secondGuardReached.await(1, TimeUnit.SECONDS);

      releaseRegister.countDown();
      winner.get(5, TimeUnit.SECONDS);
      contender.get(5, TimeUnit.SECONDS);

      assertFalse(
          contenderPassedGuard,
          "Second concurrent load entered the load body while the first held the lock — "
              + "loadJob is not serialized per definition id");
      // Serialized: only the winner registers; the contender is rejected by the guard.
      verify(pluginRegistryService, times(1))
        .registerDynamicPlugin(any(BatchJobPlugin.class));

      // Release the real classloader/temp-JAR handle opened by the successful load.
      service.unloadJob(20L, true);
    } finally {
      // Unblock any thread still parked in registerDynamicPlugin so shutdown cannot hang.
      releaseRegister.countDown();
      pool.shutdownNow();
    }
  }

  // ── Inner test plugin ─────────────────────────────────────────────────────

  /**
   * Minimal {@link BatchJobPlugin} implementation used in {@code mockConstruction} tests where the
   * mocked classloader returns this class.
   *
   * <p>Must be {@code public} so it can be instantiated via reflection from a mocked classloader
   * context.
   */
  public static class TestBatchJobPlugin implements BatchJobPlugin {

    @Override
    public String getJobName() {
      return "test-job";
    }

    @Override
    public String getVersion() {
      return "1.0-test";
    }

    @Override
    public Job configureJob(
        JobRepository jobRepository,
        PlatformTransactionManager transactionManager,
        ApplicationContext parentContext) {
      return new JobBuilder("test-job", jobRepository)
          .start(
              new StepBuilder("test-step", jobRepository)
                  .tasklet((c, cc) -> RepeatStatus.FINISHED)
                  .build())
          .build();
    }

    @Override
    public Map<String, String> getDefaultParameters() {
      return Collections.emptyMap();
    }

    @Override
    public List<String> getRequiredDependencies() {
      return Collections.emptyList();
    }

    @Override
    public JobMetadata getMetadata() {
      return new JobMetadata() {
        @Override
        public String getDisplayName() {
          return "Test Plugin";
        }

        @Override
        public String getDescription() {
          return "Test BatchJobPlugin implementation";
        }

        @Override
        public String getAuthor() {
          return "test";
        }

        @Override
        public List<String> getTags() {
          return Collections.emptyList();
        }

        @Override
        public Duration getEstimatedRuntime() {
          return Duration.ZERO;
        }
      };
    }
  }
}
