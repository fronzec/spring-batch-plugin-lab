/* 2024-2026 */
package com.fronzec.frbatchservice.batchjobs.plugins.loader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fronzec.frbatchservice.batchjobs.JobsManagerService;
import com.fronzec.frbatchservice.batchjobs.plugins.PluginRegistryService;
import com.fronzec.frbatchservice.batchjobs.plugins.entity.JobDefinitionEntity;
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobDefinitionRepository;
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobExecutionAuditRepository;
import com.fronzec.frbatchservice.restclients.ApiClient;
import com.fronzec.frbatchservice.restclients.DataCalculatedResponse;
import com.fronzec.frbatchservice.web.SingleJobDataRequest;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.*;
import org.springframework.batch.core.ExitStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

/**
 * End-to-end integration tests for the dynamic job loading pipeline: upload, enable, load, unload,
 * reload, load-all, merged plugin listing, and job execution.
 *
 * <p>Uses a pre-compiled {@code DynamicTestPlugin} whose {@code .class} files are packaged into a
 * JAR at test-runtime and uploaded through the real {@code /jobs/upload} multipart endpoint.
 *
 * <p>Uses {@code @TestInstance(PER_CLASS)} so that {@code @AfterAll} can access autowired beans
 * for cleanup. This prevents H2 in-memory DB leakage ({@code DB_CLOSE_DELAY=-1}) across test
 * classes.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@ActiveProfiles("test")
// Disable auto-approval so this suite exercises the real manual lifecycle
// (upload -> enable -> approve -> load), including the explicit approve endpoint.
@TestPropertySource(properties = "app.plugins.approval.auto-approve=false")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DynamicJobLoadingIntegrationTest {

  /* ── Test constants ─────────────────────────────────────────────────────── */

  private static final String JOBS_BASE = "/jobs";
  private static final String JOB_NAME = "dynamic-test-job";
  private static final String VERSION = "1.0.0";
  private static final String MAIN_CLASS = "com.fronzec.test.dynamic.DynamicTestPlugin";
  private static final String JAR_FILENAME = "dynamic-test-plugin.jar";

  /* ── Static state (shared across @BeforeAll / @AfterAll / ordered tests) ── */

  private static Path tempDir;
  private static Path testJarPath;
  static Long definitionId;

  /* ── Spring wiring ─────────────────────────────────────────────────────── */

  @Autowired private WebApplicationContext webApplicationContext;
  @Autowired private JobDefinitionRepository jobDefinitionRepository;
  @Autowired private JobsManagerService jobsManagerService;
  @Autowired private PluginRegistryService pluginRegistryService;
  @Autowired private DynamicJobLoaderService loaderService;
  @Autowired private JobExecutionAuditRepository jobExecutionAuditRepository;

  @MockitoBean private ApiClient apiClient;

  private MockMvc mockMvc;

  /* ── Lifecycle: JAR creation & cleanup ─────────────────────────────────── */

  @BeforeAll
  void createTestJar() throws Exception {
    tempDir = Files.createTempDirectory("dyn-load-test-");
    testJarPath = tempDir.resolve(JAR_FILENAME);

    // Locate the pre-compiled DynamicTestPlugin.class on the test classpath
    String classResource = MAIN_CLASS.replace('.', '/') + ".class";
    URL classUrl =
        DynamicJobLoadingIntegrationTest.class.getClassLoader().getResource(classResource);
    assert classUrl != null : "DynamicTestPlugin.class not found on test classpath — did Maven compile it?";

    // Walk up from the .class file to find the test-classes root
    Path classFile = Path.of(classUrl.toURI());
    Path packageDir = classFile.getParent(); // .../com/fronzec/test/dynamic/
    // Compute the base directory by walking up to the package root
    Path computed = packageDir;
    for (int i = 0; i < MAIN_CLASS.split("\\.").length; i++) {
      computed = computed.getParent();
    }
    final Path baseDir = computed;

    // Package all .class files from the package into a JAR
    try (JarOutputStream jos =
        new JarOutputStream(new BufferedOutputStream(new FileOutputStream(testJarPath.toFile())))) {
      try (var files = Files.walk(packageDir)) {
        files
            .filter(p -> p.toString().endsWith(".class"))
            .forEach(
                p -> {
                  try {
                    String entryName = baseDir.relativize(p).toString().replace(File.separatorChar, '/');
                    jos.putNextEntry(new JarEntry(entryName));
                    Files.copy(p, jos);
                    jos.closeEntry();
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                });
      }
    }
  }

  @AfterAll
  void tearDown() {
    // Clean up the DB: unregister dynamic plugin and delete the definition
    // to prevent H2 in-memory DB leakage (DB_CLOSE_DELAY=-1) to other test classes.
    try {
      if (definitionId != null && jobDefinitionRepository.existsById(definitionId)) {
        var entity = jobDefinitionRepository.findById(definitionId);
        entity.ifPresent(e -> {
          try {
            pluginRegistryService.unregisterDynamicPlugin(e.getJobName());
          } catch (Exception ignored) {
            // best-effort
          }
          jobDefinitionRepository.delete(e);
        });
      }
    } catch (Exception ignored) {
      // best-effort cleanup
    }
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
    // Clean up uploaded JAR in the plugin storage directory
    try {
      Files.deleteIfExists(Path.of("./target/test-plugins/jars/" + JOB_NAME + "-" + VERSION + ".jar"));
    } catch (IOException ignored) {
      // best-effort cleanup
    }
  }

  @BeforeEach
  void setUp() {
    mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
  }

  /* ── Helper: create a MockMultipartFile from the test JAR ──────────────── */

  private MockMultipartFile testJar() throws IOException {
    return new MockMultipartFile(
        "file",
        JAR_FILENAME,
        MediaType.APPLICATION_OCTET_STREAM_VALUE,
        Files.readAllBytes(testJarPath));
  }

  /* ── 1. Upload valid plugin ────────────────────────────────────────────── */

  @Test
  @Order(1)
  void uploadValidJar_returnsCreated() throws Exception {
    MockMultipartFile jarFile = testJar();

    String responseBody =
        mockMvc
            .perform(
                multipart(JOBS_BASE + "/upload")
                    .file(jarFile)
                    .param("jobName", JOB_NAME)
                    .param("version", VERSION)
                    .param("mainClassName", MAIN_CLASS))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.id").isNumber())
            .andExpect(jsonPath("$.job_name").value(JOB_NAME))
            .andExpect(jsonPath("$.version").value(VERSION))
            .andExpect(jsonPath("$.jar_file_name").value(JOB_NAME + "-" + VERSION + ".jar"))
            .andReturn()
            .getResponse()
            .getContentAsString();

    // Capture the definition ID for subsequent tests
    int idStart = responseBody.indexOf("\"id\":") + 5;
    int idEnd = responseBody.indexOf(",", idStart);
    definitionId = Long.parseLong(responseBody.substring(idStart, idEnd).trim());
    assertThat(definitionId).isPositive();
  }

  /* ── 2. Enable the definition ──────────────────────────────────────────── */

  @Test
  @Order(2)
  void enableDefinition_setsEnabledTrue() throws Exception {
    mockMvc
        .perform(put(JOBS_BASE + "/definitions/" + definitionId + "/enable"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.enabled").value(true));
  }

  /* ── 3. Approve the definition ────────────────────────────────────────── */

  @Test
  @Order(3)
  void approveDefinition_setsApproved() throws Exception {
    // The approvedBy field in the request body is accepted for backward compatibility
    // but intentionally ignored — the controller derives the approver from the
    // authenticated principal (AuditService.currentUserId()). In the test context
    // there is no authenticated user, so the fallback "system" is recorded.
    mockMvc
        .perform(
            put(JOBS_BASE + "/definitions/" + definitionId + "/approve")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"approved_by\":\"test-user\"}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.approval_status").value("APPROVED"))
        .andExpect(jsonPath("$.approved_by").value("system"))
        .andExpect(jsonPath("$.approved_at").exists());
  }

  /* ── 4. Load valid plugin → 200 LOADED ─────────────────────────────────── */

  @Test
  @Order(4)
  void loadValidPlugin_returnsLoaded() throws Exception {
    mockMvc
        .perform(post(JOBS_BASE + "/definitions/" + definitionId + "/load"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.job_name").value(JOB_NAME))
        .andExpect(jsonPath("$.status").value("LOADED"))
        .andExpect(jsonPath("$.message").value("Successfully loaded"));
  }

  /* ── 5. Load non-existent definition → 404 ────────────────────────────── */

  @Test
  @Order(5)
  void loadNonExistentDefinition_returnsNotFound() throws Exception {
    mockMvc
        .perform(post(JOBS_BASE + "/definitions/99999/load"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.status").value(404))
        .andExpect(jsonPath("$.error").value("Not Found"));
  }

  /* ── 6. Load already-loaded → 409 ─────────────────────────────────────── */

  @Test
  @Order(6)
  void loadAlreadyLoaded_returnsConflict() throws Exception {
    mockMvc
        .perform(post(JOBS_BASE + "/definitions/" + definitionId + "/load"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.status").value(409))
        .andExpect(jsonPath("$.error").value("Conflict"))
        .andExpect(jsonPath("$.message").value(containsString("already loaded")));
  }

  /* ── 7. Unload loaded plugin → 200 UNLOADED ───────────────────────────── */

  @Test
  @Order(7)
  void unloadLoadedPlugin_returnsUnloaded() throws Exception {
    mockMvc
        .perform(post(JOBS_BASE + "/definitions/" + definitionId + "/unload"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.job_name").value(JOB_NAME))
        .andExpect(jsonPath("$.status").value("UNLOADED"))
        .andExpect(jsonPath("$.message").value("Successfully unloaded"));
  }

  /* ── 8. Unload non-loaded plugin → 200 (idempotent) ──────────────────── */

  @Test
  @Order(8)
  void unloadNonLoadedPlugin_returnsUnloaded() throws Exception {
    // The definition was already unloaded in test 6; a second unload succeeds
    // because the service handles the "not in registry" case gracefully and just
    // updates the DB status.
    mockMvc
        .perform(post(JOBS_BASE + "/definitions/" + definitionId + "/unload"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("UNLOADED"));
  }

  /* ── 9. Reload → 200 LOADED (atomic unload+load) ──────────────────────── */

  @Test
  @Order(9)
  void reloadPlugin_returnsLoaded() throws Exception {
    mockMvc
        .perform(post(JOBS_BASE + "/definitions/" + definitionId + "/reload"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.job_name").value(JOB_NAME))
        .andExpect(jsonPath("$.status").value("LOADED"));
  }

  /* ── 10. GET /jobs/plugins → merged response includes dynamic plugin ───── */

  @Test
  @Order(10)
  void getPlugins_includesDynamicPlugin_andClasspathPlugins() throws Exception {
    mockMvc
        .perform(get(JOBS_BASE + "/plugins"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$").isArray())
        .andExpect(jsonPath("$.length()").value(3))
        // First two are classpath plugins (job1, job2)
        .andExpect(jsonPath("$[0].source").value("CLASSPATH"))
        .andExpect(jsonPath("$[1].source").value("CLASSPATH"))
        // Third is the dynamic plugin — appears via getPlugins() with CLASSPATH source
        .andExpect(jsonPath("$[2].source").value("CLASSPATH"))
        .andExpect(jsonPath("$[2].status").value("ACTIVE"))
        .andExpect(jsonPath("$[2].job_name").value(JOB_NAME))
        .andExpect(jsonPath("$[2].version").value(VERSION))
        .andExpect(jsonPath("$[2].display_name").value("Dynamic Test Plugin"));
  }

  /* ── 11. Execute dynamic plugin via JobsManagerService ─────────────────── */

  @Test
  @Order(11)
  void dynamicPlugin_isExecutable_viaJobsManagerService() {
    // Mock ApiClient in case the job execution infrastructure accesses it
    DataCalculatedResponse salaryResponse = new DataCalculatedResponse();
    salaryResponse.setValue(BigDecimal.valueOf(50000));
    when(apiClient.getRandomValue(any())).thenReturn(salaryResponse);
    when(apiClient.sendBatch(any())).thenReturn(true);

    SingleJobDataRequest request = new SingleJobDataRequest();
    request.setJobBeanName(JOB_NAME);
    request.setParam("DATE", LocalDate.now().toString());
    request.setParam("ATTEMPT_NUMBER", "1");
    request.setParam("description", "dynamic-loading-integration-test");

    Map<String, String> result = jobsManagerService.syncRunJobWithParams(request);

    assertEquals(ExitStatus.COMPLETED.toString(), result.get("result"),
        "Dynamic plugin job should complete successfully");
  }

  /* ── 12. Load-all → loads unloaded definitions ─────────────────────────── */

  @Test
  @Order(12)
  void loadAll_loadsUnloadedDefinitions() throws Exception {
    // First, reload to ensure the definition is LOADED, then unload it so
    // load-all has something to pick up.
    var unloadResult =
        mockMvc
            .perform(post(JOBS_BASE + "/definitions/" + definitionId + "/unload"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("UNLOADED"));

    var mvcResult =
        mockMvc
            .perform(post(JOBS_BASE + "/load-all"))
            .andExpect(status().isOk())
            .andReturn();

    String body = mvcResult.getResponse().getContentAsString();
    assertThat(body).contains("\"job_name\":\"" + JOB_NAME + "\"");
    assertThat(body).contains("\"status\":\"" + LoadResult.LOADED + "\"");
    assertThat(body).contains("\"message\":\"Successfully loaded\"");
  }

  /* ── 13. Auto-load re-instantiates definitions with stale LOADED status ── */

  @Test
  @Order(13)
  void loadAllEnabled_reloadsDefinitionWithStaleLoadedStatus() {
    // Simulate a backend restart: the database still says LOADED from a previous
    // JVM, but the in-memory registry is empty. Auto-load must actually load the
    // plugin (re-instantiate it), not skip it as "Already loaded" off the stale
    // persisted status.

    // Ensure the registry does not hold it (unload if a prior test left it loaded).
    if (pluginRegistryService.getRegisteredJobNames().contains(JOB_NAME)) {
      loaderService.unloadJob(definitionId, true);
    }
    assertThat(pluginRegistryService.getRegisteredJobNames()).doesNotContain(JOB_NAME);

    // Force the stale persisted state: enabled + LOADED, but not in the registry.
    JobDefinitionEntity def = jobDefinitionRepository.findById(definitionId).orElseThrow();
    def.setEnabled(true);
    def.setLoadStatus(LoadResult.LOADED);
    jobDefinitionRepository.save(def);

    // Precondition: the persisted state really is the stale "LOADED" we mean to test
    // (otherwise the assertions below could pass for the wrong reason).
    assertThat(jobDefinitionRepository.findById(definitionId).orElseThrow().getLoadStatus())
        .isEqualTo(LoadResult.LOADED);

    Map<String, LoadResult> results = loaderService.loadAllEnabled();

    LoadResult result = results.get(JOB_NAME);
    assertThat(result).as("auto-load result for %s", JOB_NAME).isNotNull();
    assertThat(result.status()).isEqualTo(LoadResult.LOADED);
    assertThat(result.message())
        .as("stale-LOADED definition must be loaded, not skipped")
        .isEqualTo("Successfully loaded");
    assertThat(pluginRegistryService.getRegisteredJobNames()).contains(JOB_NAME);
  }

  /* ── 14. DELETE with execution history → 409 before unload, nothing changes ── */

  @Test
  @Order(14)
  void deleteWithExecutionHistory_returnsConflict_andKeepsPluginRowAndJar() throws Exception {
    // Test 11 executed the job, so job_executions_audit rows reference this definition.
    // The history guard runs before unload, so even force=true must leave everything intact.
    assertThat(jobExecutionAuditRepository.existsByJobDefinitionId(definitionId)).isTrue();
    assertThat(pluginRegistryService.getRegisteredJobNames()).contains(JOB_NAME);
    Path storedJar =
        Path.of(jobDefinitionRepository.findById(definitionId).orElseThrow().getJarFilePath());

    mockMvc
        .perform(delete(JOBS_BASE + "/definitions/" + definitionId).param("force", "true"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.status").value(409))
        .andExpect(jsonPath("$.error").value("Conflict"))
        .andExpect(jsonPath("$.message").value(containsString("execution history")))
        .andExpect(jsonPath("$.path").value(JOBS_BASE + "/definitions/" + definitionId));

    assertThat(jobDefinitionRepository.existsById(definitionId)).isTrue();
    assertThat(storedJar).exists();
    assertThat(pluginRegistryService.getRegisteredJobNames()).contains(JOB_NAME);
  }

  /* ── 15. DELETE a loaded plugin → 204, unregistered, row and JAR removed ── */

  @Test
  @Order(15)
  void deleteLoadedPlugin_returnsNoContent_andUnregistersJob() throws Exception {
    // Remove the execution history that blocked test 14 so this test isolates the
    // lifecycle behaviour of deleting a loaded plugin.
    jobExecutionAuditRepository.deleteAll(
        jobExecutionAuditRepository.findByJobDefinitionId(definitionId));
    if (!pluginRegistryService.getRegisteredJobNames().contains(JOB_NAME)) {
      loaderService.loadJob(definitionId);
    }
    assertThat(pluginRegistryService.getRegisteredJobNames()).contains(JOB_NAME);
    Path storedJar =
        Path.of(jobDefinitionRepository.findById(definitionId).orElseThrow().getJarFilePath());
    assertThat(storedJar).exists();

    mockMvc
        .perform(delete(JOBS_BASE + "/definitions/" + definitionId))
        .andExpect(status().isNoContent());

    assertThat(pluginRegistryService.getRegisteredJobNames()).doesNotContain(JOB_NAME);
    assertThat(jobDefinitionRepository.existsById(definitionId)).isFalse();
    assertThat(storedJar).doesNotExist();
  }

  /* ── 16. DELETE an unknown definition → 404 error envelope ────────────── */

  @Test
  @Order(16)
  void deleteNonExistentDefinition_returnsNotFound() throws Exception {
    mockMvc
        .perform(delete(JOBS_BASE + "/definitions/99999"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.status").value(404))
        .andExpect(jsonPath("$.error").value("Not Found"));
  }
}
