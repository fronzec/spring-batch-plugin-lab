/* 2024-2025 */
package com.fronzec.frbatchservice.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fronzec.frbatchservice.batchjobs.plugins.entity.JobDefinitionEntity;
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobDefinitionRepository;
import com.fronzec.frbatchservice.web.dto.ErrorResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

/**
 * End-to-end integration tests for the plugin management REST API.
 *
 * <p>Validates the full upload-list-get-update-delete lifecycle against an
 * in-memory H2 database using MockMvc. Also exercises every handler in
 * {@link GlobalExceptionHandler} where feasible in the MockMvc layer.
 *
 * <p>Tests are ordered via {@link Order} to build on each other: upload first,
 * read and mutate in the middle, delete last.
 *
 * <p>MockMvc bypasses the servlet container, so the context-path
 * ({@code /api/batch-service}) is NOT prepended to request URIs.
 */
@SpringBootTest(webEnvironment = WebEnvironment.MOCK)
@ActiveProfiles("test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PluginManagementIntegrationTest {

  private static final String JOBS_BASE = "/jobs";
  private static final String TEST_JOB_NAME = "integration-test-plugin";
  private static final String TEST_VERSION = "1.0.0";
  private static final String TEST_MAIN_CLASS = "com.example.TestPlugin";

  @Autowired private WebApplicationContext webApplicationContext;
  @Autowired private JobDefinitionRepository jobDefinitionRepository;

  private MockMvc mockMvc;

  // captured by upload test and reused by later tests
  private static Long createdDefinitionId;

  @BeforeEach
  void setUp() {
    mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
  }

  private static byte[] createMinimalJarBytes(String entryName) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(baos)) {
      ZipEntry entry = new ZipEntry(entryName);
      zos.putNextEntry(entry);
      zos.write(("Created-By: PluginManagementIntegrationTest\r\n")
          .getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create test JAR bytes", e);
    }
    return baos.toByteArray();
  }

  private MockMultipartFile testJar(String filename) {
    return new MockMultipartFile(
        "file", filename, MediaType.APPLICATION_OCTET_STREAM_VALUE, createMinimalJarBytes("META-INF/MANIFEST.MF"));
  }

  @BeforeEach
  void cleanupCapturedIdAfterDeletion() {
    // If the entity was already deleted in a prior run's test, reset the captured ID.
    // (The @DirtiesContext AFTER_CLASS means the DB is not reset between tests,
    // but a re-run may leave stale IDs.)
  }

  // ─── happy path: upload ────────────────────────────────────────────────────

  @Test
  @Order(1)
  void uploadValidJar_returnsCreatedWithMetadata() throws Exception {
    MockMultipartFile jarFile = testJar("test-plugin.jar");

    String responseBody =
        mockMvc
            .perform(
                multipart(JOBS_BASE + "/upload")
                    .file(jarFile)
                    .param("jobName", TEST_JOB_NAME)
                    .param("version", TEST_VERSION)
                    .param("mainClassName", TEST_MAIN_CLASS))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.id").isNumber())
            .andExpect(jsonPath("$.job_name").value(TEST_JOB_NAME))
            .andExpect(jsonPath("$.version").value(TEST_VERSION))
            .andExpect(jsonPath("$.jar_file_name").exists())
            .andExpect(jsonPath("$.jar_checksum").isString())
            .andReturn()
            .getResponse()
            .getContentAsString();

    // Extract ID from response for subsequent tests
    int idStart = responseBody.indexOf("\"id\":") + 5;
    int idEnd = responseBody.indexOf(",", idStart);
    createdDefinitionId = Long.parseLong(responseBody.substring(idStart, idEnd).trim());
    assertThat(createdDefinitionId).isPositive();
  }

  // ─── error: invalid file ───────────────────────────────────────────────────

  @Test
  @Order(2)
  void uploadNonJarFile_returnsBadRequest() throws Exception {
    MockMultipartFile nonJar =
        new MockMultipartFile(
            "file", "notajar.txt", MediaType.TEXT_PLAIN_VALUE,
            "this is not a jar".getBytes(StandardCharsets.UTF_8));

    mockMvc
        .perform(
            multipart(JOBS_BASE + "/upload")
                .file(nonJar)
                .param("jobName", "non-jar-test")
                .param("version", "1.0")
                .param("mainClassName", "com.Foo"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.error").value("Bad Request"))
        .andExpect(jsonPath("$.message").isString())
        .andExpect(jsonPath("$.timestamp").exists())
        .andExpect(jsonPath("$.path").value(JOBS_BASE + "/upload"));
  }

  // ─── error: duplicate job name ─────────────────────────────────────────────

  @Test
  @Order(3)
  void uploadDuplicateJobName_returnsConflict() throws Exception {
    MockMultipartFile jarFile = testJar("duplicate.jar");

    mockMvc
        .perform(
            multipart(JOBS_BASE + "/upload")
                .file(jarFile)
                .param("jobName", TEST_JOB_NAME)
                .param("version", "2.0.0")
                .param("mainClassName", TEST_MAIN_CLASS))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.status").value(409))
        .andExpect(jsonPath("$.error").value("Conflict"))
        .andExpect(jsonPath("$.message").isString())
        .andExpect(jsonPath("$.path").value(JOBS_BASE + "/upload"));
  }

  // ─── list definitions ──────────────────────────────────────────────────────

  @Test
  @Order(4)
  void listDefinitions_includesUploadedDefinition() throws Exception {
    mockMvc
        .perform(get(JOBS_BASE + "/definitions"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$").isArray())
        .andExpect(jsonPath("$").isNotEmpty());
  }

  // ─── get single definition ─────────────────────────────────────────────────

  @Test
  @Order(5)
  void getDefinitionById_returnsDefinition() throws Exception {
    mockMvc
        .perform(get(JOBS_BASE + "/definitions/" + createdDefinitionId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.id").value(createdDefinitionId))
        .andExpect(jsonPath("$.job_name").value(TEST_JOB_NAME))
        .andExpect(jsonPath("$.version").value(TEST_VERSION))
        .andExpect(jsonPath("$.enabled").value(false));
  }

  @Test
  @Order(6)
  void getDefinitionByNonExistentId_returnsNotFound() throws Exception {
    mockMvc
        .perform(get(JOBS_BASE + "/definitions/99999"))
        .andExpect(status().isNotFound());
  }

  // ─── enable / disable ──────────────────────────────────────────────────────

  @Test
  @Order(7)
  void enableDefinition_flipsEnabledToTrue() throws Exception {
    mockMvc
        .perform(put(JOBS_BASE + "/definitions/" + createdDefinitionId + "/enable"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.enabled").value(true));
  }

  @Test
  @Order(8)
  void disableDefinition_flipsEnabledToFalse() throws Exception {
    mockMvc
        .perform(put(JOBS_BASE + "/definitions/" + createdDefinitionId + "/disable"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.enabled").value(false));
  }

  // ─── delete ────────────────────────────────────────────────────────────────

  @Test
  @Order(9)
  void deleteDefinition_returnsNoContent_andSubsequentGetReturnsNotFound() throws Exception {
    mockMvc
        .perform(delete(JOBS_BASE + "/definitions/" + createdDefinitionId))
        .andExpect(status().isNoContent());

    // Subsequent GET must return 404
    mockMvc
        .perform(get(JOBS_BASE + "/definitions/" + createdDefinitionId))
        .andExpect(status().isNotFound());

    // A repeated DELETE of the same id is now an unknown definition: 404 error envelope
    mockMvc
        .perform(delete(JOBS_BASE + "/definitions/" + createdDefinitionId))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.status").value(404))
        .andExpect(jsonPath("$.path").value(JOBS_BASE + "/definitions/" + createdDefinitionId));
  }

  // ─── merged plugins endpoint ───────────────────────────────────────────────

  @Test
  @Order(10)
  void getPlugins_returnsMergedResponse_withClasspathPlugins() throws Exception {
    // This test runs AFTER the upload was deleted, so only classpath plugins remain.
    mockMvc
        .perform(get(JOBS_BASE + "/plugins"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$").isArray())
        .andExpect(jsonPath("$.length()").value(2))
        .andExpect(jsonPath("$[0].source").value("CLASSPATH"))
        .andExpect(jsonPath("$[1].source").value("CLASSPATH"));
  }

  // ─── error: file size exceeded (handler-only test) ─────────────────────────
  // MockMvc bypasses the servlet-container multipart resolver, so
  // MaxUploadSizeExceededException cannot be triggered through mock requests.
  // Instead we verify the handler directly via the GlobalExceptionHandler bean.
  // This is documented in the test name as a "handler verify" rather than a
  // full HTTP round-trip.

  @Autowired private GlobalExceptionHandler globalExceptionHandler;

  @Test
  @Order(11)
  void maxUploadSizeExceeded_handlerReturnsPayloadTooLarge() {
    // Construct the exception as the container would throw it.
    org.springframework.web.multipart.MaxUploadSizeExceededException ex =
        new org.springframework.web.multipart.MaxUploadSizeExceededException(1024 * 1024);
    var mockRequest = new org.springframework.mock.web.MockHttpServletRequest("POST", JOBS_BASE + "/upload");

    var response = globalExceptionHandler.handleMaxUploadSize(ex, mockRequest);

    assertThat(response.getStatusCode().value()).isEqualTo(413);
    ErrorResponse body = response.getBody();
    assertThat(body).isNotNull();
    assertThat(body.status()).isEqualTo(413);
    assertThat(body.error()).isEqualTo("Payload Too Large");
    assertThat(body.path()).isEqualTo(JOBS_BASE + "/upload");
  }

  // ─── auto-approval persistence ──────────────────────────────────────────────

  @Test
  @Order(12)
  void uploadUnderNonProductionProfile_persistsAutoApproval() throws Exception {
    // Under any non-production profile, AutoApproveConfig (@Profile("!production"))
    // auto-approves uploads. The approval must be PERSISTED, so a fresh read from
    // the database reflects APPROVED — not the default PENDING.
    String jobName = "auto-approve-persist-job";
    MockMultipartFile jarFile =
        new MockMultipartFile(
            "file",
            "auto-approve.jar",
            MediaType.APPLICATION_OCTET_STREAM_VALUE,
            createMinimalJarBytes("META-INF/MANIFEST.MF"));

    String responseBody =
        mockMvc
            .perform(
                multipart(JOBS_BASE + "/upload")
                    .file(jarFile)
                    .param("jobName", jobName)
                    .param("version", TEST_VERSION)
                    .param("mainClassName", TEST_MAIN_CLASS))
            .andExpect(status().isCreated())
            .andReturn()
            .getResponse()
            .getContentAsString();

    int idStart = responseBody.indexOf("\"id\":") + 5;
    int idEnd = responseBody.indexOf(",", idStart);
    long id = Long.parseLong(responseBody.substring(idStart, idEnd).trim());

    JobDefinitionEntity persisted = jobDefinitionRepository.findById(id).orElseThrow();
    assertThat(persisted.getApprovalStatus()).isEqualTo("APPROVED");
    assertThat(persisted.getApprovedBy()).isEqualTo("system");
    assertThat(persisted.getApprovedAt()).isNotNull();
  }

  // ─── JAR replacement ────────────────────────────────────────────────────────

  @Test
  @Order(13)
  void replaceJarUnderNonProductionProfile_keepsIdAndReappliesAutoApproval() throws Exception {
    String jobName = "replace-auto-approve-job";
    String uploadBody =
        mockMvc
            .perform(
                multipart(JOBS_BASE + "/upload")
                    .file(testJar("replace-auto-approve.jar"))
                    .param("jobName", jobName)
                    .param("version", TEST_VERSION)
                    .param("mainClassName", TEST_MAIN_CLASS))
            .andExpect(status().isCreated())
            .andReturn()
            .getResponse()
            .getContentAsString();
    int idStart = uploadBody.indexOf("\"id\":") + 5;
    int idEnd = uploadBody.indexOf(",", idStart);
    long id = Long.parseLong(uploadBody.substring(idStart, idEnd).trim());
    String oldJarPath = jobDefinitionRepository.findById(id).orElseThrow().getJarFilePath();

    // The replacement must contain its main class entry.
    MockMultipartFile replacement =
        new MockMultipartFile(
            "file",
            "replace-auto-approve-1.1.0.jar",
            MediaType.APPLICATION_OCTET_STREAM_VALUE,
            createMinimalJarBytes(TEST_MAIN_CLASS.replace('.', '/') + ".class"));

    mockMvc
        .perform(
            multipart(HttpMethod.PUT, JOBS_BASE + "/definitions/" + id + "/jar")
                .file(replacement)
                .param("version", "1.1.0")
                .param("mainClassName", TEST_MAIN_CLASS))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.id").value(id))
        .andExpect(jsonPath("$.version").value("1.1.0"))
        // AutoApproveConfig is active under the test profile, so the reset PENDING approval
        // is re-applied after commit, exactly as on upload.
        .andExpect(jsonPath("$.approval_status").value("APPROVED"))
        .andExpect(jsonPath("$.approved_by").value("system"));

    JobDefinitionEntity persisted = jobDefinitionRepository.findById(id).orElseThrow();
    assertThat(persisted.getApprovalStatus()).isEqualTo("APPROVED");
    assertThat(persisted.getJarFilePath()).isNotEqualTo(oldJarPath);
    assertThat(java.nio.file.Path.of(oldJarPath)).doesNotExist();
    assertThat(java.nio.file.Path.of(persisted.getJarFilePath())).exists();
  }

  @Test
  @Order(14)
  void replaceJarWithoutMainClassEntry_returnsBadRequest() throws Exception {
    JobDefinitionEntity existing =
        jobDefinitionRepository.findByJobName("replace-auto-approve-job").orElseThrow();

    mockMvc
        .perform(
            multipart(HttpMethod.PUT, JOBS_BASE + "/definitions/" + existing.getId() + "/jar")
                .file(testJar("no-main-class.jar"))
                .param("version", "1.2.0")
                .param("mainClassName", TEST_MAIN_CLASS))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString(TEST_MAIN_CLASS)));

    JobDefinitionEntity after = jobDefinitionRepository.findById(existing.getId()).orElseThrow();
    assertThat(after.getVersion()).isEqualTo("1.1.0");
    assertThat(after.getJarFilePath()).isEqualTo(existing.getJarFilePath());
  }
}
