/* 2024-2026 */
package com.fronzec.frbatchservice.batchjobs.plugins.service;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fronzec.frbatchservice.batchjobs.plugins.audit.AuditService;
import com.fronzec.frbatchservice.batchjobs.plugins.entity.JobDefinitionEntity;
import com.fronzec.frbatchservice.batchjobs.plugins.metrics.PluginMetrics;
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobDefinitionRepository;
import com.fronzec.frbatchservice.batchjobs.plugins.util.ChecksumUtil;
import com.fronzec.frbatchservice.config.AutoApproveConfig;
import com.fronzec.frbatchservice.web.dto.JarUploadResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import java.util.Optional;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

/** Verifies the upload pipeline of {@link JarUploadService}. */
@ExtendWith(MockitoExtension.class)
@DisplayName("JarUploadService")
class JarUploadServiceTest {

  @Mock private JobDefinitionRepository jobDefinitionRepository;
  @Mock private JarSignatureVerifier jarSignatureVerifier;
  @Mock private AuditService auditService;
  @Mock private PluginMetrics pluginMetrics;

  @TempDir private Path jarDir;

  private JarUploadService service;

  @BeforeEach
  void setUp() {
    service =
        new JarUploadService(
            jobDefinitionRepository,
            Optional.<AutoApproveConfig>empty(),
            jarSignatureVerifier,
            auditService,
            pluginMetrics,
            "permissive",
            jarDir.toString());
  }

  @Test
  @DisplayName("computes the persisted checksum from the stored file, never from the multipart stream")
  void uploadJar_checksumIsComputedFromStoredFile_notMultipartStream() throws IOException {
    byte[] jarBytes = minimalJar();
    MockMultipartFile file =
        new MockMultipartFile(
            "file", "test-job-1.0.0.jar", "application/java-archive", jarBytes);
    Path storedPath = jarDir.resolve("test-job-1.0.0.jar");

    when(jobDefinitionRepository.findByJobName("test-job")).thenReturn(Optional.empty());
    when(jarSignatureVerifier.verify(any()))
        .thenReturn(new SignatureResult(true, null, List.of()));
    when(jobDefinitionRepository.save(any(JobDefinitionEntity.class)))
        .thenAnswer(
            invocation -> {
              JobDefinitionEntity saved = invocation.getArgument(0);
              saved.setId(1L);
              return saved;
            });

    JarUploadResponse response;
    try (MockedStatic<ChecksumUtil> checksum = mockStatic(ChecksumUtil.class)) {
      // Stub only the Path overload: the digest the service stores must come from
      // the persisted file. The multipart overload is left unstubbed so that a
      // regression to hashing the stream would surface a null checksum AND trip
      // the never() verification below.
      checksum.when(() -> ChecksumUtil.computeSha256(storedPath)).thenReturn("disk-digest");

      response = service.uploadJar(file, "test-job", "1.0.0", "com.example.MyPlugin");

      // The checksum was read from the persisted file, not from the upload stream.
      checksum.verify(() -> ChecksumUtil.computeSha256(storedPath));
      checksum.verify(() -> ChecksumUtil.computeSha256(any(MultipartFile.class)), never());
    }

    ArgumentCaptor<JobDefinitionEntity> captor =
        ArgumentCaptor.forClass(JobDefinitionEntity.class);
    verify(jobDefinitionRepository).save(captor.capture());

    assertTrue(Files.exists(storedPath), "JAR should be written to disk before hashing");
    assertArrayEquals(
        jarBytes, Files.readAllBytes(storedPath), "stored bytes should match the upload");
    assertEquals(
        "disk-digest",
        captor.getValue().getJarChecksum(),
        "persisted checksum must be the digest of the stored file");
    assertEquals("disk-digest", response.jarChecksum(), "response must expose the same checksum");
  }

  // ─── replacement JAR storage ───────────────────────────────────────────────

  @Test
  @DisplayName("stores a replacement JAR under a unique name without overwriting the current file")
  void storeReplacementJar_storesUniqueFileWithoutOverwritingCurrentJar() throws IOException {
    Path currentJar = jarDir.resolve("test-job-1.0.0.jar");
    Files.write(currentJar, "current".getBytes());
    byte[] jarBytes = minimalJar();
    when(jarSignatureVerifier.verify(any()))
        .thenReturn(new SignatureResult(true, null, List.of()));

    JarUploadService.StoredJar first =
        service.storeReplacementJar(
            replacementFile(jarBytes), "test-job", "1.0.0", "com.example.MyPlugin");
    JarUploadService.StoredJar second =
        service.storeReplacementJar(
            replacementFile(jarBytes), "test-job", "1.0.0", "com.example.MyPlugin");

    assertNotEquals(currentJar, first.path());
    assertNotEquals(first.path(), second.path(), "each replacement must get a unique file name");
    assertTrue(
        first.path().getFileName().toString().matches("test-job-1\\.0\\.0-[0-9a-f]{8}\\.jar"),
        first.path().toString());
    assertEquals(jarDir.toAbsolutePath().normalize(), first.path().getParent());
    assertArrayEquals(jarBytes, Files.readAllBytes(first.path()));
    assertArrayEquals(
        "current".getBytes(), Files.readAllBytes(currentJar), "current JAR must be untouched");
    assertEquals(ChecksumUtil.computeSha256(first.path()), first.checksum());
    verifyNoInteractions(jobDefinitionRepository, auditService);
  }

  @Test
  @DisplayName("rejects a replacement JAR that does not contain the main class and stores nothing")
  void storeReplacementJar_missingMainClassEntry_rejectsAndLeavesNoFile() throws IOException {
    InvalidJarException ex =
        assertThrows(
            InvalidJarException.class,
            () ->
                service.storeReplacementJar(
                    replacementFile(minimalJar()), "test-job", "2.0.0", "com.example.Other"));

    assertTrue(ex.getMessage().contains("com.example.Other"), ex.getMessage());
    assertEquals(0, filesIn(jarDir));
    verifyNoInteractions(jarSignatureVerifier);
  }

  @Test
  @DisplayName("rejects a replacement that is not a JAR before storing anything")
  void storeReplacementJar_notAJar_rejectsWithoutStoring() throws IOException {
    MockMultipartFile notAJar =
        new MockMultipartFile("file", "plugin.jar", "application/java-archive", "hello".getBytes());

    assertThrows(
        InvalidJarException.class,
        () -> service.storeReplacementJar(notAJar, "test-job", "2.0.0", "com.example.MyPlugin"));

    assertEquals(0, filesIn(jarDir));
    verifyNoInteractions(jarSignatureVerifier);
  }

  @Test
  @DisplayName("deletes the stored replacement when strict signature verification fails")
  void storeReplacementJar_strictSignatureFailure_rejectsAndDeletesStoredFile() throws IOException {
    JarUploadService strictService =
        new JarUploadService(
            jobDefinitionRepository,
            Optional.<AutoApproveConfig>empty(),
            jarSignatureVerifier,
            auditService,
            pluginMetrics,
            "strict",
            jarDir.toString());
    when(jarSignatureVerifier.verify(any()))
        .thenReturn(new SignatureResult(false, null, List.of("unsigned")));

    InvalidJarException ex =
        assertThrows(
            InvalidJarException.class,
            () ->
                strictService.storeReplacementJar(
                    replacementFile(minimalJar()), "test-job", "2.0.0", "com.example.MyPlugin"));

    assertTrue(ex.getMessage().contains("strict"), ex.getMessage());
    assertEquals(0, filesIn(jarDir));
  }

  // ─── auto-approval ─────────────────────────────────────────────────────────

  @Test
  @DisplayName("auto-approves and re-saves a definition when auto-approval is configured")
  void autoApproveIfEnabled_withConfig_approvesAndSaves() {
    AutoApproveConfig autoApprove = mock(AutoApproveConfig.class);
    JarUploadService autoApproveService =
        new JarUploadService(
            jobDefinitionRepository,
            Optional.of(autoApprove),
            jarSignatureVerifier,
            auditService,
            pluginMetrics,
            "permissive",
            jarDir.toString());
    JobDefinitionEntity entity = new JobDefinitionEntity();

    assertTrue(autoApproveService.autoApproveIfEnabled(entity));

    verify(autoApprove).approve(entity);
    verify(jobDefinitionRepository).save(entity);
  }

  @Test
  @DisplayName("leaves a definition untouched when auto-approval is not configured")
  void autoApproveIfEnabled_withoutConfig_returnsFalseWithoutSaving() {
    assertFalse(service.autoApproveIfEnabled(new JobDefinitionEntity()));

    verifyNoInteractions(jobDefinitionRepository);
  }

  @Test
  @DisplayName("deletes a partially written replacement JAR when the copy fails mid-stream")
  void storeReplacementJar_copyFailsMidStream_leavesNoPartialFile() throws IOException {
    byte[] jarBytes = minimalJar();
    MultipartFile failingFile = mock(MultipartFile.class);
    when(failingFile.getOriginalFilename()).thenReturn("plugin.jar");
    // Each stream yields the first bytes (enough for the header check) and then fails.
    when(failingFile.getInputStream()).thenAnswer(invocation -> failingAfter(jarBytes, 16));

    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () ->
                service.storeReplacementJar(
                    failingFile, "test-job", "2.0.0", "com.example.MyPlugin"));

    assertTrue(ex.getMessage().contains("Failed to store JAR"), ex.getMessage());
    assertEquals(0, filesIn(jarDir), "no partial replacement JAR may remain");
  }

  @Test
  @DisplayName("never deletes an existing file when the unique replacement name collides")
  void storeReplacementJar_nameCollision_keepsExistingFile() throws IOException {
    UUID fixed = UUID.fromString("abcdef12-0000-0000-0000-000000000000");
    Path existing = jarDir.resolve("test-job-2.0.0-abcdef12.jar");
    Files.write(existing, "someone-else".getBytes());

    try (MockedStatic<UUID> uuid = mockStatic(UUID.class)) {
      uuid.when(UUID::randomUUID).thenReturn(fixed);

      assertThrows(
          RuntimeException.class,
          () ->
              service.storeReplacementJar(
                  replacementFile(minimalJar()), "test-job", "2.0.0", "com.example.MyPlugin"));
    }

    assertArrayEquals(
        "someone-else".getBytes(),
        Files.readAllBytes(existing),
        "a colliding file that this call did not write must be kept");
  }

  /** Returns a stream that yields the first {@code limit} bytes and then throws. */
  private static InputStream failingAfter(byte[] bytes, int limit) {
    return new InputStream() {
      private int position;

      @Override
      public int read() throws IOException {
        if (position >= limit) {
          throw new IOException("simulated stream failure");
        }
        return position < bytes.length ? bytes[position++] & 0xFF : -1;
      }
    };
  }

  private static MockMultipartFile replacementFile(byte[] jarBytes) {
    return new MockMultipartFile("file", "plugin.jar", "application/java-archive", jarBytes);
  }

  private static long filesIn(Path dir) throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      return files.count();
    }
  }

  /** Builds a minimal valid (unsigned) JAR with a manifest and one entry. */
  private static byte[] minimalJar() throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (JarOutputStream jar = new JarOutputStream(out, manifest)) {
      jar.putNextEntry(new ZipEntry("com/example/MyPlugin.class"));
      jar.write("stub".getBytes());
      jar.closeEntry();
    }
    return out.toByteArray();
  }
}
