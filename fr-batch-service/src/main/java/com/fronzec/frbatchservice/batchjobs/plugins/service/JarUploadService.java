/* 2024-2025 */
package com.fronzec.frbatchservice.batchjobs.plugins.service;

import com.fronzec.frbatchservice.batchjobs.plugins.audit.AuditEvent;
import com.fronzec.frbatchservice.batchjobs.plugins.audit.AuditEventType;
import com.fronzec.frbatchservice.batchjobs.plugins.audit.AuditService;
import com.fronzec.frbatchservice.batchjobs.plugins.entity.JobDefinitionEntity;
import com.fronzec.frbatchservice.batchjobs.plugins.metrics.PluginMetrics;
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobDefinitionRepository;
import com.fronzec.frbatchservice.batchjobs.plugins.util.ChecksumUtil;
import com.fronzec.frbatchservice.config.AutoApproveConfig;
import com.fronzec.frbatchservice.web.dto.JarUploadResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

/**
 * Receives a {@link MultipartFile}, validates it is a valid JAR (extension + ZIP/PK
 * magic bytes), computes its SHA-256 checksum, stores it to disk, and persists
 * metadata via {@link JobDefinitionRepository}.
 *
 * <p>Uploaded JARs default to {@code enabled=false} and {@code loadStatus=UNLOADED}.
 * Dynamic registration from the stored JAR is handled in Phase 4.
 */
@Service
public class JarUploadService {

    private static final Logger log = LoggerFactory.getLogger(JarUploadService.class);

    /** ZIP file header magic bytes: {@code PK\x03\x04}. */
    private static final byte[] ZIP_MAGIC = {(byte) 0x50, (byte) 0x4B, (byte) 0x03, (byte) 0x04};

    /** Allowed characters in job name and version used as path components. */
    private static final Pattern SAFE_NAME = Pattern.compile("[a-zA-Z0-9._\\-]+");

    /**
     * Fully-qualified Java class name pattern.
     * Matches names like {@code com.fronzec.plugins.ticketpdf.TicketPdfJobPlugin}.
     * Requires at least one package segment followed by a simple class name.
     */
    private static final Pattern FQCN =
        Pattern.compile("([a-zA-Z_$][a-zA-Z0-9_$]*\\.)+[a-zA-Z_$][a-zA-Z0-9_$]*");

  private final JobDefinitionRepository jobDefinitionRepository;
  private final Optional<AutoApproveConfig> autoApproveConfig;
  private final JarSignatureVerifier jarSignatureVerifier;
  private final AuditService auditService;
  private final PluginMetrics pluginMetrics;
  private final boolean signatureStrict;
  private final String jarDir;

  public JarUploadService(
      JobDefinitionRepository jobDefinitionRepository,
      Optional<AutoApproveConfig> autoApproveConfig,
      JarSignatureVerifier jarSignatureVerifier,
      AuditService auditService,
      PluginMetrics pluginMetrics,
      @Value("${app.plugins.signature.mode:permissive}") String signatureMode,
      @Value("${fr-batch-service.plugins.jar-dir}") String jarDir) {
    this.jobDefinitionRepository = jobDefinitionRepository;
    this.autoApproveConfig = autoApproveConfig;
    this.jarSignatureVerifier = jarSignatureVerifier;
    this.auditService = auditService;
    this.pluginMetrics = pluginMetrics;
    this.signatureStrict = "strict".equalsIgnoreCase(signatureMode);
    this.jarDir = jarDir;
  }

    /**
     * Validates, stores, and registers a new JAR plugin.
     *
     * @param file          the uploaded JAR file
     * @param jobName       unique job name for the definition
     * @param version       semantic version string
     * @param mainClassName fully-qualified class name of the {@code BatchJobPlugin} impl
     * @return response DTO with persisted metadata
     * @throws InvalidJarException              if the file fails validation
     * @throws DuplicateJobDefinitionException  if a definition with {@code jobName} already exists
     */
    public JarUploadResponse uploadJar(
            MultipartFile file, String jobName, String version, String mainClassName) {

        String userId = AuditService.currentUserId();
        try {

        validateJar(file);
        validateMainClassName(mainClassName);

        checkDuplicate(jobName);

    Path storedPath = storeFile(file, jobName, version);

    // Compute the checksum from the persisted file (not the multipart stream) so the
    // stored digest matches the bytes that load-time integrity verification re-checks.
    String checksum = ChecksumUtil.computeSha256(storedPath);

    // ── signature verification ───────────────────────────────────────────────
    SignatureResult sigResult = jarSignatureVerifier.verify(storedPath);
    if (!sigResult.valid()) {
      List<String> warnings = sigResult.warnings();
      if (signatureStrict) {
        throw new InvalidJarException(
            "JAR signature verification failed in strict mode: " + String.join("; ", warnings));
      }
      // Permissive: log and allow upload
      log.warn(
          "JAR signature verification issues (permissive mode — upload allowed): {}",
          warnings);
    }

    JobDefinitionEntity entity = persistMetadata(jobName, version, mainClassName, checksum, storedPath);

        pluginMetrics.incrementUpload();

        // Auto-approve in non-production profiles (no-op when AutoApproveConfig is absent).
        // approve() only mutates the entity in memory, and persistMetadata's save has
        // already committed it as PENDING, so re-save to persist the approval.
        autoApproveConfig.ifPresent(
            ac -> {
              ac.approve(entity);
              jobDefinitionRepository.save(entity);
            });

        log.info(
                "JAR uploaded: job={}, version={}, id={}, checksum={}",
                jobName, version, entity.getId(), checksum);

        // ── audit: success ─────────────────────────────────────────────────
        auditService.logEvent(
            new AuditEvent(
                AuditEventType.JAR_UPLOADED,
                jobName,
                userId,
                "uploaded version " + version + " (id=" + entity.getId() + ")",
                AuditEvent.SUCCESS,
                LocalDateTime.now()));

        return JarUploadResponse.fromEntity(entity);

        } catch (Exception e) {
          auditService.logEvent(
              new AuditEvent(
                  AuditEventType.JAR_UPLOADED,
                  jobName,
                  userId,
                  "upload failed: " + e.getMessage(),
                  AuditEvent.FAILURE,
                  LocalDateTime.now()));
          throw e;
        }
    }

    // ─── validation ────────────────────────────────────────────────────────────

    /** Rejects files without {@code .jar} extension or without ZIP magic bytes. */
    private void validateJar(MultipartFile file) {
        String filename = file.getOriginalFilename();
        if (filename == null || !filename.toLowerCase().endsWith(".jar")) {
            throw new InvalidJarException(
                    "File must have .jar extension; received: " + (filename != null ? filename : "null"));
        }

        byte[] header = readHeader(file, 4);
        if (!Arrays.equals(header, ZIP_MAGIC)) {
            throw new InvalidJarException(
                    "File is not a valid JAR — missing ZIP/PK magic bytes. "
                            + "Expected 0x504B0304, got: "
                            + HexFormat.of().formatHex(header));
        }
    }

    /**
     * Validates that {@code mainClassName} is a fully-qualified Java class name.
     *
     * <p>Requires at least one package segment (e.g. {@code com.example.MyClass}).
     * A bare simple name ({@code MyClass}) is rejected because a plugin entry-point
     * without a package would conflict with the default package and is almost always
     * an input error.
     *
     * @param mainClassName the class name to validate
     * @throws InvalidJarException if the value is blank or does not match the FQCN pattern
     */
    private void validateMainClassName(String mainClassName) {
        if (mainClassName == null || mainClassName.isBlank()) {
            throw new InvalidJarException("mainClassName must not be blank");
        }
        if (!FQCN.matcher(mainClassName).matches()) {
            throw new InvalidJarException(
                "mainClassName must be a fully-qualified Java class name "
                    + "(e.g. com.example.MyClass): " + mainClassName);
        }
    }

    /** Reads up to {@code n} bytes from the beginning of the file. */
    private byte[] readHeader(MultipartFile file, int n) {
        try (InputStream in = file.getInputStream()) {
            byte[] buf = new byte[n];
            int read = in.read(buf);
            if (read == -1) {
                throw new InvalidJarException("File is empty — cannot read magic bytes");
            }
            if (read < n) {
                byte[] truncated = new byte[read];
                System.arraycopy(buf, 0, truncated, 0, read);
                return truncated;
            }
            return buf;
        } catch (IOException e) {
            throw new InvalidJarException("Failed to read file contents: " + e.getMessage());
        }
    }

  // ─── duplicate detection ──────────────────────────────────────────────────

    /**
     * Fast-fail hint only. The definitive duplicate guard is the database
     * {@code UNIQUE KEY} on {@code job_name}, enforced atomically at persist
     * time by catching {@link DataIntegrityViolationException}.
     */
    private void checkDuplicate(String jobName) {
        if (jobDefinitionRepository.findByJobName(jobName).isPresent()) {
            throw new DuplicateJobDefinitionException(
                    "A job definition with name '" + jobName + "' already exists");
        }
    }

    // ─── file storage ──────────────────────────────────────────────────────────

    /** Stores the JAR to {@code {jarDir}/{jobName}-{version}.jar}. */
    private Path storeFile(MultipartFile file, String jobName, String version) {
        String safeJobName = sanitizeNameComponent(jobName);
        String safeVersion = sanitizeNameComponent(version);

        Path dir = Path.of(jarDir).toAbsolutePath().normalize();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create JAR storage directory: " + jarDir, e);
        }

        String filename = safeJobName + "-" + safeVersion + ".jar";
        Path target = dir.resolve(filename);

        // Defence-in-depth: verify the resolved path is still under jarDir
        if (!target.normalize().startsWith(dir.normalize())) {
            throw new InvalidJarException(
                    "Job name or version would escape the storage directory: "
                            + "jobName=" + jobName + ", version=" + version);
        }

        try {
            Files.copy(file.getInputStream(), target, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed to store JAR at " + target, e);
        }

        log.debug("JAR stored at {}", target);
        return target;
    }

    /** Rejects names that contain path separators or unsafe characters. */
    static String sanitizeNameComponent(String value) {
        if (value == null || value.isBlank()) {
            throw new InvalidJarException("Job name and version must not be blank");
        }
        if (!SAFE_NAME.matcher(value).matches()) {
            throw new InvalidJarException(
                    "Job name and version must contain only alphanumeric, dot, dash, or underscore: "
                            + value);
        }
        return value;
    }

    // ─── persistence ───────────────────────────────────────────────────────────

    /**
     * Creates a {@link JobDefinitionEntity} with default values:
     * {@code enabled=false}, {@code loadStatus=UNLOADED}.
     * <p>
     * The database {@code UNIQUE KEY uk_job_name} provides the atomic duplicate
     * guard. If a concurrent upload slips past the fast-fail hint in
     * {@link #checkDuplicate(String)}, the constraint will catch it here.
     */
    private JobDefinitionEntity persistMetadata(
            String jobName, String version, String mainClassName, String checksum, Path storedPath) {

        JobDefinitionEntity entity = new JobDefinitionEntity();
        entity.setJobName(jobName);
        entity.setDisplayName(jobName);
        entity.setVersion(version);
        entity.setMainClassName(mainClassName);
        entity.setJarChecksum(checksum);
        entity.setJarFilePath(storedPath.toString());
        entity.setEnabled(false);
        entity.setAutoStart(false);
        entity.setLoadStatus("UNLOADED");
        entity.setCreatedBy(AuditService.currentUserId());

        try {
            return jobDefinitionRepository.save(entity);
        } catch (DataIntegrityViolationException e) {
            throw new DuplicateJobDefinitionException(
                    "A job definition with name '" + jobName + "' already exists", e);
        }
    }
}
