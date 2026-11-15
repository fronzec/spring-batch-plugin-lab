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
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.jar.JarFile;
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
 *
 * <p>Also validates and stores replacement JARs for existing definitions (see
 * {@link #storeReplacementJar}); the replacement itself is coordinated by
 * {@code DynamicJobLoaderService#replaceJar}.
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

  /**
   * A validated replacement JAR stored on disk but not yet referenced by any definition.
   *
   * @param path the absolute path of the stored file
   * @param checksum the SHA-256 checksum computed from the stored file
   */
  public record StoredJar(Path path, String checksum) {}

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
     * <p>The JAR is stored under a unique file name, {@code {jarDir}/{jobName}-{version}-{random}.jar},
     * without replacing any existing file, so a concurrent upload can never overwrite a JAR that
     * another definition references. If a check fails after the file was stored, or the database
     * unique key rejects the definition as a duplicate, the file this call stored is deleted. Any
     * other failure while saving the definition has an unknown outcome, so the file is kept.
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

    Path storedPath = storeUniqueFile(file, jobName, version);

    String checksum;
    try {
        // Compute the checksum from the persisted file (not the multipart stream) so the
        // stored digest matches the bytes that load-time integrity verification re-checks.
        checksum = ChecksumUtil.computeSha256(storedPath);

        verifySignature(storedPath);
    } catch (RuntimeException e) {
        // No definition references the file this call stored, so it must not stay on disk.
        deleteRejectedJar(storedPath, e);
        throw e;
    }

    JobDefinitionEntity entity;
    try {
        entity = persistMetadata(jobName, version, mainClassName, checksum, storedPath);
    } catch (DuplicateJobDefinitionException e) {
        // The unique key rejected the insert, so no definition references this file.
        deleteRejectedJar(storedPath, e);
        throw e;
    } catch (RuntimeException e) {
        // The insert may have committed before the failure surfaced, so the file is kept.
        log.warn(
                "Saving definition '{}' failed with an unknown outcome; keeping its JAR, possibly"
                        + " as an orphan: {}",
                jobName,
                storedPath);
        throw e;
    }

        pluginMetrics.incrementUpload();

        autoApproveIfEnabled(entity);

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

    /**
     * Validates a replacement JAR for an existing definition and stores it under a unique
     * file name, {@code {jarDir}/{jobName}-{version}-{random}.jar}.
     *
     * <p>Runs the same checks as {@link #uploadJar}: {@code .jar} extension, ZIP magic bytes,
     * fully-qualified main class name, a checksum computed from the stored bytes, and the
     * configured signature policy. In addition, the JAR must contain the main class entry.
     * No plugin code is executed. The file is created without replacing any existing file,
     * so the definition's current JAR is never overwritten. If a check fails after the file
     * was stored, the file is deleted before the exception propagates.
     *
     * <p>This method does not touch the database or the audit trail; the caller owns both.
     *
     * @param file          the uploaded replacement JAR
     * @param jobName       the job name of the definition being replaced
     * @param version       the version of the replacement
     * @param mainClassName fully-qualified class name of the {@code BatchJobPlugin} impl
     * @return the stored file and its checksum
     * @throws InvalidJarException if the file fails validation
     */
    public StoredJar storeReplacementJar(
            MultipartFile file, String jobName, String version, String mainClassName) {
        validateJar(file);
        validateMainClassName(mainClassName);

        Path storedPath = storeUniqueFile(file, jobName, version);
        try {
            requireMainClassEntry(storedPath, mainClassName);
            String checksum = ChecksumUtil.computeSha256(storedPath);
            verifySignature(storedPath);
            log.debug("Replacement JAR stored at {} (checksum={})", storedPath, checksum);
            return new StoredJar(storedPath, checksum);
        } catch (RuntimeException e) {
            deleteRejectedJar(storedPath, e);
            throw e;
        }
    }

    /**
     * Deletes a JAR this service stored but no definition references. A failed delete is attached
     * to {@code cause} as a suppressed exception and logged, so {@code cause} stays the reported
     * failure.
     */
    private static void deleteRejectedJar(Path storedPath, RuntimeException cause) {
        try {
            Files.deleteIfExists(storedPath);
        } catch (IOException | SecurityException deleteEx) {
            cause.addSuppressed(deleteEx);
            log.warn("Could not delete rejected JAR {}: {}", storedPath, deleteEx.toString());
        }
    }

    /**
     * Applies the non-production auto-approval to a persisted definition and saves it again.
     *
     * <p>A no-op when {@link AutoApproveConfig} is absent (production, or auto-approval
     * disabled). {@code approve()} only mutates the entity in memory, so the entity is
     * re-saved to persist the approval.
     *
     * @param entity the persisted definition to approve
     * @return {@code true} if the definition was auto-approved
     */
    public boolean autoApproveIfEnabled(JobDefinitionEntity entity) {
        if (autoApproveConfig.isEmpty()) {
            return false;
        }
        autoApproveConfig.get().approve(entity);
        jobDefinitionRepository.save(entity);
        return true;
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

    /**
     * Requires the stored JAR to contain the class file of {@code mainClassName}, without
     * loading or executing it.
     *
     * @throws InvalidJarException if the entry is missing or the JAR cannot be read
     */
    private void requireMainClassEntry(Path jarPath, String mainClassName) {
        String entryName = mainClassName.replace('.', '/') + ".class";
        try (JarFile jar = new JarFile(jarPath.toFile())) {
            if (jar.getEntry(entryName) == null) {
                throw new InvalidJarException(
                        "Main class " + mainClassName + " not found in JAR (expected entry " + entryName + ")");
            }
        } catch (IOException e) {
            throw new InvalidJarException("File is not a readable JAR: " + e.getMessage());
        }
    }

    /**
     * Applies the configured signature policy: strict mode rejects an invalid signature,
     * permissive mode logs the issues and allows the JAR.
     *
     * @throws InvalidJarException if verification fails in strict mode
     */
    private void verifySignature(Path storedPath) {
        SignatureResult sigResult = jarSignatureVerifier.verify(storedPath);
        if (!sigResult.valid()) {
            List<String> warnings = sigResult.warnings();
            if (signatureStrict) {
                throw new InvalidJarException(
                        "JAR signature verification failed in strict mode: " + String.join("; ", warnings));
            }
            log.warn(
                    "JAR signature verification issues (permissive mode — JAR allowed): {}",
                    warnings);
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

    /**
     * Stores the JAR to {@code {jarDir}/{jobName}-{version}-{random}.jar}. The file is created
     * without replacing an existing one: a name collision fails and leaves the existing file
     * untouched.
     */
    private Path storeUniqueFile(MultipartFile file, String jobName, String version) {
        String suffix = "-" + UUID.randomUUID().toString().substring(0, 8);
        String safeJobName = sanitizeNameComponent(jobName);
        String safeVersion = sanitizeNameComponent(version);

        Path dir = Path.of(jarDir).toAbsolutePath().normalize();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create JAR storage directory: " + jarDir, e);
        }

        String filename = safeJobName + "-" + safeVersion + suffix + ".jar";
        Path target = dir.resolve(filename);

        // Defence-in-depth: verify the resolved path is still under jarDir
        if (!target.normalize().startsWith(dir.normalize())) {
            throw new InvalidJarException(
                    "Job name or version would escape the storage directory: "
                            + "jobName=" + jobName + ", version=" + version);
        }

        try {
            // No REPLACE_EXISTING: a name collision fails instead of overwriting a stored JAR.
            Files.copy(file.getInputStream(), target);
        } catch (FileAlreadyExistsException e) {
            // Nothing was written: the existing file belongs to someone else and must be kept.
            throw new RuntimeException("Failed to store JAR at " + target, e);
        } catch (IOException e) {
            // A copy that fails mid-stream can leave a partial file; remove it before propagating.
            try {
                Files.deleteIfExists(target);
            } catch (IOException | SecurityException deleteEx) {
                e.addSuppressed(deleteEx);
            }
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
