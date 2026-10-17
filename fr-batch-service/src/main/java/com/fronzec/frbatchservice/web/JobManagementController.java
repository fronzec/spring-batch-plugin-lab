/* 2024-2025 */
package com.fronzec.frbatchservice.web;

import com.fronzec.frbatchservice.batchjobs.plugins.audit.AuditEvent;
import com.fronzec.frbatchservice.batchjobs.plugins.audit.AuditEventType;
import com.fronzec.frbatchservice.batchjobs.plugins.audit.AuditService;
import com.fronzec.frbatchservice.batchjobs.plugins.entity.JobDefinitionEntity;
import com.fronzec.frbatchservice.batchjobs.plugins.loader.DynamicJobLoaderService;
import com.fronzec.frbatchservice.batchjobs.plugins.loader.LoadResult;
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobDefinitionRepository;
import com.fronzec.frbatchservice.batchjobs.plugins.service.JarUploadService;
import com.fronzec.frbatchservice.web.dto.ApprovalRequest;
import com.fronzec.frbatchservice.web.dto.JarUploadResponse;
import com.fronzec.frbatchservice.web.dto.JobDefinitionResponse;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

/**
 * REST API for managing uploaded job definitions.
 *
 * <p>Effective URL prefix (with context-path): {@code /api/batch-service/jobs}
 *
 * <p>Exception handling is delegated to {@link GlobalExceptionHandler}, which
 * provides consistent {@link ErrorResponse} envelopes for validation failures,
 * duplicate conflicts, not-found lookups, and unexpected server errors.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>{@code POST /upload} — upload a JAR and register a job definition</li>
 *   <li>{@code GET  /definitions} — list all job definitions</li>
 *   <li>{@code GET  /definitions/{id}} — get single definition</li>
 *   <li>{@code PUT  /definitions/{id}/enable} — enable a definition</li>
 *   <li>{@code PUT  /definitions/{id}/disable} — disable a definition</li>
 *   <li>{@code PUT  /definitions/{id}/jar} — replace the JAR in place (same id, re-approval)</li>
 *   <li>{@code DELETE /definitions/{id}[?force=true]} — unload, delete DB rows, then remove JAR</li>
 *   <li>{@code POST /definitions/{id}/load} — load plugin from JAR into registry</li>
 *   <li>{@code POST /definitions/{id}/unload} — unregister plugin and close classloader</li>
 *   <li>{@code POST /definitions/{id}/reload} — atomic unload + load</li>
 *   <li>{@code POST /load-all} — load all enabled, not-yet-loaded definitions</li>
 * </ul>
 */
@RestController
@RequestMapping("/jobs")
public class JobManagementController {

    private static final Logger log = LoggerFactory.getLogger(JobManagementController.class);

    private final JarUploadService jarUploadService;
    private final JobDefinitionRepository jobDefinitionRepository;
    private final DynamicJobLoaderService loaderService;
    private final AuditService auditService;

    public JobManagementController(
            JarUploadService jarUploadService,
            JobDefinitionRepository jobDefinitionRepository,
            DynamicJobLoaderService loaderService,
            AuditService auditService) {
        this.jarUploadService = jarUploadService;
        this.jobDefinitionRepository = jobDefinitionRepository;
        this.loaderService = loaderService;
        this.auditService = auditService;
    }

    /**
     * Uploads a JAR file and registers a new job definition.
     *
     * <p>On success, returns {@code 201 Created} with the persisted metadata.
     * Validation failures and duplicate job names are handled by
     * {@link GlobalExceptionHandler}.
     */
    @PostMapping("/upload")
    public ResponseEntity<JarUploadResponse> uploadJar(
            @RequestParam("file") MultipartFile file,
            @RequestParam("jobName") String jobName,
            @RequestParam("version") String version,
            @RequestParam("mainClassName") String mainClassName) {

        JarUploadResponse response =
                jarUploadService.uploadJar(file, jobName, version, mainClassName);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    // ─── CRUD: list definitions ────────────────────────────────────────────────

    /**
     * Lists all job definitions stored in the database.
     *
     * <p>Returns a (possibly empty) list of {@link JobDefinitionResponse} DTOs.
     */
    @GetMapping("/definitions")
    public ResponseEntity<List<JobDefinitionResponse>> listDefinitions() {
        List<JobDefinitionResponse> definitions = jobDefinitionRepository.findAll().stream()
                .map(JobDefinitionResponse::fromEntity)
                .toList();
        log.debug("Returning {} job definitions", definitions.size());
        return ResponseEntity.ok(definitions);
    }

    /**
     * Returns a single job definition by ID, or {@code 404 Not Found}.
     */
    @GetMapping("/definitions/{id}")
    public ResponseEntity<?> getDefinition(@PathVariable long id) {
        return jobDefinitionRepository
                .findById(id)
                .map(entity -> ResponseEntity.ok(JobDefinitionResponse.fromEntity(entity)))
                .orElseGet(() -> {
                    log.warn("Job definition not found: id={}", id);
                    return ResponseEntity.notFound().build();
                });
    }

    // ─── CRUD: enable / disable ────────────────────────────────────────────────

    /**
     * Enables a job definition by flipping {@code enabled = true}.
     *
     * <p>Returns the updated definition on success, or {@code 404 Not Found}
     * if no definition exists with the given ID.
     */
    @PutMapping("/definitions/{id}/enable")
    public ResponseEntity<?> enableDefinition(@PathVariable long id) {
        Optional<JobDefinitionEntity> opt = jobDefinitionRepository.findById(id);
        if (opt.isEmpty()) {
            log.warn("Cannot enable — job definition not found: id={}", id);
            return ResponseEntity.notFound().build();
        }

        JobDefinitionEntity entity = opt.get();
        entity.setEnabled(true);
        JobDefinitionEntity saved = jobDefinitionRepository.save(entity);
        log.info("Job definition enabled: id={}, jobName={}", saved.getId(), saved.getJobName());

        auditService.logEvent(
            new AuditEvent(
                AuditEventType.JOB_ENABLED,
                saved.getJobName(),
                AuditService.currentUserId(),
                "Job enabled: id=" + saved.getId(),
                AuditEvent.SUCCESS,
                LocalDateTime.now()));

        return ResponseEntity.ok(JobDefinitionResponse.fromEntity(saved));
    }

    /**
     * Disables a job definition by flipping {@code enabled = false}.
     *
     * <p>Returns the updated definition on success, or {@code 404 Not Found}
     * if no definition exists with the given ID.
     */
    @PutMapping("/definitions/{id}/disable")
    public ResponseEntity<?> disableDefinition(@PathVariable long id) {
        Optional<JobDefinitionEntity> opt = jobDefinitionRepository.findById(id);
        if (opt.isEmpty()) {
            log.warn("Cannot disable — job definition not found: id={}", id);
            return ResponseEntity.notFound().build();
        }

        JobDefinitionEntity entity = opt.get();
        entity.setEnabled(false);
        JobDefinitionEntity saved = jobDefinitionRepository.save(entity);
        log.info("Job definition disabled: id={}, jobName={}", saved.getId(), saved.getJobName());

        auditService.logEvent(
            new AuditEvent(
                AuditEventType.JOB_DISABLED,
                saved.getJobName(),
                AuditService.currentUserId(),
                "Job disabled: id=" + saved.getId(),
                AuditEvent.SUCCESS,
                LocalDateTime.now()));

        return ResponseEntity.ok(JobDefinitionResponse.fromEntity(saved));
    }

    // ─── Approval workflow ────────────────────────────────────────────────────

    /**
     * Approves a job definition, setting its status to {@code APPROVED} and recording
     * the approver and timestamp.
     *
     * <p>The approver identity is always derived from the authenticated principal
     * (via {@link AuditService#currentUserId()}). Any {@code approvedBy} field
     * supplied in the request body is accepted for backward compatibility with existing
     * scripts (e.g. {@code load-plugins.hurl}) but is intentionally ignored — it
     * cannot be trusted because callers can forge any value.
     *
     * <p>Returns {@code 200 OK} with the updated definition on success.
     * {@code 404 Not Found} if the definition does not exist.
     * {@code 409 Conflict} if the definition is already approved.
     */
    @PutMapping("/definitions/{id}/approve")
    public ResponseEntity<?> approveDefinition(
            @PathVariable long id,
            @RequestBody(required = false) ApprovalRequest request) {
        Optional<JobDefinitionEntity> opt = jobDefinitionRepository.findById(id);
        if (opt.isEmpty()) {
            log.warn("Cannot approve — job definition not found: id={}", id);
            return ResponseEntity.notFound().build();
        }

        JobDefinitionEntity entity = opt.get();

        if ("APPROVED".equals(entity.getApprovalStatus())) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of(
                            "error", "Already approved",
                            "message", "Job definition " + id + " is already approved"));
        }

        // Derive the approver from the authenticated principal, not from the request body.
        // The body field is kept for backward-compat deserialization but its value is discarded.
        String approver = AuditService.currentUserId();
        entity.setApprovalStatus("APPROVED");
        entity.setApprovedBy(approver);
        entity.setApprovedAt(LocalDateTime.now());
        JobDefinitionEntity saved = jobDefinitionRepository.save(entity);
        log.info(
                "Job definition approved: id={}, jobName={}, approvedBy={}",
                saved.getId(),
                saved.getJobName(),
                approver);

        auditService.logEvent(
            new AuditEvent(
                AuditEventType.JOB_APPROVED,
                saved.getJobName(),
                approver,
                "Approved by " + approver,
                AuditEvent.SUCCESS,
                LocalDateTime.now()));

        return ResponseEntity.ok(JobDefinitionResponse.fromEntity(saved));
    }

    /**
     * Rejects a job definition, setting its status to {@code REJECTED} and recording
     * the reviewer and timestamp.
     *
     * <p>The reviewer identity is always derived from the authenticated principal
     * (via {@link AuditService#currentUserId()}). Any {@code approvedBy} field
     * supplied in the request body is accepted for backward compatibility but is
     * intentionally ignored — it cannot be trusted because callers can forge any value.
     *
     * <p>Returns {@code 200 OK} with the updated definition on success.
     * {@code 404 Not Found} if the definition does not exist.
     */
    @PutMapping("/definitions/{id}/reject")
    public ResponseEntity<?> rejectDefinition(
            @PathVariable long id,
            @RequestBody(required = false) ApprovalRequest request) {
        Optional<JobDefinitionEntity> opt = jobDefinitionRepository.findById(id);
        if (opt.isEmpty()) {
            log.warn("Cannot reject — job definition not found: id={}", id);
            return ResponseEntity.notFound().build();
        }

        // Derive the reviewer from the authenticated principal, not from the request body.
        // The body field is kept for backward-compat deserialization but its value is discarded.
        String reviewer = AuditService.currentUserId();
        JobDefinitionEntity entity = opt.get();
        entity.setApprovalStatus("REJECTED");
        entity.setApprovedBy(reviewer);
        entity.setApprovedAt(LocalDateTime.now());
        JobDefinitionEntity saved = jobDefinitionRepository.save(entity);
        log.info(
                "Job definition rejected: id={}, jobName={}, rejectedBy={}",
                saved.getId(),
                saved.getJobName(),
                reviewer);

        auditService.logEvent(
            new AuditEvent(
                AuditEventType.JOB_REJECTED,
                saved.getJobName(),
                reviewer,
                "Rejected by " + reviewer,
                AuditEvent.SUCCESS,
                LocalDateTime.now()));

        return ResponseEntity.ok(JobDefinitionResponse.fromEntity(saved));
    }

    // ─── JAR replacement ───────────────────────────────────────────────────────

    /**
     * Replaces the JAR of an existing job definition in place, delegated to
     * {@link DynamicJobLoaderService#replaceJar}. The definition keeps its ID, so its
     * execution history and parameter templates stay linked.
     *
     * <p>The plugin is unloaded first, the new JAR is validated like an upload and stored
     * under a unique file name, and one transaction updates the JAR path, checksum,
     * version, and main class and resets the approval to {@code PENDING} (non-production
     * profiles re-apply auto-approval). The definition stays unloaded and must be approved
     * and loaded again. The previous JAR is deleted after commit.
     *
     * <p>Returns {@code 200 OK} with the updated definition, {@code 404 Not Found} if the
     * definition does not exist, {@code 409 Conflict} if the job has running executions
     * (nothing is changed), and {@code 400 Bad Request} if the JAR fails validation (the
     * definition keeps its JAR but stays unloaded).
     *
     * @param id            the job definition ID
     * @param file          the replacement JAR
     * @param version       the version of the replacement
     * @param mainClassName fully-qualified class name of the {@code BatchJobPlugin} impl
     * @return {@code 200 OK} with the updated definition
     */
    @PutMapping("/definitions/{id}/jar")
    public ResponseEntity<JobDefinitionResponse> replaceJar(
            @PathVariable long id,
            @RequestParam("file") MultipartFile file,
            @RequestParam("version") String version,
            @RequestParam("mainClassName") String mainClassName) {
        JobDefinitionEntity updated = loaderService.replaceJar(id, file, version, mainClassName);
        return ResponseEntity.ok(JobDefinitionResponse.fromEntity(updated));
    }

    // ─── CRUD: delete ──────────────────────────────────────────────────────────

    /**
     * Deletes a job definition as one lifecycle-coordinated operation, delegated to
     * {@link DynamicJobLoaderService#deleteJob(Long, boolean)}: the plugin is unloaded
     * first (unregistered and its classloader released), the parameter templates and
     * definition row are deleted in one database transaction, and the JAR file is
     * removed from disk only after that transaction commits.
     *
     * <p>Returns {@code 204 No Content} on success, {@code 404 Not Found} if the
     * definition does not exist, and {@code 409 Conflict} if the definition has
     * execution history (disable it instead; {@code force} does not bypass this) or
     * if the job has running executions and {@code force} is {@code false}. On a
     * conflict nothing is unloaded or deleted. A missing JAR file, or a JAR that
     * cannot be removed after commit, does not fail the request; an orphan JAR is
     * logged and named in the {@code JOB_DELETED} audit.
     *
     * @param id    the job definition ID
     * @param force if {@code true}, delete even when executions are running; running
     *              executions are not stopped, and the execution-history guard still
     *              applies
     * @return {@code 204 No Content} on success
     */
    @DeleteMapping("/definitions/{id}")
    public ResponseEntity<Void> deleteDefinition(
            @PathVariable long id, @RequestParam(defaultValue = "false") boolean force) {
        loaderService.deleteJob(id, force);
        return ResponseEntity.noContent().build();
    }

    // ─── Dynamic plugin loading / unloading ────────────────────────────────────

    /**
     * Loads a job definition from its JAR, instantiates the plugin, and registers it
     * with the job registry.
     *
     * <p>Returns {@code 200 OK} with {@link LoadResult} on success.
     * {@code 404 Not Found} if the definition does not exist (via
     * {@link GlobalExceptionHandler#handleNotFound}).
     * {@code 409 Conflict} if the definition is disabled or already loaded (via
     * {@code IllegalStateException}).
     * {@code 400 Bad Request} if the JAR is invalid or the plugin class is missing
     * (via {@code JobLoadException}).
     */
    @PostMapping("/definitions/{id}/load")
    public ResponseEntity<LoadResult> loadJob(@PathVariable long id) {
        LoadResult result = loaderService.loadJob(id);
        return ResponseEntity.ok(result);
    }

    /**
     * Unloads a previously loaded job: unregisters from the registry, closes the
     * classloader, and marks the definition as {@code UNLOADED}.
     *
     * <p>Returns {@code 200 OK} with {@link LoadResult} on success.
     * {@code 404 Not Found} if the definition does not exist.
     * {@code 409 Conflict} if the job has running executions and {@code force} is
     * {@code false} (via {@link
     * com.fronzec.frbatchservice.batchjobs.plugins.loader.JobUnloadConflictException}).
     */
    @PostMapping("/definitions/{id}/unload")
    public ResponseEntity<LoadResult> unloadJob(
            @PathVariable long id,
            @RequestParam(defaultValue = "false") boolean force) {
        LoadResult result = loaderService.unloadJob(id, force);
        return ResponseEntity.ok(result);
    }

    /**
     * Reloads a job by unloading (forced) then loading sequentially. The operation is
     * sequential, so the status never appears partially ready.
     *
     * <p>Returns {@code 200 OK} with {@link LoadResult} on success.
     * {@code 404 Not Found} if the definition does not exist.
     * {@code 400 Bad Request} if the load step fails (via {@code JobLoadException}).
     */
    @PostMapping("/definitions/{id}/reload")
    public ResponseEntity<LoadResult> reloadJob(@PathVariable long id) {
        LoadResult result = loaderService.reloadJob(id);
        return ResponseEntity.ok(result);
    }

    /**
     * Loads all enabled job definitions whose {@code loadStatus} is not already
     * {@code LOADED}. Each definition is loaded independently; one failure does not
     * block the rest.
     *
     * <p>Returns {@code 200 OK} with a map from job name to per-definition
     * {@link LoadResult}.
     */
    @PostMapping("/load-all")
    public ResponseEntity<Map<String, LoadResult>> loadAllEnabled() {
        Map<String, LoadResult> results = loaderService.loadAllEnabled();
        log.info(
                "load-all complete: {} definitions processed",
                results.size());
        return ResponseEntity.ok(results);
    }
}
