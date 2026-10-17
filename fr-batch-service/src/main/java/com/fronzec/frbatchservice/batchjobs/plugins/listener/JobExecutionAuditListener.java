/* 2024-2026 */
package com.fronzec.frbatchservice.batchjobs.plugins.listener;

import com.fronzec.frbatchservice.batchjobs.plugins.entity.JobDefinitionEntity;
import com.fronzec.frbatchservice.batchjobs.plugins.entity.JobExecutionAuditEntity;
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobDefinitionRepository;
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobExecutionAuditRepository;
import java.time.Duration;
import java.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.stereotype.Component;

/**
 * Spring Batch listener that captures job execution lifecycle events into the
 * {@code job_executions_audit} table.
 *
 * <p>This listener is the single writer of execution audit history. {@code PluginRegistryService}
 * attaches it to every plugin job right before the job enters the {@code JobRegistry}, so every
 * launch path (synchronous or asynchronous, single job or launch-all) is audited the same way.
 *
 * <p>Each execution pair (before → after) produces one audit row:
 * <ul>
 *   <li>{@link #beforeJob(JobExecution)} — creates the row with start metadata</li>
 *   <li>{@link #afterJob(JobExecution)} — updates the row with finish time, duration, and the
 *       execution's raw {@link org.springframework.batch.core.BatchStatus} name as the outcome</li>
 * </ul>
 *
 * <p>The {@link JobDefinitionEntity} is resolved by the job name from the execution's
 * job instance. If no matching definition exists (e.g., a classpath-registered job
 * with no DB row), the execution is not audited: the audit row requires a definition.
 *
 * <p>Audit persistence is best-effort: any failure is logged and never fails the job.
 */
@Component
public class JobExecutionAuditListener implements JobExecutionListener {

  private static final Logger log = LoggerFactory.getLogger(JobExecutionAuditListener.class);

  /** Key used in {@code JobExecution.getExecutionContext()} to pass the audit row ID. */
  private static final String AUDIT_ENTITY_ID_KEY = "_auditEntityId";

  private final JobExecutionAuditRepository auditRepository;
  private final JobDefinitionRepository jobDefinitionRepository;

  /** Creates a listener that persists audit rows for jobs with a matching job definition. */
  public JobExecutionAuditListener(
      JobExecutionAuditRepository auditRepository,
      JobDefinitionRepository jobDefinitionRepository) {
    this.auditRepository = auditRepository;
    this.jobDefinitionRepository = jobDefinitionRepository;
  }

  /**
   * Creates the audit row for a starting execution and stores its ID in the execution context.
   *
   * <p>Any audit ID inherited from a previous execution (restarts copy the execution context) is
   * removed first, so {@link #afterJob(JobExecution)} can never update another execution's row.
   *
   * @param jobExecution the starting execution
   */
  @Override
  public void beforeJob(JobExecution jobExecution) {
    jobExecution.getExecutionContext().remove(AUDIT_ENTITY_ID_KEY);
    String jobName = jobExecution.getJobInstance().getJobName();
    try {
      // Only audit plugin-managed definitions (resolvable by name in our registry)
      JobDefinitionEntity def =
          jobDefinitionRepository.findByJobName(jobName).orElse(null);
      if (def == null) {
        log.debug(
            "No job definition found for '{}' — skipping execution audit", jobName);
        return;
      }

      JobExecutionAuditEntity entity = new JobExecutionAuditEntity();
      entity.setJobDefinitionId(def.getId());
      entity.setJobExecutionId(jobExecution.getId());
      entity.setJobVersion(def.getVersion());
      entity.setExecutionMetadata(
          metadataJson(jobExecution.getStartTime(), null, null, null));

      JobExecutionAuditEntity saved = auditRepository.save(entity);

      // Stash the row ID in the execution context so afterJob can find it
      jobExecution.getExecutionContext().put(AUDIT_ENTITY_ID_KEY, saved.getId());

      log.debug(
          "Audit row created: id={}, jobName={}, executionId={}",
          saved.getId(),
          jobName,
          jobExecution.getId());
    } catch (RuntimeException e) {
      log.warn(
          "Failed to create execution audit row for job '{}' (executionId={}); the job continues"
              + " unaudited",
          jobName,
          jobExecution.getId(),
          e);
    }
  }

  /**
   * Completes the audit row created by {@link #beforeJob(JobExecution)} with the finish time,
   * duration, and outcome. Spring Batch calls this for completed, failed, and stopped executions.
   *
   * @param jobExecution the finished execution
   */
  @Override
  public void afterJob(JobExecution jobExecution) {
    Object rawId = jobExecution.getExecutionContext().get(AUDIT_ENTITY_ID_KEY);
    if (!(rawId instanceof Long entityId)) {
      log.debug(
          "No audit entity ID in execution context for executionId={} — skipping after-job audit",
          jobExecution.getId());
      return;
    }

    try {
      auditRepository
          .findById(entityId)
          .ifPresentOrElse(
              entity -> {
                long durationMs =
                    Duration.between(
                            jobExecution.getStartTime(),
                            jobExecution.getEndTime() != null
                                ? jobExecution.getEndTime()
                                : jobExecution.getStartTime())
                        .toMillis();
                String outcome = jobExecution.getStatus().name();

                entity.setExecutionMetadata(
                    metadataJson(
                        jobExecution.getStartTime(),
                        jobExecution.getEndTime(),
                        durationMs,
                        outcome));
                auditRepository.save(entity);

                log.debug(
                    "Audit row updated: id={}, outcome={}, durationMs={}",
                    entityId,
                    outcome,
                    durationMs);
              },
              () ->
                  log.warn(
                      "Audit entity id={} not found in DB for after-job update (executionId={})",
                      entityId,
                      jobExecution.getId()));
    } catch (RuntimeException e) {
      log.warn(
          "Failed to complete execution audit row id={} for job '{}' (executionId={})",
          entityId,
          jobExecution.getJobInstance().getJobName(),
          jobExecution.getId(),
          e);
    }
  }

  // ── helpers ────────────────────────────────────────────────────────────────

  /**
   * Builds a compact JSON snapshot for {@code execution_metadata}.
   *
   * <p>Uses string concatenation to avoid a dependency on Jackson for the listener.
   */
  static String metadataJson(
      LocalDateTime startedAt, LocalDateTime finishedAt, Long durationMs, String outcome) {
    StringBuilder sb = new StringBuilder(256);
    sb.append('{');

    appendField(sb, "startedAt", startedAt != null ? startedAt.toString() : null);
    if (finishedAt != null) {
      sb.append(',');
      appendField(sb, "finishedAt", finishedAt.toString());
    }
    if (durationMs != null) {
      sb.append(",\"durationMs\":").append(durationMs);
    }
    if (outcome != null) {
      sb.append(",\"outcome\":\"").append(outcome).append('"');
    }

    sb.append('}');
    return sb.toString();
  }

  private static void appendField(StringBuilder sb, String key, String value) {
    sb.append('"').append(key).append("\":\"");
    sb.append(value != null ? value : "");
    sb.append('"');
  }
}
