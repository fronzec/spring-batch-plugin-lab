/* 2024-2026 */
package com.fronzec.frbatchservice.batchjobs.plugins.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fronzec.frbatchservice.batchjobs.plugins.entity.JobDefinitionEntity;
import com.fronzec.frbatchservice.batchjobs.plugins.entity.JobExecutionAuditEntity;
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobDefinitionRepository;
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobExecutionAuditRepository;
import java.time.LocalDateTime;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;

@ExtendWith(MockitoExtension.class)
class JobExecutionAuditListenerTest {

  private static final String JOB_NAME = "audited-job";
  private static final String AUDIT_ENTITY_ID_KEY = "_auditEntityId";
  private static final LocalDateTime STARTED_AT = LocalDateTime.of(2026, 10, 16, 10, 0, 0);
  private static final LocalDateTime FINISHED_AT = STARTED_AT.plusSeconds(3);

  @Mock private JobExecutionAuditRepository auditRepository;
  @Mock private JobDefinitionRepository jobDefinitionRepository;

  private JobExecutionAuditListener listener;
  private JobExecution execution;

  @BeforeEach
  void setUp() {
    listener = new JobExecutionAuditListener(auditRepository, jobDefinitionRepository);
    execution = new JobExecution(77L, new JobInstance(5L, JOB_NAME), new JobParameters());
    execution.setStartTime(STARTED_AT);
  }

  private JobDefinitionEntity definition() {
    JobDefinitionEntity def = new JobDefinitionEntity();
    def.setId(9L);
    def.setJobName(JOB_NAME);
    def.setVersion("1.2.3");
    return def;
  }

  private void stubSaveAssigningId(long id) {
    when(auditRepository.save(any(JobExecutionAuditEntity.class)))
        .thenAnswer(
            invocation -> {
              JobExecutionAuditEntity entity = invocation.getArgument(0);
              if (entity.getId() == null) {
                entity.setId(id);
              }
              return entity;
            });
  }

  @Test
  void beforeJob_withDefinition_savesRowAndStoresItsIdInContext() {
    when(jobDefinitionRepository.findByJobName(JOB_NAME)).thenReturn(Optional.of(definition()));
    stubSaveAssigningId(42L);

    listener.beforeJob(execution);

    ArgumentCaptor<JobExecutionAuditEntity> captor =
        ArgumentCaptor.forClass(JobExecutionAuditEntity.class);
    verify(auditRepository).save(captor.capture());
    JobExecutionAuditEntity saved = captor.getValue();
    assertThat(saved.getJobDefinitionId()).isEqualTo(9L);
    assertThat(saved.getJobExecutionId()).isEqualTo(77L);
    assertThat(saved.getJobVersion()).isEqualTo("1.2.3");
    assertThat(saved.getExecutionMetadata()).isEqualTo("{\"startedAt\":\"" + STARTED_AT + "\"}");
    assertThat(execution.getExecutionContext().get(AUDIT_ENTITY_ID_KEY)).isEqualTo(42L);
  }

  @Test
  void beforeJob_withoutDefinition_savesNothing() {
    when(jobDefinitionRepository.findByJobName(JOB_NAME)).thenReturn(Optional.empty());

    listener.beforeJob(execution);

    verify(auditRepository, never()).save(any());
    assertThat(execution.getExecutionContext().containsKey(AUDIT_ENTITY_ID_KEY)).isFalse();
  }

  @Test
  void beforeJob_clearsStaleAuditIdSoAfterJobCannotUpdateAnOldRow() {
    // A restarted execution inherits the previous execution's context, including the old row id.
    execution.getExecutionContext().put(AUDIT_ENTITY_ID_KEY, 99L);
    when(jobDefinitionRepository.findByJobName(JOB_NAME)).thenReturn(Optional.empty());

    listener.beforeJob(execution);
    listener.afterJob(execution);

    assertThat(execution.getExecutionContext().containsKey(AUDIT_ENTITY_ID_KEY)).isFalse();
    verify(auditRepository, never()).findById(any());
    verify(auditRepository, never()).save(any());
  }

  @Test
  void beforeJob_replacesStaleAuditIdWithTheNewRowId() {
    execution.getExecutionContext().put(AUDIT_ENTITY_ID_KEY, 99L);
    when(jobDefinitionRepository.findByJobName(JOB_NAME)).thenReturn(Optional.of(definition()));
    stubSaveAssigningId(43L);

    listener.beforeJob(execution);

    assertThat(execution.getExecutionContext().get(AUDIT_ENTITY_ID_KEY)).isEqualTo(43L);
  }

  @Test
  void beforeJob_repositoryFailure_doesNotFailTheJob() {
    when(jobDefinitionRepository.findByJobName(JOB_NAME)).thenReturn(Optional.of(definition()));
    when(auditRepository.save(any(JobExecutionAuditEntity.class)))
        .thenThrow(new IllegalStateException("database unavailable"));

    assertThatCode(() -> listener.beforeJob(execution)).doesNotThrowAnyException();
    assertThat(execution.getExecutionContext().containsKey(AUDIT_ENTITY_ID_KEY)).isFalse();
  }

  @Test
  void beforeJob_definitionLookupFailure_doesNotFailTheJob() {
    when(jobDefinitionRepository.findByJobName(JOB_NAME))
        .thenThrow(new IllegalStateException("database unavailable"));

    assertThatCode(() -> listener.beforeJob(execution)).doesNotThrowAnyException();
  }

  @Test
  void afterJob_repositoryFailure_doesNotFailTheJob() {
    execution.getExecutionContext().put(AUDIT_ENTITY_ID_KEY, 42L);
    execution.setEndTime(FINISHED_AT);
    execution.setStatus(BatchStatus.COMPLETED);
    when(auditRepository.findById(42L)).thenThrow(new IllegalStateException("database unavailable"));

    assertThatCode(() -> listener.afterJob(execution)).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @EnumSource(
      value = BatchStatus.class,
      names = {"COMPLETED", "FAILED", "STOPPED"})
  void afterJob_writesFinishTimeDurationAndRawBatchStatusOutcome(BatchStatus status) {
    JobExecutionAuditEntity row = new JobExecutionAuditEntity();
    row.setId(42L);
    row.setExecutionMetadata("{\"startedAt\":\"" + STARTED_AT + "\"}");
    execution.getExecutionContext().put(AUDIT_ENTITY_ID_KEY, 42L);
    execution.setEndTime(FINISHED_AT);
    execution.setStatus(status);
    when(auditRepository.findById(42L)).thenReturn(Optional.of(row));

    listener.afterJob(execution);

    verify(auditRepository).save(row);
    assertThat(row.getExecutionMetadata())
        .isEqualTo(
            "{\"startedAt\":\""
                + STARTED_AT
                + "\",\"finishedAt\":\""
                + FINISHED_AT
                + "\",\"durationMs\":3000,\"outcome\":\""
                + status.name()
                + "\"}");
  }
}
