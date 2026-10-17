/* 2024 */
package com.fronzec.frbatchservice.batchjobs.plugins.repository;

import com.fronzec.frbatchservice.batchjobs.plugins.entity.JobExecutionAuditEntity;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JobExecutionAuditRepository
        extends JpaRepository<JobExecutionAuditEntity, Long> {

    List<JobExecutionAuditEntity> findByJobDefinitionId(long jobDefinitionId);

    /**
     * Reports whether any execution history references the given job definition.
     *
     * @param jobDefinitionId the job definition ID
     * @return {@code true} if at least one {@code job_executions_audit} row references it
     */
    boolean existsByJobDefinitionId(long jobDefinitionId);
}
