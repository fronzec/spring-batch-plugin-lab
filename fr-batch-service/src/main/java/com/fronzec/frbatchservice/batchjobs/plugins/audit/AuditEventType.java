/* 2024-2026 */
package com.fronzec.frbatchservice.batchjobs.plugins.audit;

/**
 * Lifecycle event types tracked by the audit trail.
 *
 * <p>Every event maps to a specific operation in the plugin lifecycle:
 * upload → approve/reject → load → (run) → unload → replace JAR → delete.
 */
public enum AuditEventType {
  JAR_UPLOADED,
  JAR_REPLACED,
  JOB_LOADED,
  JOB_UNLOADED,
  JOB_DELETED,
  JOB_ENABLED,
  JOB_DISABLED,
  JOB_APPROVED,
  JOB_REJECTED
}
