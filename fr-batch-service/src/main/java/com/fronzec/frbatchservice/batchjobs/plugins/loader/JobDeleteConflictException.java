/* 2026 */
package com.fronzec.frbatchservice.batchjobs.plugins.loader;

/**
 * Thrown when a job definition cannot be deleted because its current state forbids it, for example
 * because it has execution history in {@code job_executions_audit}. Unlike {@link
 * JobUnloadConflictException}, {@code force=true} does not bypass this conflict. Maps to HTTP 409
 * Conflict in the controller layer.
 */
public class JobDeleteConflictException extends RuntimeException {

  /**
   * Creates a delete conflict with an operator-facing message.
   *
   * @param message why the delete was rejected and what to do instead
   */
  public JobDeleteConflictException(String message) {
    super(message);
  }
}
