/* 2024-2025 */
package com.fronzec.frbatchservice.web;

import com.fronzec.frbatchservice.batchjobs.plugins.loader.JobDeleteConflictException;
import com.fronzec.frbatchservice.batchjobs.plugins.loader.JobLoadException;
import com.fronzec.frbatchservice.batchjobs.plugins.loader.JobUnloadConflictException;
import com.fronzec.frbatchservice.batchjobs.plugins.service.DuplicateJobDefinitionException;
import com.fronzec.frbatchservice.batchjobs.plugins.service.InvalidJarException;
import com.fronzec.frbatchservice.web.dto.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

/**
 * Global exception handler that maps domain and framework exceptions to structured
 * {@link ErrorResponse} JSON bodies.
 *
 * <p>Applied to all {@code @Controller} beans (not only {@code @RestController}).
 * Each handler logs at the appropriate level and returns a consistent error envelope
 * with {@code status}, {@code error}, {@code message}, {@code timestamp}, and
 * {@code path} fields.
 */
@ControllerAdvice
public class GlobalExceptionHandler {

  private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

  @ExceptionHandler(InvalidJarException.class)
  public ResponseEntity<ErrorResponse> handleInvalidJar(
      InvalidJarException ex, HttpServletRequest request) {
    log.warn("JAR upload validation failed: {}", ex.getMessage());
    return ResponseEntity.badRequest()
        .body(buildError(400, "Bad Request", ex.getMessage(), request));
  }

  @ExceptionHandler(DuplicateJobDefinitionException.class)
  public ResponseEntity<ErrorResponse> handleDuplicate(
      DuplicateJobDefinitionException ex, HttpServletRequest request) {
    log.warn("Duplicate job definition: {}", ex.getMessage());
    return ResponseEntity.status(HttpStatus.CONFLICT)
        .body(buildError(409, "Conflict", ex.getMessage(), request));
  }

  /**
   * Covers {@code findById(id).orElseThrow()} patterns that throw
   * {@link NoSuchElementException} when a resource is missing.
   */
  @ExceptionHandler(NoSuchElementException.class)
  public ResponseEntity<ErrorResponse> handleNotFound(
      NoSuchElementException ex, HttpServletRequest request) {
    log.warn("Resource not found: {}", ex.getMessage());
    return ResponseEntity.status(HttpStatus.NOT_FOUND)
        .body(buildError(404, "Not Found", ex.getMessage(), request));
  }

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<ErrorResponse> handleValidation(
      MethodArgumentNotValidException ex, HttpServletRequest request) {
    String detail =
        ex.getBindingResult().getFieldErrors().stream()
            .map(e -> e.getField() + ": " + e.getDefaultMessage())
            .collect(Collectors.joining("; "));
    log.warn("Validation error: {}", detail);
    return ResponseEntity.badRequest()
        .body(buildError(400, "Bad Request", detail, request));
  }

  @ExceptionHandler(MaxUploadSizeExceededException.class)
  public ResponseEntity<ErrorResponse> handleMaxUploadSize(
      MaxUploadSizeExceededException ex, HttpServletRequest request) {
    log.warn("Upload size exceeded: {}", ex.getMessage());
    return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE)
        .body(
            buildError(
                413,
                "Payload Too Large",
                "File size exceeds the maximum allowed upload size",
                request));
  }

  /**
   * Maps {@link JobLoadException} — a missing or invalid JAR, a class that does not
   * implement the plugin contract, or a configuration failure — to {@code 400 Bad
   * Request}.
   */
  @ExceptionHandler(JobLoadException.class)
  public ResponseEntity<ErrorResponse> handleJobLoad(
      JobLoadException ex, HttpServletRequest request) {
    log.error("Job load failed: {}", ex.getMessage());
    return ResponseEntity.badRequest()
        .body(buildError(400, "Bad Request", ex.getMessage(), request));
  }

  /**
   * Maps {@link JobUnloadConflictException} — a job with running executions where
   * {@code force} was not requested — to {@code 409 Conflict}.
   */
  @ExceptionHandler(JobUnloadConflictException.class)
  public ResponseEntity<ErrorResponse> handleUnloadConflict(
      JobUnloadConflictException ex, HttpServletRequest request) {
    log.warn("Job unload conflict: {}", ex.getMessage());
    return ResponseEntity.status(HttpStatus.CONFLICT)
        .body(buildError(409, "Conflict", ex.getMessage(), request));
  }

  /**
   * Maps {@link JobDeleteConflictException} — a definition that cannot be deleted, such as one
   * with execution history; {@code force} does not bypass it — to {@code 409 Conflict}.
   */
  @ExceptionHandler(JobDeleteConflictException.class)
  public ResponseEntity<ErrorResponse> handleDeleteConflict(
      JobDeleteConflictException ex, HttpServletRequest request) {
    log.warn("Job delete conflict: {}", ex.getMessage());
    return ResponseEntity.status(HttpStatus.CONFLICT)
        .body(buildError(409, "Conflict", ex.getMessage(), request));
  }

  /**
   * Maps {@link IllegalStateException} — e.g. a definition that is disabled or
   * already loaded — to {@code 409 Conflict}.
   */
  @ExceptionHandler(IllegalStateException.class)
  public ResponseEntity<ErrorResponse> handleIllegalState(
      IllegalStateException ex, HttpServletRequest request) {
    log.warn("Illegal state: {}", ex.getMessage());
    return ResponseEntity.status(HttpStatus.CONFLICT)
        .body(buildError(409, "Conflict", ex.getMessage(), request));
  }

  /**
   * Maps Spring Security {@link AccessDeniedException} — the caller is
   * authenticated but lacks the required role — to {@code 403 Forbidden}.
   */
  @ExceptionHandler(AccessDeniedException.class)
  public ResponseEntity<ErrorResponse> handleAccessDenied(
      AccessDeniedException ex, HttpServletRequest request) {
    log.warn("Access denied on {}: {}", request.getRequestURI(), ex.getMessage());
    return ResponseEntity.status(HttpStatus.FORBIDDEN)
        .body(buildError(403, "Forbidden", "Access denied", request));
  }

  /**
   * Maps Spring Security {@link AuthenticationException} — no or invalid
   * credentials — to {@code 401 Unauthorized}.
   */
  @ExceptionHandler(AuthenticationException.class)
  public ResponseEntity<ErrorResponse> handleAuthentication(
      AuthenticationException ex, HttpServletRequest request) {
    log.warn("Authentication failed on {}: {}", request.getRequestURI(), ex.getMessage());
    return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
        .body(buildError(401, "Unauthorized", "Authentication required", request));
  }

  /**
   * Catch-all for any exception not explicitly handled above.
   *
   * <p>Logs the full stack-trace at {@code ERROR} level to aid diagnostics, but
   * returns a sanitised message to the client.
   */
  @ExceptionHandler(Exception.class)
  public ResponseEntity<ErrorResponse> handleGeneral(Exception ex, HttpServletRequest request) {
    log.error("Unhandled exception on {}: {}", request.getRequestURI(), ex.getMessage(), ex);
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
        .body(buildError(500, "Internal Server Error", "An unexpected error occurred", request));
  }

  private static ErrorResponse buildError(
      int status, String error, String message, HttpServletRequest request) {
    return new ErrorResponse(status, error, message, LocalDateTime.now(), request.getRequestURI());
  }
}
