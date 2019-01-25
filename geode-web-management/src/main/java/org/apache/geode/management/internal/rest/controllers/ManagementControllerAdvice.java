package org.apache.geode.management.internal.rest.controllers;

import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;

import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.api.ClusterManagementResult;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.NotAuthorizedException;

@ControllerAdvice
public class ManagementControllerAdvice {
  private static final Logger logger = LogService.getLogger();

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ClusterManagementResult> internalError(final Exception e) {
    logger.error(e.getMessage(), e);
    return new ResponseEntity<>(new ClusterManagementResult(false, e.getMessage()),
        HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @ExceptionHandler({AuthenticationFailedException.class, AuthenticationException.class})
  public ResponseEntity<ClusterManagementResult> unauthorized(AuthenticationFailedException e) {
    return new ResponseEntity<>(new ClusterManagementResult(false, e.getMessage()),
        HttpStatus.UNAUTHORIZED);
  }

  @ExceptionHandler({NotAuthorizedException.class, SecurityException.class})
  public ResponseEntity<ClusterManagementResult> forbidden(Exception e) {
    logger.info(e.getMessage());
    return new ResponseEntity<>(new ClusterManagementResult(false, e.getMessage()),
        HttpStatus.FORBIDDEN);
  }

  @ExceptionHandler(MalformedObjectNameException.class)
  public ResponseEntity<ClusterManagementResult> badRequest(final MalformedObjectNameException e) {
    logger.info(e.getMessage(), e);
    return new ResponseEntity<>(new ClusterManagementResult(false, e.getMessage()),
        HttpStatus.BAD_REQUEST);
  }

  @ExceptionHandler(InstanceNotFoundException.class)
  public ResponseEntity<ClusterManagementResult> notFound(final InstanceNotFoundException e) {
    logger.info(e.getMessage(), e);
    return new ResponseEntity<>(new ClusterManagementResult(false, e.getMessage()),
        HttpStatus.NOT_FOUND);
  }

  /**
   * Handles an AccessDenied Exception thrown by a REST API web service endpoint, HTTP request
   * handler method.
   * <p/>
   *
   * @param cause the Exception causing the error.
   * @return a ResponseEntity with an appropriate HTTP status code (403 - Forbidden)
   */
  @ExceptionHandler(AccessDeniedException.class)
  public ResponseEntity<ClusterManagementResult> handleException(
      final AccessDeniedException cause) {
    logger.info(cause.getMessage(), cause);
    return new ResponseEntity<>(new ClusterManagementResult(false, cause.getMessage()),
        HttpStatus.FORBIDDEN);
  }

}
