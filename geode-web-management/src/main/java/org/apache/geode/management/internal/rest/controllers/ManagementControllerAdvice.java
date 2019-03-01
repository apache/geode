/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.rest.controllers;


import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.internal.exceptions.EntityExistsException;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.NotAuthorizedException;

@ControllerAdvice
public class ManagementControllerAdvice {
  private static final Logger logger = LogService.getLogger();

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ClusterManagementResult> internalError(final Exception e) {
    logger.error(e.getMessage(), e);
    return new ResponseEntity<>(
        new ClusterManagementResult(ClusterManagementResult.StatusCode.ERROR,
            e.getMessage()),
        HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @ExceptionHandler(EntityExistsException.class)
  public ResponseEntity<ClusterManagementResult> entityExists(final Exception e) {
    return new ResponseEntity<>(
        new ClusterManagementResult(ClusterManagementResult.StatusCode.ENTITY_EXISTS,
            e.getMessage()),
        HttpStatus.CONFLICT);
  }

  @ExceptionHandler({AuthenticationFailedException.class, AuthenticationException.class})
  public ResponseEntity<ClusterManagementResult> unauthorized(Exception e) {
    return new ResponseEntity<>(
        new ClusterManagementResult(ClusterManagementResult.StatusCode.UNAUTHENTICATED,
            e.getMessage()),
        HttpStatus.UNAUTHORIZED);
  }

  @ExceptionHandler({NotAuthorizedException.class, SecurityException.class})
  public ResponseEntity<ClusterManagementResult> forbidden(Exception e) {
    return new ResponseEntity<>(new ClusterManagementResult(
        ClusterManagementResult.StatusCode.UNAUTHORIZED, e.getMessage()),
        HttpStatus.FORBIDDEN);
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<ClusterManagementResult> badRequest(final IllegalArgumentException e) {
    return new ResponseEntity<>(
        new ClusterManagementResult(ClusterManagementResult.StatusCode.ILLEGAL_ARGUMENT,
            e.getMessage()),
        HttpStatus.BAD_REQUEST);
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
    return new ResponseEntity<>(
        new ClusterManagementResult(ClusterManagementResult.StatusCode.UNAUTHORIZED,
            cause.getMessage()),
        HttpStatus.FORBIDDEN);
  }

}
