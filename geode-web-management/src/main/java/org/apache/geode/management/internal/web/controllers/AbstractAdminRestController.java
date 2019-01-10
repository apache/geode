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
package org.apache.geode.management.internal.web.controllers;

import java.io.PrintWriter;
import java.io.StringWriter;

import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;

import org.apache.logging.log4j.Logger;
import org.springframework.beans.propertyeditors.StringArrayPropertyEditor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.InitBinder;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.NotAuthorizedException;

/**
 * The AbstractCommandsController class is the abstract base class encapsulating common
 * functionality across all Management Controller classes that expose REST API web service endpoints
 * (URLs/URIs) for GemFire shell (Gfsh) commands.
 *
 * @see MemberMXBean
 * @see Gfsh
 * @see ResponseEntity
 * @see org.springframework.stereotype.Controller
 * @see ExceptionHandler
 * @see InitBinder
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public abstract class AbstractAdminRestController {
  protected static final String REST_API_VERSION = "/v1";
  private static final Logger logger = LogService.getLogger();

  @ExceptionHandler(Exception.class)
  public ResponseEntity<String> internalError(final Exception e) {
    final String stackTrace = getPrintableStackTrace(e);
    logger.fatal(stackTrace);
    return new ResponseEntity<>(stackTrace, HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @ExceptionHandler(AuthenticationFailedException.class)
  public ResponseEntity<String> unauthorized(AuthenticationFailedException e) {
    return new ResponseEntity<>(e.getMessage(), HttpStatus.UNAUTHORIZED);
  }

  @ExceptionHandler({NotAuthorizedException.class, SecurityException.class})
  public ResponseEntity<String> forbidden(Exception e) {
    return new ResponseEntity<>(e.getMessage(), HttpStatus.FORBIDDEN);
  }

  @ExceptionHandler(MalformedObjectNameException.class)
  public ResponseEntity<String> badRequest(final MalformedObjectNameException e) {
    logger.info(e);
    return new ResponseEntity<>(getPrintableStackTrace(e), HttpStatus.BAD_REQUEST);
  }

  @ExceptionHandler(InstanceNotFoundException.class)
  public ResponseEntity<String> notFound(final InstanceNotFoundException e) {
    logger.info(e);
    return new ResponseEntity<>(getPrintableStackTrace(e), HttpStatus.NOT_FOUND);
  }

  /**
   * Writes the stack trace of the Throwable to a String.
   *
   * @param t a Throwable object who's stack trace will be written to a String.
   * @return a String containing the stack trace of the Throwable.
   * @see StringWriter
   * @see Throwable#printStackTrace(PrintWriter)
   */
  private static String getPrintableStackTrace(final Throwable t) {
    final StringWriter stackTraceWriter = new StringWriter();
    t.printStackTrace(new PrintWriter(stackTraceWriter));
    return stackTraceWriter.toString();
  }

  /**
   * Initializes data bindings for various HTTP request handler method parameter Java class types.
   *
   * @param dataBinder the DataBinder implementation used for Web transactions.
   * @see WebDataBinder
   * @see InitBinder
   */
  @InitBinder
  public void initBinder(final WebDataBinder dataBinder) {
    dataBinder.registerCustomEditor(String[].class,
        new StringArrayPropertyEditor(StringArrayPropertyEditor.DEFAULT_SEPARATOR, false));
  }
}
