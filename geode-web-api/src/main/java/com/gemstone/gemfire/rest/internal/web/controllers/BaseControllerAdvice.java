/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.rest.internal.web.controllers;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.rest.internal.web.exception.DataTypeNotSupportedException;
import com.gemstone.gemfire.rest.internal.web.exception.GemfireRestException;
import com.gemstone.gemfire.rest.internal.web.exception.MalformedJsonException;
import com.gemstone.gemfire.rest.internal.web.exception.RegionNotFoundException;
import com.gemstone.gemfire.rest.internal.web.exception.ResourceNotFoundException;

/**
 * The CrudControllerAdvice class handles exception thrown while serving the REST request
 * <p/>
 * @since GemFire 8.0
 */

@ControllerAdvice
@SuppressWarnings("unused")
public class BaseControllerAdvice extends AbstractBaseController{

  private static final Logger logger = LogService.getLogger();
  
  protected static final String REST_API_VERSION = "/v1";
   
  @Override
  protected String getRestApiVersion() {
    return REST_API_VERSION;
  }
  /**
   * Handles both ResourceNotFoundExceptions and specifically, RegionNotFoundExceptions, occurring when a resource
   * or a Region (a.k.a. resource) does not exist in GemFire.
   * <p/>
   * @param e the RuntimeException thrown when the accessed/requested resource does not exist in GemFire.
   * @return the String message from the RuntimeException.
   */
  @ExceptionHandler({ RegionNotFoundException.class, ResourceNotFoundException.class })
  @ResponseBody
  @ResponseStatus(HttpStatus.NOT_FOUND)
  public String handle(final RuntimeException e) {
    return convertErrorAsJson(e.getMessage());
  }

  /**
   * Handles MalformedJsonFoundException, occurring when REST service encounters incorrect or malformed JSON document
   * <p/>
   * @param e the RuntimeException thrown when malformed JSON is encounterd.
   * @return the String message from the RuntimeException.
   */
  @ExceptionHandler({ MalformedJsonException.class })
  @ResponseBody
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  public String handleException(final RuntimeException e) {
    return convertErrorAsJson(e.getMessage());
  }
  
  /**
   * Handles any GemfireRestException thrown by a REST API web service endpoint, HTTP request handler method.
   * <p/>
   * @param ge the GemfireRestException thrown when it found problem processing REST request.
   * @return the String message from the RuntimeException.
   */
  @ExceptionHandler(GemfireRestException.class)
  @ResponseBody
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  public String handleException(final GemfireRestException ge) {
    return convertErrorAsJson(ge);
  }
  
  /**
   * Handles any DataTypeNotSupportedException thrown by a REST API web service endpoint, HTTP request handler method.
   * <p/>
   * @param tns the DataTypeNotSupportedException thrown if problem occurs in cache values to JSON conversion.
   * @return the String message from the RuntimeException.
   */
  @ExceptionHandler(DataTypeNotSupportedException.class)
  @ResponseBody
  @ResponseStatus(HttpStatus.NOT_ACCEPTABLE)
  public String handleException(final DataTypeNotSupportedException tns) {
    return convertErrorAsJson(tns.getMessage());
  }
  
  /**
   * Handles HttpRequestMethodNotSupportedException thrown by a REST API web service when request is 
   * received with unsupported HTTP method.
   * <p/>
   * @param e the HttpRequestMethodNotSupportedException thrown when REST request is received with NOT support methods.
   * @return the String message from the RuntimeException.
   */
  @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
  @ResponseBody
  @ResponseStatus(HttpStatus.METHOD_NOT_ALLOWED)
  public String handleException(final HttpRequestMethodNotSupportedException e) {
    return convertErrorAsJson(e.getMessage());
  }
 
  /**
   * Handles any Exception thrown by a REST API web service endpoint, HTTP request handler method.
   * <p/>
   * @param cause the Exception causing the error.
   * @return a ResponseEntity with an appropriate HTTP status code (500 - Internal Server Error) and HTTP response body
   * containing the stack trace of the Exception.
   */
  @ExceptionHandler(Throwable.class)
  @ResponseBody
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  public String handleException(final Throwable cause) {
    final StringWriter stackTraceWriter = new StringWriter();
    cause.printStackTrace(new PrintWriter(stackTraceWriter));
    final String stackTrace = stackTraceWriter.toString();
    
    if(logger.isDebugEnabled()){
      logger.debug(stackTrace);  
    }
    
    return convertErrorAsJson(cause.getMessage());
  }
  
}

