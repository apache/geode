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

import static org.apache.geode.management.rest.internal.Constants.INCLUDE_CLASS_HEADER;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.json.AbstractJackson2HttpMessageConverter;
import org.springframework.http.converter.json.Jackson2ObjectMapperFactoryBean;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.api.ClusterManagementException;
import org.apache.geode.management.api.ClusterManagementRealizationException;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementResult.StatusCode;
import org.apache.geode.management.configuration.Links;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.NotAuthorizedException;

@ControllerAdvice
public class ManagementControllerAdvice implements ResponseBodyAdvice<Object> {
  private static final Logger logger = LogService.getLogger();
  private String requestContext;

  @Autowired
  private Jackson2ObjectMapperFactoryBean objectMapperFactory;

  @Override
  public boolean supports(MethodParameter returnType, Class converterType) {
    // only invoke our beforeBodyWrite for conversions to JSON (not String, etc)
    return AbstractJackson2HttpMessageConverter.class.isAssignableFrom(converterType);
  }

  @Override
  public Object beforeBodyWrite(Object body, MethodParameter returnType,
      MediaType selectedContentType, Class selectedConverterType,
      ServerHttpRequest request, ServerHttpResponse response) {

    List<String> values = request.getHeaders().get(INCLUDE_CLASS_HEADER);
    boolean includeClass = values != null && values.contains("true");

    if (requestContext == null) {
      requestContext = ServletUriComponentsBuilder.fromCurrentContextPath().build().toString();
    }

    try {
      ObjectMapper objectMapper = objectMapperFactory.getObject();
      String json = objectMapper.writeValueAsString(body);
      if (!includeClass) {
        json = removeClassFromJsonText(json);
      }
      json = qualifyHrefsInJsonText(json, requestContext);
      response.getHeaders().add(HttpHeaders.CONTENT_TYPE,
          "application/json;charset=UTF-8");
      response.getBody().write(json.getBytes());
      response.close();
      return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String removeClassFromJsonText(String json) {
    // remove entire key and object if class was the only attribute present
    // otherwise remove just the class attribute
    return json
        .replaceAll("\"[^\"]*\":\\{\"class\":\"[^\"]*\"},?", "")
        .replaceAll("\"class\":\"[^\"]*\",", "");
  }

  static String qualifyHrefsInJsonText(String json, String requestContext) {
    if (requestContext == null) {
      return json;
    }
    return json.replace(Links.HREF_PREFIX + Links.URI_CONTEXT, requestContext);
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ClusterManagementResult> internalError(final Exception e) {
    logger.error(e.getMessage(), e);
    return new ResponseEntity<>(
        new ClusterManagementResult(StatusCode.ERROR,
            cleanup(e.getMessage())),
        HttpStatus.INTERNAL_SERVER_ERROR);
  }

  private String cleanup(String message) {
    if (message == null) {
      return "";
    } else {
      return message.replace("java.lang.Exception: ", "")
          .replace("java.lang.RuntimeException: ", "")
          .replace("java.util.concurrent.ExecutionException: ", "");
    }
  }

  @ExceptionHandler(ClusterManagementException.class)
  public ResponseEntity<ClusterManagementResult> clusterManagementException(final Exception e) {
    ClusterManagementResult result = ((ClusterManagementException) e).getResult();
    return new ResponseEntity<>(result, mapToHttpStatus(result.getStatusCode()));
  }

  @ExceptionHandler(ClusterManagementRealizationException.class)
  public ResponseEntity<ClusterManagementRealizationResult> clusterManagementRealizationException(
      final Exception e) {
    ClusterManagementRealizationResult result =
        (ClusterManagementRealizationResult) ((ClusterManagementException) e).getResult();
    return new ResponseEntity<>(result, mapToHttpStatus(result.getStatusCode()));
  }

  private HttpStatus mapToHttpStatus(StatusCode statusCode) {
    switch (statusCode) {
      case ENTITY_EXISTS:
        return HttpStatus.CONFLICT;
      case ENTITY_NOT_FOUND:
        return HttpStatus.NOT_FOUND;
      case ILLEGAL_ARGUMENT:
        return HttpStatus.BAD_REQUEST;
      default:
        return HttpStatus.INTERNAL_SERVER_ERROR;
    }
  }

  @ExceptionHandler({AuthenticationFailedException.class, AuthenticationException.class})
  public ResponseEntity<ClusterManagementResult> unauthorized(Exception e) {
    return new ResponseEntity<>(
        new ClusterManagementResult(StatusCode.UNAUTHENTICATED, e.getMessage()),
        HttpStatus.UNAUTHORIZED);
  }

  @ExceptionHandler({NotAuthorizedException.class, SecurityException.class})
  public ResponseEntity<ClusterManagementResult> forbidden(Exception e) {
    return new ResponseEntity<>(
        new ClusterManagementResult(StatusCode.UNAUTHORIZED, e.getMessage()), HttpStatus.FORBIDDEN);
  }

  @ExceptionHandler({IllegalArgumentException.class, HttpMessageNotReadableException.class})
  public ResponseEntity<ClusterManagementResult> badRequest(final Exception e) {
    return new ResponseEntity<>(
        new ClusterManagementResult(StatusCode.ILLEGAL_ARGUMENT, e.getMessage()),
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
        new ClusterManagementResult(StatusCode.UNAUTHORIZED,
            cause.getMessage()),
        HttpStatus.FORBIDDEN);
  }
}
