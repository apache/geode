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
package org.apache.geode.management.internal.web.shell;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;

import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.NotAuthorizedException;

public class RestOperationErrorHandler implements ResponseErrorHandler {
  private final Gfsh gfsh;

  RestOperationErrorHandler(Gfsh gfsh) {
    this.gfsh = gfsh;
  }

  public RestOperationErrorHandler() {
    this(null);
  }

  @Override
  public boolean hasError(final ClientHttpResponse response) throws IOException {
    final HttpStatus status = response.getStatusCode();

    switch (status) {
      case BAD_REQUEST: // 400
      case UNAUTHORIZED: // 401
      case FORBIDDEN: // 403
      case NOT_FOUND: // 404
      case METHOD_NOT_ALLOWED: // 405
      case NOT_ACCEPTABLE: // 406
      case REQUEST_TIMEOUT: // 408
      case CONFLICT: // 409
      case REQUEST_ENTITY_TOO_LARGE: // Old 413
      case PAYLOAD_TOO_LARGE: // 413
      case REQUEST_URI_TOO_LONG: // Old 414
      case URI_TOO_LONG: // 414
      case UNSUPPORTED_MEDIA_TYPE: // 415
      case TOO_MANY_REQUESTS: // 429
      case INTERNAL_SERVER_ERROR: // 500
      case NOT_IMPLEMENTED: // 501
      case BAD_GATEWAY: // 502
      case SERVICE_UNAVAILABLE: // 503
        return true;
      default:
        return false;
    }
  }

  @Override
  public void handleError(final ClientHttpResponse response) throws IOException {
    String body = IOUtils.toString(response.getBody(), StandardCharsets.UTF_8);
    final String message = String.format("The HTTP request failed with: %1$d - %2$s",
        response.getRawStatusCode(), body);

    if (gfsh != null && gfsh.getDebug()) {
      gfsh.logSevere(body, null);
    }

    if (response.getRawStatusCode() == 401) {
      throw new AuthenticationFailedException(message);
    } else if (response.getRawStatusCode() == 403) {
      throw new NotAuthorizedException(message);
    } else {
      throw new RuntimeException(message);
    }

  }
}
