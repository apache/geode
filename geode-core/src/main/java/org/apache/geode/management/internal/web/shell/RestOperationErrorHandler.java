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

import org.apache.commons.io.IOUtils;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.DefaultResponseErrorHandler;

import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.NotAuthorizedException;

public class RestOperationErrorHandler extends DefaultResponseErrorHandler {
  private final Gfsh gfsh;

  RestOperationErrorHandler(Gfsh gfsh) {
    this.gfsh = gfsh;
  }

  public RestOperationErrorHandler() {
    this(null);
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
