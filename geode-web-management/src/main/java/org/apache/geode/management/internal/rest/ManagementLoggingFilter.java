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

package org.apache.geode.management.internal.rest;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.filter.AbstractRequestLoggingFilter;

public class ManagementLoggingFilter extends AbstractRequestLoggingFilter {

  // Because someone is going to want to disable this.
  private static final Boolean ENABLE_REQUEST_LOGGING =
      Boolean.parseBoolean(System.getProperty("geode.management.request.logging", "true"));

  public ManagementLoggingFilter() {
    super.setIncludeQueryString(true);
    super.setIncludePayload(true);
    super.setMaxPayloadLength(1000);
    super.setAfterMessagePrefix("Management request: [");
  }

  @Override
  protected void beforeRequest(HttpServletRequest request, String message) {
    // No logging here - this would not display the payload
  }

  @Override
  protected void afterRequest(HttpServletRequest request, String message) {
    if (ENABLE_REQUEST_LOGGING) {
      logger.info(message);
    }
  }
}
