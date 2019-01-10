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

import javax.servlet.ServletContext;

import org.springframework.web.context.ServletContextAware;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.JettyHelper;

public class AbstractManagementController extends AbstractAdminRestController implements
    ServletContextAware {

  protected static final String MANAGEMENT_API_VERSION = "/v2";
  protected SecurityService securityService;

  @Override
  public void setServletContext(ServletContext servletContext) {
    securityService = (SecurityService) servletContext
        .getAttribute(JettyHelper.SECURITY_SERVICE_SERVLET_CONTEXT_PARAM);
  }
}
