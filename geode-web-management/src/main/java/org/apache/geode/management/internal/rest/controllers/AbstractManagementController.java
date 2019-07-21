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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.ServletContext;

import org.apache.shiro.subject.Subject;
import org.springframework.beans.propertyeditors.StringArrayPropertyEditor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.context.ServletContextAware;

import org.apache.geode.internal.cache.HttpService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.internal.api.LocatorClusterManagementService;

public class AbstractManagementController implements ServletContextAware {

  protected static final String MANAGEMENT_API_VERSION = "/v2";
  protected SecurityService securityService;
  protected LocatorClusterManagementService clusterManagementService;

  @Override
  public void setServletContext(ServletContext servletContext) {
    securityService = (SecurityService) servletContext
        .getAttribute(HttpService.SECURITY_SERVICE_SERVLET_CONTEXT_PARAM);
    clusterManagementService = (LocatorClusterManagementService) servletContext
        .getAttribute(HttpService.CLUSTER_MANAGEMENT_SERVICE_CONTEXT_PARAM);
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

  /** adds the informational "operator" and "authorization" properties by reflection */
  protected void decorate(ClusterManagementResult result) {
    if (result == null) {
      return;
    }

    if (securityService != null) {
      Subject subject = securityService.getSubject();
      if (subject != null) {
        result.setOperator(subject.toString());
      }
    }

    String caller = new Exception().getStackTrace()[1].getMethodName();
    Set<String> allPreAuths =
        Arrays.stream(getClass().getMethods()).filter(m -> m.getName().equals(caller))
            .flatMap(method -> Arrays.stream(method.getAnnotations()))
            .filter(annotation -> annotation instanceof PreAuthorize)
            .map(annotation -> ((PreAuthorize) annotation).value())
            .map(AbstractManagementController::parseAuth)
            .collect(Collectors.toSet());
    if (allPreAuths.size() > 1)
      throw new IllegalStateException(
          "multiple @PreAuthorize annotations found for " + getClass() + "::" + caller);
    if (allPreAuths.size() < 1)
      throw new IllegalStateException(
          "no @PreAuthorize annotation found for " + getClass() + "::" + caller);
    allPreAuths.forEach(result::setAuthorization);
  }

  /**
   * @param auth example: "@securityService.authorize('X', 'Y', 'Z')"
   * @return example: "X:Y:Z"
   */
  private static String parseAuth(String auth) {
    return auth.replace("@securityService.authorize(", "").replace(")", "").replace("'", "")
        .replace(" ", "").replace(',', ':');
  }
}
