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
package org.apache.geode.management.internal.rest.security;

import javax.servlet.ServletContext;

import org.springframework.stereotype.Component;
import org.springframework.web.context.ServletContextAware;

import org.apache.geode.cache.internal.HttpService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

@Component("securityService")
public class RestSecurityService implements ServletContextAware {

  private SecurityService securityService;

  public boolean authorize(String resource, String operation) {
    return authorize(resource, operation, null, null);
  }

  public boolean authorize(String resource, String operation, String region) {
    return authorize(resource, operation, region, null);
  }

  /**
   * this does not need to return a boolean since it's not used in the @PreAuthorize tag
   */
  public void authorize(ResourcePermission permission) {
    securityService.authorize(permission);
  }


  /**
   * calls used in @PreAuthorize tag needs to return a boolean
   */
  public boolean authorize(String resource, String operation, String region, String key) {
    securityService.authorize(Resource.valueOf(resource), Operation.valueOf(operation), region,
        key);
    return true;
  }

  public boolean authorize(String operation, String region, String[] keys) {
    boolean authorized = false;
    for (String key : keys) {
      authorized = authorize("DATA", operation, region, key);
      if (!authorized) {
        return false;
      }
    }
    return true;
  }

  public Object postProcess(String regionPath, Object key, Object value,
      boolean valueIsSerialized) {
    return securityService.postProcess(regionPath, key, value, valueIsSerialized);
  }

  @Override
  public void setServletContext(ServletContext servletContext) {
    securityService = (SecurityService) servletContext
        .getAttribute(HttpService.SECURITY_SERVICE_SERVLET_CONTEXT_PARAM);
  }
}
