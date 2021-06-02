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
package org.apache.geode.management.internal.web.controllers.support;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.Logger;
import org.springframework.web.context.ServletContextAware;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.internal.HttpService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.security.ResourceConstants;

/**
 * The GetEnvironmentHandlerInterceptor class handles extracting Gfsh environment variables encoded
 * in the HTTP request message as request parameters.
 * <p/>
 *
 * @see javax.servlet.http.HttpServletRequest
 * @see javax.servlet.http.HttpServletResponse
 * @see org.springframework.web.servlet.handler.HandlerInterceptorAdapter
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class LoginHandlerInterceptor extends HandlerInterceptorAdapter
    implements ServletContextAware {

  private static final Logger logger = LogService.getLogger();

  private SecurityService securityService;

  private static final ThreadLocal<Map<String, String>> ENV =
      new ThreadLocal<Map<String, String>>() {
        @Override
        protected Map<String, String> initialValue() {
          return Collections.emptyMap();
        }
      };

  protected static final String ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX = "vf.gf.env.";

  protected static final String SECURITY_VARIABLE_REQUEST_HEADER_PREFIX =
      DistributionConfig.SECURITY_PREFIX_NAME;

  public LoginHandlerInterceptor() {}

  @VisibleForTesting
  LoginHandlerInterceptor(SecurityService securityService) {
    this.securityService = securityService;
  }

  public static Map<String, String> getEnvironment() {
    return ENV.get();
  }

  @Override
  public boolean preHandle(final HttpServletRequest request, final HttpServletResponse response,
      final Object handler) throws Exception {
    final Map<String, String> requestParameterValues = new HashMap<String, String>();

    for (Enumeration<String> requestParameters = request.getParameterNames(); requestParameters
        .hasMoreElements();) {
      final String requestParameter = requestParameters.nextElement();
      if (requestParameter.startsWith(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX)) {
        String requestValue = request.getParameter(requestParameter);
        requestParameterValues.put(
            requestParameter.substring(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX.length()),
            requestValue);
      }
    }

    ENV.set(requestParameterValues);

    String username = request.getHeader(ResourceConstants.USER_NAME);
    String password = request.getHeader(ResourceConstants.PASSWORD);
    Properties credentials = new Properties();
    if (username != null) {
      credentials.put(ResourceConstants.USER_NAME, username);
    }
    if (password != null) {
      credentials.put(ResourceConstants.PASSWORD, password);
    }
    this.securityService.login(credentials);

    return true;
  }

  @Override
  public void afterCompletion(final HttpServletRequest request, final HttpServletResponse response,
      final Object handler, final Exception ex) throws Exception {
    afterConcurrentHandlingStarted(request, response, handler);
    this.securityService.logout();
  }

  @Override
  public void afterConcurrentHandlingStarted(HttpServletRequest request,
      HttpServletResponse response, Object handler) throws Exception {
    ENV.remove();
  }

  /**
   * This is used to pass attributes into the Spring/Jetty environment from the instantiating Geode
   * environment.
   *
   */
  @Override
  public void setServletContext(ServletContext servletContext) {
    securityService = (SecurityService) servletContext
        .getAttribute(HttpService.SECURITY_SERVICE_SERVLET_CONTEXT_PARAM);
  }
}
