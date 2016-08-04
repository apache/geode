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
package com.gemstone.gemfire.management.internal.web.controllers.support;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.security.IntegratedSecurityService;
import com.gemstone.gemfire.internal.security.SecurityService;
import com.gemstone.gemfire.management.internal.cli.multistep.CLIMultiStepHelper;
import com.gemstone.gemfire.management.internal.security.ResourceConstants;
import com.gemstone.gemfire.management.internal.web.util.UriUtils;
import com.gemstone.gemfire.security.Authenticator;

import org.apache.logging.log4j.Logger;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

/**
 * The GetEnvironmentHandlerInterceptor class handles extracting Gfsh environment variables encoded in the HTTP request
 * message as request parameters.
 * <p/>
 * @see javax.servlet.http.HttpServletRequest
 * @see javax.servlet.http.HttpServletResponse
 * @see org.springframework.web.servlet.handler.HandlerInterceptorAdapter
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class LoginHandlerInterceptor extends HandlerInterceptorAdapter {

  private static final Logger logger = LogService.getLogger();

  private Cache cache;

  private Authenticator auth = null;

  private SecurityService securityService = IntegratedSecurityService.getSecurityService();

  private static final ThreadLocal<Map<String, String>> ENV = new ThreadLocal<Map<String, String>>() {
    @Override
    protected Map<String, String> initialValue() {
      return Collections.emptyMap();
    }
  };

  protected static final String ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX = "vf.gf.env.";

  protected static final String SECURITY_VARIABLE_REQUEST_HEADER_PREFIX = DistributionConfig.SECURITY_PREFIX_NAME;

  public static Map<String, String> getEnvironment() {
    return ENV.get();
  }

  @Override
  public boolean preHandle(final HttpServletRequest request, final HttpServletResponse response, final Object handler)
    throws Exception
  {
    final Map<String, String> requestParameterValues = new HashMap<String, String>();

    for (Enumeration<String> requestParameters = request.getParameterNames(); requestParameters.hasMoreElements(); ) {
      final String requestParameter = requestParameters.nextElement();
      if (requestParameter.startsWith(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX)) {
        String requestValue = request.getParameter(requestParameter);
        //GEODE-1469: since we enced stepArgs, we will need to decode it here. See #ClientHttpRequest
        if(requestParameter.contains(CLIMultiStepHelper.STEP_ARGS)){
          requestValue = UriUtils.decode(requestValue);
        }
        requestParameterValues.put(requestParameter.substring(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX.length()),
          requestValue);
      }
    }



    for (Enumeration<String> requestHeaders = request.getHeaderNames(); requestHeaders.hasMoreElements();) {

      final String requestHeader = requestHeaders.nextElement();

      if (requestHeader.startsWith(SECURITY_VARIABLE_REQUEST_HEADER_PREFIX)) {
        requestParameterValues.put(requestHeader, request.getHeader(requestHeader));
      }

    }

    String username = requestParameterValues.get(ResourceConstants.USER_NAME);
    String password = requestParameterValues.get(ResourceConstants.PASSWORD);
    this.securityService.login(username, password);

    ENV.set(requestParameterValues);

    return true;
  }


  @Override
  public void afterCompletion(final HttpServletRequest request,
                              final HttpServletResponse response,
                              final Object handler,
                              final Exception ex)
    throws Exception
  {
    afterConcurrentHandlingStarted(request, response, handler);
    this.securityService.logout();
  }

  @Override
  public void afterConcurrentHandlingStarted(
    HttpServletRequest request, HttpServletResponse response, Object handler)
    throws Exception {
    ENV.remove();
  }
}
