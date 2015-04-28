/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.controllers.support;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

/**
 * The GetEnvironmentHandlerInterceptor class handles extracting Gfsh environment variables encoded in the HTTP request
 * message as request parameters.
 * <p/>
 * @author John Blum
 * @see javax.servlet.http.HttpServletRequest
 * @see javax.servlet.http.HttpServletResponse
 * @see org.springframework.web.servlet.handler.HandlerInterceptorAdapter
 * @since 8.0
 */
@SuppressWarnings("unused")
public class EnvironmentVariablesHandlerInterceptor extends HandlerInterceptorAdapter {

  private static final ThreadLocal<Map<String, String>> ENV = new ThreadLocal<Map<String, String>>() {
    @Override
    protected Map<String, String> initialValue() {
      return Collections.emptyMap();
    }
  };

  protected static final String ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX = "vf.gf.env.";

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
        requestParameterValues.put(requestParameter.substring(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX.length()),
          request.getParameter(requestParameter));
      }
    }

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
  }

  @Override
  public void afterConcurrentHandlingStarted(final HttpServletRequest request,
                                             final HttpServletResponse response,
                                             final Object handler)
    throws Exception
  {
    ENV.remove();
  }

}
