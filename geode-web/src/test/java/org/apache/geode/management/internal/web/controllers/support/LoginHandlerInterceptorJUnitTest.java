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

import static java.lang.Thread.currentThread;
import static java.util.Collections.enumeration;
import static org.apache.geode.management.internal.security.ResourceConstants.PASSWORD;
import static org.apache.geode.management.internal.security.ResourceConstants.USER_NAME;
import static org.apache.geode.management.internal.web.controllers.support.LoginHandlerInterceptor.ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mock;
import org.springframework.web.servlet.HandlerInterceptor;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.test.junit.rules.ConcurrencyRule;

public class LoginHandlerInterceptorJUnitTest {
  private static Logger log = LogService.getLogger();
  @Mock
  private SecurityService securityService;
  private HandlerInterceptor interceptor;

  @Rule
  public TestName name = new TestName();

  @Rule
  public ConcurrencyRule runConcurrently = new ConcurrencyRule();

  @Before
  public void setUp() {
    LoginHandlerInterceptor.getEnvironment().clear();
    initMocks(this);
    interceptor = new LoginHandlerInterceptor(securityService);
  }

  @After
  public void tearDown() {
    LoginHandlerInterceptor.getEnvironment().clear();
  }

  @Test
  public void preHandleSetsEnvironmentVariablesFromPrefixedRequestParameters()
      throws Exception {
    final Map<String, String> requestParameters = new HashMap<>(2);
    requestParameters.put("parameter", "one");
    requestParameters.put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "variable", "two");
    final HttpServletRequest mockHttpRequest = mock(HttpServletRequest.class, name.getMethodName());
    when(mockHttpRequest.getParameterNames()).thenReturn(enumeration(requestParameters.keySet()));
    when(mockHttpRequest.getHeader(USER_NAME)).thenReturn("admin");
    when(mockHttpRequest.getHeader(PASSWORD)).thenReturn("password");
    when(mockHttpRequest.getParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "variable"))
        .thenReturn("two");

    Map<String, String> environmentBeforePreHandle = LoginHandlerInterceptor.getEnvironment();
    assertThat(environmentBeforePreHandle)
        .describedAs("environment before preHandle()")
        .isEmpty();

    assertThat(interceptor.preHandle(mockHttpRequest, null, null))
        .describedAs("preHandle() result")
        .isTrue();
    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment after preHandle()")
        .isNotSameAs(environmentBeforePreHandle)
        .hasSize(1)
        .containsEntry("variable", "two");

    Properties expectedLoginProperties = new Properties();
    expectedLoginProperties.put(USER_NAME, "admin");
    expectedLoginProperties.put(PASSWORD, "password");
    verify(securityService, times(1)).login(expectedLoginProperties);
  }

  @Test
  public void afterCompletionCleansTheEnvironment() throws Exception {
    Map<String, String> requestParameters = new HashMap<>(2);
    requestParameters.put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "variable", "two");
    HttpServletRequest mockHttpRequest = mock(HttpServletRequest.class, name.getMethodName());
    when(mockHttpRequest.getParameterNames()).thenReturn(enumeration(requestParameters.keySet()));
    when(mockHttpRequest.getHeader(USER_NAME)).thenReturn("admin");
    when(mockHttpRequest.getHeader(PASSWORD)).thenReturn("password");
    when(mockHttpRequest.getParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "variable"))
        .thenReturn("two");

    assertThat(interceptor.preHandle(mockHttpRequest, null, null))
        .describedAs("preHandle() result")
        .isTrue();

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment after preHandle()")
        .hasSize(1)
        .containsEntry("variable", "two");

    Properties expectedLoginProperties = new Properties();
    expectedLoginProperties.put(USER_NAME, "admin");
    expectedLoginProperties.put(PASSWORD, "password");
    verify(securityService, times(1)).login(expectedLoginProperties);

    interceptor.afterCompletion(mockHttpRequest, null, null, null);

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment after afterCompletion()")
        .isEmpty();
    verify(securityService, times(1)).logout();
  }

  @Test
  public void eachRequestThreadsEnvironmentIsConfinedToItsThread() {
    Semaphore thread1Permit = new Semaphore(0);
    Semaphore thread2Permit = new Semaphore(0);

    Callable<Void> request1Task = () -> processRequest("thread 1", thread1Permit, thread2Permit);
    Callable<Void> request2Task = () -> processRequest("thread 2", thread2Permit, thread1Permit);

    runConcurrently.setTimeout(Duration.ofSeconds(1));
    runConcurrently.add(request1Task);
    runConcurrently.add(request2Task);
    thread1Permit.release();
    runConcurrently.executeInParallel();
  }

  private Void processRequest(String taskName, Semaphore thisTaskPermit,
      Semaphore otherTaskPermit) throws Exception {
    currentThread().setName(taskName);
    log.info(taskName + " started");

    // sync up threads to allow both to start before proceeding
    thisTaskPermit.acquire();

    log.info(taskName + " handing off");
    otherTaskPermit.release();
    thisTaskPermit.acquire();
    log.info(taskName + " running preHandle()");

    Map<String, String> requestParameters = new HashMap<>();
    requestParameters.put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "COMMON",
        "COMMON value for " + taskName);
    requestParameters.put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX
        + "REQUEST-SPECIFIC " + taskName,
        "REQUEST-SPECIFIC value for " + taskName);

    HttpServletRequest request = request(taskName, requestParameters);

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment before preHandle() in " + taskName)
        .isEmpty();

    interceptor.preHandle(request, null, null);

    Map<String, String> requestEnvironment = LoginHandlerInterceptor.getEnvironment();

    log.info(taskName + " handing off");
    otherTaskPermit.release();
    thisTaskPermit.acquire();
    log.info(taskName + " checking for pollution of request environment");

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment remains unchanged in " + taskName)
        .containsAllEntriesOf(requestEnvironment)
        .hasSameSizeAs(requestEnvironment);

    log.info(taskName + " handing off");
    otherTaskPermit.release();
    thisTaskPermit.acquire();
    log.info(taskName + " checking for pollution of request environment");

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment before afterCompletion() in " + taskName)
        .containsAllEntriesOf(requestEnvironment)
        .hasSameSizeAs(requestEnvironment);

    log.info(taskName + " running afterCompletion()");

    interceptor.afterCompletion(request, null, null, null);

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment after afterCompletion() in " + taskName)
        .isEmpty();

    log.info(taskName + " handing off and terminating");
    otherTaskPermit.release();

    return null;
  }

  private static HttpServletRequest request(String taskName, Map<String, String> parameters) {
    HttpServletRequest request = mock(HttpServletRequest.class, taskName + " request");

    when(request.getParameterNames()).thenReturn(enumeration(parameters.keySet()));
    parameters.keySet()
        .forEach(name -> when(request.getParameter(name)).thenReturn(parameters.get(name)));

    when(request.getHeader(ResourceConstants.USER_NAME)).thenReturn(taskName + " admin");
    when(request.getHeader(ResourceConstants.PASSWORD)).thenReturn(taskName + " password");

    return request;
  }
}
