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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.http.HttpServletRequest;

import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourceConstants;

/**
 * The LoginHandlerInterceptorJUnitTest class is a test suite of test cases to test the contract and
 * functionality of the Spring HandlerInterceptor, LoginHandlerInterceptor class.
 *
 * @see org.junit.Test
 * @since GemFire 8.0
 */
public class LoginHandlerInterceptorJUnitTest {
  private SecurityService securityService;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() {
    LoginHandlerInterceptor.getEnvironment().clear();
    securityService = mock(SecurityService.class);
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

    LoginHandlerInterceptor handlerInterceptor = new LoginHandlerInterceptor(securityService);
    Map<String, String> environmentBeforePreHandle = LoginHandlerInterceptor.getEnvironment();
    assertThat(environmentBeforePreHandle)
        .describedAs("environment before preHandle()")
        .isEmpty();

    assertThat(handlerInterceptor.preHandle(mockHttpRequest, null, null))
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

    LoginHandlerInterceptor handlerInterceptor = new LoginHandlerInterceptor(securityService);
    assertThat(handlerInterceptor.preHandle(mockHttpRequest, null, null))
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

    handlerInterceptor.afterCompletion(mockHttpRequest, null, null, null);

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment after afterCompletion()")
        .isEmpty();
    verify(securityService, times(1)).logout();
  }

  @Test
  public void testHandlerInterceptorThreadSafety() throws Throwable {
    TestFramework.runOnce(new HandlerInterceptorThreadSafetyMultiThreadedTestCase(securityService));
  }

  private static class HandlerInterceptorThreadSafetyMultiThreadedTestCase
      extends MultithreadedTestCase {
    private final SecurityService securityService;
    private HttpServletRequest request1;
    private HttpServletRequest request2;
    private LoginHandlerInterceptor handlerInterceptor;
    private final AtomicBoolean thread1Started = new AtomicBoolean(false);
    private final AtomicBoolean thread2Started = new AtomicBoolean(false);

    private HandlerInterceptorThreadSafetyMultiThreadedTestCase(SecurityService securityService) {
      this.securityService = securityService;
      setTrace(true);
    }

    @Override
    public void initialize() {
      super.initialize();

      Map<String, String> request1Parameters = new HashMap<>();
      request1Parameters
          .put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "STAGE", "request1 STAGE");
      request1Parameters.put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "GEODE_HOME",
          "request1 GEODE_HOME");

      request1 = mock(HttpServletRequest.class, "request1");
      when(request1.getParameterNames())
          .thenReturn(enumeration(request1Parameters.keySet()));
      when(request1.getHeader(ResourceConstants.USER_NAME)).thenReturn("admin");
      when(request1.getHeader(ResourceConstants.PASSWORD)).thenReturn("password");

      when(request1.getParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "STAGE"))
          .thenReturn(
              request1Parameters.get(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "STAGE"));
      when(request1
          .getParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "GEODE_HOME"))
              .thenReturn(
                  request1Parameters
                      .get(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "GEODE_HOME"));

      request2 =
          mock(HttpServletRequest.class, "request2");
      Map<String, String> request2Parameters = new HashMap<>();
      request2Parameters
          .put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "HOST", "request2 HOST");
      request2Parameters.put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "GEODE_HOME",
          "request2 GEODE_HOME");
      when(request2.getParameterNames())
          .thenReturn(enumeration(request2Parameters.keySet()));
      when(request2.getHeader(ResourceConstants.USER_NAME)).thenReturn("admin");
      when(request2.getHeader(ResourceConstants.PASSWORD)).thenReturn("password");
      when(request2.getParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "HOST"))
          .thenReturn(
              request2Parameters.get(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "HOST"));
      when(request2
          .getParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "GEODE_HOME"))
              .thenReturn(
                  request2Parameters
                      .get(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "GEODE_HOME"));

      handlerInterceptor = new LoginHandlerInterceptor(securityService);
    }

    @SuppressWarnings("unused")
    public void thread1() throws Exception {
      thread1Started.set(true);
      freezeClock();
      Awaitility.await("thread 1 waiting for thread 2 to start").atMost(4, TimeUnit.SECONDS)
          .until(thread2Started::get);
      unfreezeClock();

      String threadName = "HTTP Request Processing Thread 1";
      currentThread().setName(threadName);

      // Verify that this thread's environment starts empty
      assertThat(LoginHandlerInterceptor.getEnvironment())
          .describedAs("environment before preHandle() in " + threadName)
          .isEmpty();

      assertThat(handlerInterceptor.preHandle(request1, null, null))
          .describedAs("preHandle() result in " + threadName)
          .isTrue();

      // Verify that preHandle() populated this thread's environment
      assertThat(LoginHandlerInterceptor.getEnvironment())
          .describedAs("environment after preHandle() in " + threadName)
          .doesNotContainKeys("HOST")
          .hasSize(2)
          .containsEntry("STAGE", "request1 STAGE")
          .containsEntry("GEODE_HOME", "request1 GEODE_HOME");

      waitForTick(2);

      // Verify that the other thread's preHandle() did not affect this thread's environment
      assertThat(LoginHandlerInterceptor.getEnvironment())
          .describedAs("environment after tick 2 in " + threadName)
          .doesNotContainKeys("HOST")
          .hasSize(2)
          .containsEntry("STAGE", "request1 STAGE")
          .containsEntry("GEODE_HOME", "request1 GEODE_HOME");

      waitForTick(4);
      // The other thread has cleared its environment by calling afterCompletion()

      // Verify that the other thread's afterCompletion() did not affect this thread's environment
      assertThat(LoginHandlerInterceptor.getEnvironment())
          .describedAs("environment after tick 4 in " + threadName)
          .doesNotContainKeys("HOST")
          .hasSize(2)
          .containsEntry("STAGE", "request1 STAGE")
          .containsEntry("GEODE_HOME", "request1 GEODE_HOME");

      handlerInterceptor.afterCompletion(request1, null, null, null);

      assertThat(LoginHandlerInterceptor.getEnvironment())
          .describedAs("environment after afterCompletion() in " + threadName)
          .isEmpty();
    }

    @SuppressWarnings("unused")
    public void thread2() throws Exception {
      thread2Started.set(true);
      freezeClock();
      Awaitility.await("thread 2 waiting for thread 1 to start").atMost(4, TimeUnit.SECONDS)
          .until(thread1Started::get);
      unfreezeClock();

      String threadName = "HTTP Request Processing Thread 2";
      currentThread().setName(threadName);

      waitForTick(1);
      // The other thread has populated its environment by calling preHandle()

      // Verify that the other thread's preHandle() did not affect this thread's environment
      assertThat(LoginHandlerInterceptor.getEnvironment())
          .describedAs("environment before preHandle() in " + threadName)
          .isEmpty();

      assertThat(handlerInterceptor.preHandle(request2, null, null))
          .describedAs("preHandle() result in " + threadName)
          .isTrue();

      // Verify that preHandle() populated this thread's environment
      assertThat(LoginHandlerInterceptor.getEnvironment())
          .describedAs("environment after preHandle() in " + threadName)
          .doesNotContainKeys("STAGE")
          .hasSize(2)
          .containsEntry("HOST", "request2 HOST")
          .containsEntry("GEODE_HOME", "request2 GEODE_HOME");

      waitForTick(3);
      // The other thread has cleared its environment by calling afterCompletion()

      handlerInterceptor.afterCompletion(request2, null, null, null);

      // Verify that afterCompletion() cleared this thread's environment
      assertThat(LoginHandlerInterceptor.getEnvironment())
          .describedAs("environment after afterCompletion() in " + threadName)
          .isEmpty();
    }

    @Override
    public void finish() {
      super.finish();
      handlerInterceptor = null;
    }
  }
}
