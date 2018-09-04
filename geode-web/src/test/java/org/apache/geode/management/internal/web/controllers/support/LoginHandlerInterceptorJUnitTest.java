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

import static org.apache.geode.management.internal.security.ResourceConstants.PASSWORD;
import static org.apache.geode.management.internal.security.ResourceConstants.USER_NAME;
import static org.apache.geode.management.internal.web.controllers.support.LoginHandlerInterceptor.ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;
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

  private <T> Enumeration<T> enumeration(final Iterator<T> iterator) {
    return new Enumeration<T>() {
      public boolean hasMoreElements() {
        return iterator.hasNext();
      }

      public T nextElement() {
        return iterator.next();
      }
    };
  }

  @Test
  public void preHandleShouldSetEnvironmentVariablesFromSpecificRequestParameters()
      throws Exception {
    final Map<String, String> requestParameters = new HashMap<>(2);
    requestParameters.put("parameter", "one");
    requestParameters.put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "variable", "two");
    final HttpServletRequest mockHttpRequest = mock(HttpServletRequest.class, name.getMethodName());
    when(mockHttpRequest.getParameterNames())
        .thenReturn(enumeration(requestParameters.keySet().iterator()));
    when(mockHttpRequest.getHeader(USER_NAME)).thenReturn("admin");
    when(mockHttpRequest.getHeader(PASSWORD)).thenReturn("password");
    when(mockHttpRequest.getParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "variable"))
        .thenReturn("two");

    LoginHandlerInterceptor handlerInterceptor = new LoginHandlerInterceptor(securityService);
    Map<String, String> environmentBeforePreHandle = LoginHandlerInterceptor.getEnvironment();
    assertThat(environmentBeforePreHandle).isNotNull();
    assertThat(environmentBeforePreHandle.isEmpty()).isTrue();

    assertThat(handlerInterceptor.preHandle(mockHttpRequest, null, null)).isTrue();
    Map<String, String> environmentAfterPreHandle = LoginHandlerInterceptor.getEnvironment();
    assertThat(environmentAfterPreHandle).isNotNull();
    assertThat(environmentBeforePreHandle).isNotSameAs(environmentAfterPreHandle);
    assertThat(1).isEqualTo(environmentAfterPreHandle.size());
    assertThat(environmentAfterPreHandle.containsKey("variable")).isTrue();
    assertThat("two").isEqualTo(environmentAfterPreHandle.get("variable"));
    Properties expectedLoginProperties = new Properties();
    expectedLoginProperties.put(USER_NAME, "admin");
    expectedLoginProperties.put(PASSWORD, "password");
    verify(securityService, times(1)).login(expectedLoginProperties);
  }

  @Test
  public void afterCompletionShouldCleanTheEnvironment() throws Exception {
    final Map<String, String> requestParameters = new HashMap<>(2);
    requestParameters.put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "variable", "two");
    final HttpServletRequest mockHttpRequest = mock(HttpServletRequest.class, name.getMethodName());
    when(mockHttpRequest.getParameterNames())
        .thenReturn(enumeration(requestParameters.keySet().iterator()));
    when(mockHttpRequest.getHeader(USER_NAME)).thenReturn("admin");
    when(mockHttpRequest.getHeader(PASSWORD)).thenReturn("password");
    when(mockHttpRequest.getParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "variable"))
        .thenReturn("two");

    LoginHandlerInterceptor handlerInterceptor = new LoginHandlerInterceptor(securityService);
    assertThat(handlerInterceptor.preHandle(mockHttpRequest, null, null)).isTrue();
    Map<String, String> environmentAfterPreHandle = LoginHandlerInterceptor.getEnvironment();
    assertThat(environmentAfterPreHandle).isNotNull();
    assertThat(1).isEqualTo(environmentAfterPreHandle.size());
    assertThat(environmentAfterPreHandle.containsKey("variable")).isTrue();
    assertThat("two").isEqualTo(environmentAfterPreHandle.get("variable"));
    Properties expectedLoginProperties = new Properties();
    expectedLoginProperties.put(USER_NAME, "admin");
    expectedLoginProperties.put(PASSWORD, "password");
    verify(securityService, times(1)).login(expectedLoginProperties);

    handlerInterceptor.afterCompletion(mockHttpRequest, null, null, null);
    Map<String, String> environmentAfterCompletion = LoginHandlerInterceptor.getEnvironment();
    assertThat(environmentAfterCompletion).isNotNull();
    assertThat(environmentAfterCompletion.isEmpty()).isTrue();
    verify(securityService, times(1)).logout();
  }

  @Test
  public void testHandlerInterceptorThreadSafety() throws Throwable {
    TestFramework.runOnce(new HandlerInterceptorThreadSafetyMultiThreadedTestCase());
  }

  private class HandlerInterceptorThreadSafetyMultiThreadedTestCase extends MultithreadedTestCase {
    private HttpServletRequest mockHttpRequestOne;
    private HttpServletRequest mockHttpRequestTwo;
    private LoginHandlerInterceptor handlerInterceptor;

    @Override
    public void initialize() {
      super.initialize();

      final Map<String, String> requestParametersOne = new HashMap<>(3);
      requestParametersOne.put("param", "one");
      requestParametersOne.put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "STAGE", "test");
      requestParametersOne.put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "GEODE_HOME",
          "/path/to/geode");

      mockHttpRequestOne =
          mock(HttpServletRequest.class, "testHandlerInterceptorThreadSafety.HttpServletRequest.1");
      when(mockHttpRequestOne.getParameterNames())
          .thenReturn(enumeration(requestParametersOne.keySet().iterator()));
      when(mockHttpRequestOne.getHeader(ResourceConstants.USER_NAME)).thenReturn("admin");
      when(mockHttpRequestOne.getHeader(ResourceConstants.PASSWORD)).thenReturn("password");
      when(mockHttpRequestOne.getParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "STAGE"))
          .thenReturn("test");
      when(mockHttpRequestOne
          .getParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "GEODE_HOME"))
              .thenReturn("/path/to/geode");

      mockHttpRequestTwo =
          mock(HttpServletRequest.class, "testHandlerInterceptorThreadSafety.HttpServletRequest.2");
      final Map<String, String> requestParametersTwo = new HashMap<>(3);
      requestParametersTwo.put("parameter", "two");
      requestParametersTwo.put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "HOST", "localhost");
      requestParametersTwo.put(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "GEODE_HOME",
          "/path/to/geode/180");
      when(mockHttpRequestTwo.getParameterNames())
          .thenReturn(enumeration(requestParametersTwo.keySet().iterator()));
      when(mockHttpRequestTwo.getHeader(ResourceConstants.USER_NAME)).thenReturn("admin");
      when(mockHttpRequestTwo.getHeader(ResourceConstants.PASSWORD)).thenReturn("password");
      when(mockHttpRequestTwo.getParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "HOST"))
          .thenReturn("localhost");
      when(mockHttpRequestTwo
          .getParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "GEODE_HOME"))
              .thenReturn("/path/to/geode/180");

      handlerInterceptor = new LoginHandlerInterceptor(securityService);
    }

    @SuppressWarnings("unused")
    public void thread1() throws Exception {
      assertTick(0);
      Thread.currentThread().setName("HTTP Request Processing Thread 1");

      Map<String, String> env = LoginHandlerInterceptor.getEnvironment();
      assertThat(env).isNotNull();
      assertThat(env.isEmpty()).isTrue();
      assertThat(handlerInterceptor.preHandle(mockHttpRequestOne, null, null)).isTrue();

      env = LoginHandlerInterceptor.getEnvironment();
      assertThat(env).isNotNull();
      assertThat(2).isEqualTo(env.size());
      assertThat(env.containsKey("param")).isFalse();
      assertThat(env.containsKey("parameter")).isFalse();
      assertThat(env.containsKey("HOST")).isFalse();
      assertThat(env.containsKey("security-username")).isFalse();
      assertThat(env.containsKey("security-password")).isFalse();
      assertThat("test").isEqualTo(env.get("STAGE"));
      assertThat("/path/to/geode").isEqualTo(env.get("GEODE_HOME"));

      waitForTick(2);
      env = LoginHandlerInterceptor.getEnvironment();
      assertThat(env).isNotNull();
      assertThat(2).isEqualTo(env.size());
      assertThat(env.containsKey("param")).isFalse();
      assertThat(env.containsKey("parameter")).isFalse();
      assertThat(env.containsKey("HOST")).isFalse();
      assertThat(env.containsKey("security-username")).isFalse();
      assertThat(env.containsKey("security-password")).isFalse();
      assertThat("test").isEqualTo(env.get("STAGE"));
      assertThat("/path/to/geode").isEqualTo(env.get("GEODE_HOME"));

      waitForTick(4);
      env = LoginHandlerInterceptor.getEnvironment();
      assertThat(env).isNotNull();
      assertThat(2).isEqualTo(env.size());
      assertThat(env.containsKey("param")).isFalse();
      assertThat(env.containsKey("parameter")).isFalse();
      assertThat(env.containsKey("HOST")).isFalse();
      assertThat(env.containsKey("security-username")).isFalse();
      assertThat(env.containsKey("security-password")).isFalse();
      assertThat("test").isEqualTo(env.get("STAGE"));
      assertThat("/path/to/geode").isEqualTo(env.get("GEODE_HOME"));

      handlerInterceptor.afterCompletion(mockHttpRequestOne, null, null, null);
      env = LoginHandlerInterceptor.getEnvironment();
      assertNotNull(env);
      assertTrue(env.isEmpty());
    }

    @SuppressWarnings("unused")
    public void thread2() throws Exception {
      assertTick(0);
      Thread.currentThread().setName("HTTP Request Processing Thread 2");

      waitForTick(1);
      Map<String, String> env = LoginHandlerInterceptor.getEnvironment();
      assertThat(env).isNotNull();
      assertThat(env.isEmpty()).isTrue();
      assertThat(handlerInterceptor.preHandle(mockHttpRequestTwo, null, null)).isTrue();

      env = LoginHandlerInterceptor.getEnvironment();
      assertThat(env).isNotNull();
      assertThat(2).isEqualTo(env.size());
      assertThat(env.containsKey("parameter")).isFalse();
      assertThat(env.containsKey("param")).isFalse();
      assertThat(env.containsKey("STAGE")).isFalse();
      assertThat(env.containsKey("security-username")).isFalse();
      assertThat(env.containsKey("security-password")).isFalse();
      assertThat("localhost").isEqualTo(env.get("HOST"));
      assertThat("/path/to/geode/180").isEqualTo(env.get("GEODE_HOME"));

      waitForTick(3);
      handlerInterceptor.afterCompletion(mockHttpRequestTwo, null, null, null);
      env = LoginHandlerInterceptor.getEnvironment();
      assertThat(env).isNotNull();
      assertThat(env.isEmpty()).isTrue();
    }

    @Override
    public void finish() {
      super.finish();
      handlerInterceptor = null;
    }
  }
}
