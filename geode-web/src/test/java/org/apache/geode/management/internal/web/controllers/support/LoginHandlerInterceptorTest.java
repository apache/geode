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
import static org.apache.geode.management.internal.web.controllers.support.LoginHandlerInterceptorTest.RequestBuilder.request;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mock;
import org.springframework.web.servlet.HandlerInterceptor;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.rules.ConcurrencyRule;

public class LoginHandlerInterceptorTest {
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

  @Test
  public void beforeFirstCallToPreHandle_environmentIsEmpty() {
    assertThat(LoginHandlerInterceptor.getEnvironment()).isEmpty();
  }

  @Test
  public void preHandle_createsNewEnvironmentInstance() throws Exception {
    HttpServletRequest request = request().build();

    Map<String, String> environmentBeforePreHandle = LoginHandlerInterceptor.getEnvironment();

    interceptor.preHandle(request, null, null);

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .isNotSameAs(environmentBeforePreHandle);
  }

  @Test
  public void preHandle_copiesPrefixedRequestParametersIntoEnvironment() throws Exception {
    HttpServletRequest request = request()
        .withParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "prefixed", "prefixed value")
        .withParameter("not-prefixed", "not-prefixed value")
        .build();

    interceptor.preHandle(request, null, null);

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .containsOnly(entry("prefixed", "prefixed value"))
        .doesNotContain(entry("not-prefixed", "not-prefixed value"));
  }

  @Test
  public void preHandle_returnsTrue() throws Exception {
    HttpServletRequest request = request().build();

    assertThat(interceptor.preHandle(request, null, null))
        .isTrue();
  }

  @Test
  public void preHandle_logsInWithUserNameAndPasswordFromRequestHeaders() throws Exception {
    HttpServletRequest request = request()
        .withHeader(USER_NAME, "expected user-name")
        .withHeader(PASSWORD, "expected password")
        .build();

    interceptor.preHandle(request, null, null);

    Properties expectedLoginProperties = new Properties();
    expectedLoginProperties.setProperty(USER_NAME, "expected user-name");
    expectedLoginProperties.setProperty(PASSWORD, "expected password");

    verify(securityService, times(1)).login(expectedLoginProperties);
  }

  @Test
  public void afterCompletion_clearsTheEnvironment() throws Exception {
    HttpServletRequest request = request()
        .withParameter(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "variable", "value")
        .build();

    // Call preHandle() to put values into the environment
    interceptor.preHandle(request, null, null);

    interceptor.afterCompletion(request, null, null, null);

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .isEmpty();
  }

  @Test
  public void afterCompletion_logsOut() throws Exception {
    HttpServletRequest request = request().build();

    interceptor.afterCompletion(request, null, null, null);

    verify(securityService, times(1)).logout();
  }

  @Test
  public void eachRequestThreadEnvironmentIsConfinedToItsThread() {
    Semaphore thread1Permit = new Semaphore(0);
    Semaphore thread2Permit = new Semaphore(0);

    Callable<Void> request1Task = () -> processRequest("thread 1", thread1Permit, thread2Permit);
    Callable<Void> request2Task = () -> processRequest("thread 2", thread2Permit, thread1Permit);

    runConcurrently.setTimeout(Duration.ofMinutes(1));
    runConcurrently.add(request1Task);
    runConcurrently.add(request2Task);
    thread1Permit.release();
    runConcurrently.executeInParallel();
  }

  private Void processRequest(String taskName, Semaphore thisTaskPermit, Semaphore otherTaskPermit)
      throws Exception {
    currentThread().setName(taskName);
    System.out.println(taskName + " started");

    // sync up threads to allow both to start before proceeding
    thisTaskPermit.acquire();

    System.out.println(taskName + " handing off");
    otherTaskPermit.release();
    thisTaskPermit.acquire();
    System.out.println(taskName + " running preHandle()");

    // Each task has a unique value for this common parameter. If the interceptor is threadsafe,
    // neither task's unique value will appear in the other task's environment.
    String commonParameterName = ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + "COMMON-PARAMETER";
    String commonParameterValue = "COMMON-PARAMETER value for " + taskName;

    // Each task has a parameter with a name and value unique to the task. If the interceptor is
    // threadsafe, neither task's unique parameter name or value will appear in the other task's
    // environment.
    String uniqueParameterName = ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX
        + "REQUEST-SPECIFIC-PARAMETER " + taskName;
    String uniqueParameterValue = "REQUEST-SPECIFIC-PARAMETER value for " + taskName;

    HttpServletRequest request = request()
        .named(taskName + " request")
        .withParameter(commonParameterName, commonParameterValue)
        .withParameter(uniqueParameterName, uniqueParameterValue)
        .build();

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment before preHandle() in " + taskName)
        .isEmpty();

    interceptor.preHandle(request, null, null);

    Map<String, String> requestEnvironment = LoginHandlerInterceptor.getEnvironment();

    System.out.println(taskName + " handing off");
    otherTaskPermit.release();
    thisTaskPermit.acquire();
    System.out.println(taskName + " checking for pollution of request environment");

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment remains unchanged in " + taskName)
        .containsAllEntriesOf(requestEnvironment)
        .hasSameSizeAs(requestEnvironment);

    System.out.println(taskName + " handing off");
    otherTaskPermit.release();
    thisTaskPermit.acquire();
    System.out.println(taskName + " checking for pollution of request environment");

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment before afterCompletion() in " + taskName)
        .containsAllEntriesOf(requestEnvironment)
        .hasSameSizeAs(requestEnvironment);

    System.out.println(taskName + " running afterCompletion()");

    interceptor.afterCompletion(request, null, null, null);

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment after afterCompletion() in " + taskName)
        .isEmpty();

    System.out.println(taskName + " handing off and terminating");
    otherTaskPermit.release();

    return null;
  }

  static class RequestBuilder {
    private final Map<String, String> parameters = new HashMap<>();
    private final Map<String, String> headers = new HashMap<>();
    private String name = "request";

    static RequestBuilder request() {
      return new RequestBuilder();
    }

    HttpServletRequest build() {
      HttpServletRequest request = mock(HttpServletRequest.class, name);

      headers.keySet().forEach(k -> when(request.getHeader(k)).thenReturn(headers.get(k)));

      when(request.getParameterNames()).thenReturn(enumeration(parameters.keySet()));
      parameters.keySet().forEach(k -> when(request.getParameter(k)).thenReturn(parameters.get(k)));

      return request;
    }

    RequestBuilder named(String name) {
      this.name = name;
      return this;
    }

    RequestBuilder withHeader(String name, String value) {
      headers.put(name, value);
      return this;
    }

    RequestBuilder withParameter(String name, String value) {
      parameters.put(name, value);
      return this;
    }
  }
}
