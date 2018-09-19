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
import static java.util.Collections.synchronizedSet;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toMap;
import static org.apache.geode.management.internal.security.ResourceConstants.PASSWORD;
import static org.apache.geode.management.internal.security.ResourceConstants.USER_NAME;
import static org.apache.geode.management.internal.web.controllers.support.LoginHandlerInterceptor.ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.math.BigInteger;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletRequest;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mock;
import org.springframework.web.servlet.HandlerInterceptor;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.test.junit.rules.ConcurrencyRule;

public class LoginHandlerInterceptorJUnitTest {
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
  public void eachRequestThreadsEnvironmentIsConfinedToItsThread() throws Throwable {
//    int contentionThreadCount = 1 + Runtime.getRuntime().availableProcessors() / 2;
    int contentionThreadCount = Runtime.getRuntime().availableProcessors();
    CPUContentionService contentionService = new CPUContentionService(contentionThreadCount);

    Semaphore primaryTaskPermit = new Semaphore(0);
    Semaphore interferingTaskPermit = new Semaphore(0);

    // Uncomment the following line to create contention for CPUs
    contentionService.start();

    Callable<Void> primaryTask = () -> primaryTask(primaryTaskPermit, interferingTaskPermit);
    Callable<Void> interferingTask = () -> interferingTask(primaryTaskPermit, interferingTaskPermit);

    try {
      runConcurrently.setTimeout(Duration.ofSeconds(1));
      runConcurrently.add(primaryTask);
      runConcurrently.add(interferingTask);
      runConcurrently.executeInParallel();
    } finally {
      contentionService.stop(Duration.ofSeconds(10));
    }
  }

  private Void primaryTask(Semaphore primaryTaskPermissionToRun,
                           Semaphore interferingTaskPermissionToRun) throws Exception {
    String taskName = "primary task";
    currentThread().setName(taskName);
    System.out.println(taskName + " starting, running preHandle()");

    // Only this thread's request has a STAGE parameter. Each thread's requests has a GEODE_HOME
    // parameter, but the value is distinct in each request.
    String[] parameterNames = { "GEODE_HOME", "STAGE" };

    // Create a map of parameters, where each entry has a name name prefixed so that the handler
    // will recognize it as an environment variable, and a value peculiar to this task.
    Map<String, String> requestParameters = Stream.of(parameterNames)
        .collect(
            toMap(
                name -> ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + name,
                name -> taskName + " " + name));

    // Create a map of environment expected environment variables, where each entry has a value
    // peculiar to this task.
    Map<String, String> expectedRequestEnvironment = Stream.of(parameterNames)
        .collect(
            toMap(
                name -> name,
                name -> taskName + " " + name));
    
    HttpServletRequest request = request(taskName, requestParameters);
    
    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment before preHandle() in " + taskName)
        .isEmpty();

    assertThat(interceptor.preHandle(request, null, null))
        .describedAs("preHandle() result in " + taskName)
        .isTrue();

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment after preHandle() in " + taskName)
        .containsAllEntriesOf(expectedRequestEnvironment)
        .hasSameSizeAs(expectedRequestEnvironment);

    System.out.println(taskName + " handing off after preHandle()");
    interferingTaskPermissionToRun.release();
    primaryTaskPermissionToRun.acquire();
    System.out.println(taskName + " checking for preHandle() pollution");

    // Verify that the interfering thread's preHandle() did not affect this thread's environment
    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment after tick 2 in " + taskName)
        .containsAllEntriesOf(expectedRequestEnvironment)
        .hasSameSizeAs(expectedRequestEnvironment);

    System.out.println(taskName + " handing off after checking for preHandle() pollution");
    interferingTaskPermissionToRun.release();
    primaryTaskPermissionToRun.acquire();
    System.out.println(taskName + " checking for afterCompletion() pollution and running afterCompletion()");
    // The interfering thread has cleared its environment by calling afterCompletion()

    // Verify that the interfering thread's afterCompletion() did not affect this thread's environment
    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment after tick 4 in " + taskName)
        .containsAllEntriesOf(expectedRequestEnvironment)
        .hasSameSizeAs(expectedRequestEnvironment);

    interceptor.afterCompletion(request, null, null, null);

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment after afterCompletion() in " + taskName)
        .isEmpty();

    System.out.println(taskName + " handing off and terminating after afterCompletion()");
    interferingTaskPermissionToRun.release();

    return null;
  }

  private Void interferingTask(Semaphore primaryTaskPermissionToRun,
                               Semaphore interferingTaskPermissionToRun) throws Exception {
    String taskName = "interfering task";
    currentThread().setName(taskName);
    
    // Only this thread's request has a HOST parameter. Each thread's request has a GEODE_HOME
    // parameter, but the value is distinct in each request.
    String[] parameterNames = { "GEODE_HOME", "HOST"};

    // Create a map of parameters, where each entry has a name prefixed so that the handler will
    // recognize it as an environment variable, and a value peculiar to this task.
    Map<String, String> requestParameters = Stream.of(parameterNames)
        .collect(
            toMap(
                name -> ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + name,
                name -> taskName + " " + name));

    // Create a map of expected environment variables, where each entry has a value peculiar to this
    // task.
    Map<String, String> expectedRequestEnvironment = Stream.of(parameterNames)
        .collect(
            toMap(
                name -> name,
                name -> taskName + " " + name));
    
    HttpServletRequest request = request(taskName, requestParameters);

    System.out.println(taskName + " starting");

    System.out.println(taskName + " awaiting permission to preHandle()");
    interferingTaskPermissionToRun.acquire();
    // The primary thread has populated its environment by calling preHandle()

    // Verify that the primary thread's preHandle() did not affect this thread's environment
    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment before preHandle() in " + taskName)
        .isEmpty();

    assertThat(interceptor.preHandle(request, null, null))
        .describedAs("preHandle() result in " + taskName)
        .isTrue();

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment after preHandle() in " + taskName)
        .containsAllEntriesOf(expectedRequestEnvironment)
        .hasSameSizeAs(expectedRequestEnvironment);

    System.out.println(taskName + " handing off after preHandle()");
    primaryTaskPermissionToRun.release();
    interferingTaskPermissionToRun.acquire();
    System.out.println(taskName + " running afterCompletion()");
    // The primary thread has cleared its environment by calling afterCompletion()

    interceptor.afterCompletion(request, null, null, null);

    assertThat(LoginHandlerInterceptor.getEnvironment())
        .describedAs("environment after afterCompletion() in " + taskName)
        .isEmpty();

    System.out.println(taskName + " handing off and terminating after afterCompletion()");
    primaryTaskPermissionToRun.release();
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

  /**
   * Keeps CPUs busy by searching for probable primes.
   */
  private static class CPUContentionService {
    private final ExecutorService executor;
    private final int threadCount;
    private final Set<BigInteger> primes = synchronizedSet(new HashSet<>());

    public CPUContentionService(int threadCount) {
      this.executor = newFixedThreadPool(threadCount);
      this.threadCount = threadCount;
    }

    public void start() {
      for (int i = 0; i < threadCount; i++) {
        executor.submit(generatePrimes(primes), i);
      }
    }

    public void stop(Duration duration) throws InterruptedException {
      executor.shutdownNow();
      executor.awaitTermination(duration.toMillis(), MILLISECONDS);
    }

    private static Runnable generatePrimes(Set<BigInteger> primes) {
      return () -> {
        Random random = new Random();
        while (true) {
          if (Thread.currentThread().isInterrupted()) {
            return;
          }
          primes.add(BigInteger.probablePrime(1000, random));
        }
      };
    }
  }
}
