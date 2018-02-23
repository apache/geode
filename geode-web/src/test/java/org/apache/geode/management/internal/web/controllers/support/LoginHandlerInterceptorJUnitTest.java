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

import static org.junit.Assert.*;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * The LoginHandlerInterceptorJUnitTest class is a test suite of test cases to test the contract and
 * functionality of the Spring HandlerInterceptor, LoginHandlerInterceptor class.
 *
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 8.0
 */
@Category(UnitTest.class)
public class LoginHandlerInterceptorJUnitTest {

  private Mockery mockContext;

  private SecurityService securityService;

  @Before
  public void setUp() {
    mockContext = new Mockery();
    mockContext.setImposteriser(ClassImposteriser.INSTANCE);
    mockContext.setThreadingPolicy(new Synchroniser());
    LoginHandlerInterceptor.getEnvironment().clear();

    securityService = mockContext.mock(SecurityService.class);
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
    LoginHandlerInterceptor.getEnvironment().clear();
  }

  private String createEnvironmentVariable(final String name) {
    return (LoginHandlerInterceptor.ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + name);
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
  public void testPreHandleAfterCompletion() throws Exception {
    final Map<String, String> requestParameters = new HashMap<>(2);

    requestParameters.put("parameter", "one");
    requestParameters.put(createEnvironmentVariable("variable"), "two");

    final HttpServletRequest mockHttpRequest = mockContext.mock(HttpServletRequest.class,
        "testPreHandleAfterCompletion.HttpServletRequest");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockHttpRequest).getParameterNames();
        will(returnValue(enumeration(requestParameters.keySet().iterator())));
        oneOf(mockHttpRequest).getHeader(ResourceConstants.USER_NAME);
        will(returnValue("admin"));
        oneOf(mockHttpRequest).getHeader(ResourceConstants.PASSWORD);
        will(returnValue("password"));
        oneOf(mockHttpRequest).getParameter(with(equal(createEnvironmentVariable("variable"))));
        will(returnValue(requestParameters.get(createEnvironmentVariable("variable"))));
        oneOf(securityService).login(with(aNonNull(Properties.class)));
        oneOf(securityService).logout();
      }
    });

    LoginHandlerInterceptor handlerInterceptor = new LoginHandlerInterceptor(securityService);

    Map<String, String> envBefore = LoginHandlerInterceptor.getEnvironment();

    assertNotNull(envBefore);
    assertTrue(envBefore.isEmpty());
    assertTrue(handlerInterceptor.preHandle(mockHttpRequest, null, null));

    Map<String, String> envSet = LoginHandlerInterceptor.getEnvironment();

    assertNotNull(envSet);
    assertNotSame(envBefore, envSet);
    assertEquals(1, envSet.size());
    assertTrue(envSet.containsKey("variable"));
    assertEquals("two", envSet.get("variable"));

    handlerInterceptor.afterCompletion(mockHttpRequest, null, null, null);

    Map<String, String> envAfter = LoginHandlerInterceptor.getEnvironment();

    assertNotNull(envAfter);
    assertTrue(envAfter.isEmpty());
  }

  @Test
  public void testHandlerInterceptorThreadSafety() throws Throwable {
    TestFramework.runOnce(new HandlerInterceptorThreadSafetyMultiThreadedTestCase());
  }

  private class HandlerInterceptorThreadSafetyMultiThreadedTestCase extends MultithreadedTestCase {

    private LoginHandlerInterceptor handlerInterceptor;

    private HttpServletRequest mockHttpRequestOne;
    private HttpServletRequest mockHttpRequestTwo;

    @Override
    public void initialize() {
      super.initialize();

      final Map<String, String> requestParametersOne = new HashMap<>(3);

      requestParametersOne.put("param", "one");
      requestParametersOne.put(createEnvironmentVariable("STAGE"), "test");
      requestParametersOne.put(createEnvironmentVariable("GEODE_HOME"), "/path/to/gemfire/700");

      mockHttpRequestOne = mockContext.mock(HttpServletRequest.class,
          "testHandlerInterceptorThreadSafety.HttpServletRequest.1");

      mockContext.checking(new Expectations() {
        {
          oneOf(mockHttpRequestOne).getParameterNames();
          will(returnValue(enumeration(requestParametersOne.keySet().iterator())));
          oneOf(mockHttpRequestOne).getHeader(ResourceConstants.USER_NAME);
          will(returnValue("admin"));
          oneOf(mockHttpRequestOne).getHeader(ResourceConstants.PASSWORD);
          will(returnValue("password"));
          oneOf(mockHttpRequestOne).getParameter(with(equal(createEnvironmentVariable("STAGE"))));
          will(returnValue(requestParametersOne.get(createEnvironmentVariable("STAGE"))));
          oneOf(mockHttpRequestOne)
              .getParameter(with(equal(createEnvironmentVariable("GEODE_HOME"))));
          will(returnValue(requestParametersOne.get(createEnvironmentVariable("GEODE_HOME"))));
          oneOf(securityService).login(with(aNonNull(Properties.class)));
          oneOf(securityService).logout();
        }
      });

      mockHttpRequestTwo = mockContext.mock(HttpServletRequest.class,
          "testHandlerInterceptorThreadSafety.HttpServletRequest.2");

      final Map<String, String> requestParametersTwo = new HashMap<>(3);

      requestParametersTwo.put("parameter", "two");
      requestParametersTwo.put(createEnvironmentVariable("HOST"), "localhost");
      requestParametersTwo.put(createEnvironmentVariable("GEODE_HOME"), "/path/to/gemfire/75");

      mockContext.checking(new Expectations() {
        {
          oneOf(mockHttpRequestTwo).getParameterNames();
          will(returnValue(enumeration(requestParametersTwo.keySet().iterator())));
          oneOf(mockHttpRequestTwo).getHeader(ResourceConstants.USER_NAME);
          will(returnValue("admin"));
          oneOf(mockHttpRequestTwo).getHeader(ResourceConstants.PASSWORD);
          will(returnValue("password"));
          oneOf(mockHttpRequestTwo).getParameter(with(equal(createEnvironmentVariable("HOST"))));
          will(returnValue(requestParametersTwo.get(createEnvironmentVariable("HOST"))));
          oneOf(mockHttpRequestTwo)
              .getParameter(with(equal(createEnvironmentVariable("GEODE_HOME"))));
          will(returnValue(requestParametersTwo.get(createEnvironmentVariable("GEODE_HOME"))));
          oneOf(securityService).login(with(aNonNull(Properties.class)));
          oneOf(securityService).logout();
        }
      });

      handlerInterceptor = new LoginHandlerInterceptor(securityService);
    }

    public void thread1() throws Exception {
      assertTick(0);
      Thread.currentThread().setName("HTTP Request Processing Thread 1");

      Map<String, String> env = LoginHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertTrue(env.isEmpty());
      assertTrue(handlerInterceptor.preHandle(mockHttpRequestOne, null, null));

      env = LoginHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertEquals(2, env.size());
      assertFalse(env.containsKey("param"));
      assertFalse(env.containsKey("parameter"));
      assertFalse(env.containsKey("HOST"));
      assertFalse(env.containsKey("security-username"));
      assertFalse(env.containsKey("security-password"));
      assertEquals("test", env.get("STAGE"));
      assertEquals("/path/to/gemfire/700", env.get("GEODE_HOME"));

      waitForTick(2);

      env = LoginHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertEquals(2, env.size());
      assertFalse(env.containsKey("param"));
      assertFalse(env.containsKey("parameter"));
      assertFalse(env.containsKey("HOST"));
      assertFalse(env.containsKey("security-username"));
      assertFalse(env.containsKey("security-password"));
      assertEquals("test", env.get("STAGE"));
      assertEquals("/path/to/gemfire/700", env.get("GEODE_HOME"));

      waitForTick(4);

      env = LoginHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertEquals(2, env.size());
      assertFalse(env.containsKey("param"));
      assertFalse(env.containsKey("parameter"));
      assertFalse(env.containsKey("HOST"));
      assertFalse(env.containsKey("security-username"));
      assertFalse(env.containsKey("security-password"));
      assertEquals("test", env.get("STAGE"));
      assertEquals("/path/to/gemfire/700", env.get("GEODE_HOME"));

      handlerInterceptor.afterCompletion(mockHttpRequestOne, null, null, null);

      env = LoginHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertTrue(env.isEmpty());
    }

    public void thread2() throws Exception {
      assertTick(0);
      Thread.currentThread().setName("HTTP Request Processing Thread 2");
      waitForTick(1);

      Map<String, String> env = LoginHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertTrue(env.isEmpty());
      assertTrue(handlerInterceptor.preHandle(mockHttpRequestTwo, null, null));

      env = LoginHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertEquals(2, env.size());
      assertFalse(env.containsKey("parameter"));
      assertFalse(env.containsKey("param"));
      assertFalse(env.containsKey("STAGE"));
      assertFalse(env.containsKey("security-username"));
      assertFalse(env.containsKey("security-password"));
      assertEquals("localhost", env.get("HOST"));
      assertEquals("/path/to/gemfire/75", env.get("GEODE_HOME"));

      waitForTick(3);

      handlerInterceptor.afterCompletion(mockHttpRequestTwo, null, null, null);

      env = LoginHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertTrue(env.isEmpty());
    }

    @Override
    public void finish() {
      super.finish();
      handlerInterceptor = null;
    }
  }

}
