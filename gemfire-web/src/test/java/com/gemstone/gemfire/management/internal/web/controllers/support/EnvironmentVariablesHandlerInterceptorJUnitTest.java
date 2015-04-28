/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.controllers.support;

import static org.junit.Assert.*;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

/**
 * The EnvironmentVariablesHandlerInterceptorJUnitTest class is a test suite of test cases to test the contract
 * and functionality of the Spring HandlerInterceptor, EnvironmentVariablesHandlerInterceptor class.
 * 
 * @author John Blum
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 8.0
 */
@Category(UnitTest.class)
public class EnvironmentVariablesHandlerInterceptorJUnitTest {

  private Mockery mockContext;

  @Before
  public void setUp() {
    mockContext = new Mockery();
    mockContext.setImposteriser(ClassImposteriser.INSTANCE);
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  protected String createEnvironmentVariable(final String name) {
    return (EnvironmentVariablesHandlerInterceptor.ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + name);
  }

  protected <T> Enumeration<T> enumeration(final Iterator<T> iterator) {
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

    final HttpServletRequest mockHttpRequest = mockContext.mock(HttpServletRequest.class, "testPreHandleAfterCompletion.HttpServletRequest");

    mockContext.checking(new Expectations() {{
      oneOf(mockHttpRequest).getParameterNames();
      will(returnValue(enumeration(requestParameters.keySet().iterator())));
      oneOf(mockHttpRequest).getParameter(with(equal(createEnvironmentVariable("variable"))));
      will(returnValue(requestParameters.get(createEnvironmentVariable("variable"))));
    }});

    EnvironmentVariablesHandlerInterceptor handlerInterceptor = new EnvironmentVariablesHandlerInterceptor();

    Map<String, String> envBefore = EnvironmentVariablesHandlerInterceptor.getEnvironment();

    assertNotNull(envBefore);
    assertTrue(envBefore.isEmpty());
    assertTrue(handlerInterceptor.preHandle(mockHttpRequest, null, null));

    Map<String, String> envSet = EnvironmentVariablesHandlerInterceptor.getEnvironment();

    assertNotNull(envSet);
    assertNotSame(envBefore, envSet);
    assertEquals(1, envSet.size());
    assertTrue(envSet.containsKey("variable"));
    assertEquals("two", envSet.get("variable"));

    handlerInterceptor.afterCompletion(mockHttpRequest, null, null, null);

    Map<String, String> envAfter = EnvironmentVariablesHandlerInterceptor.getEnvironment();

    assertNotNull(envAfter);
    assertTrue(envAfter.isEmpty());
  }

  @Test
  public void testHandlerInterceptorThreadSafety() throws Throwable {
    TestFramework.runOnce(new HandlerInterceptorThreadSafetyMultiThreadedTestCase());
  }

  protected final class HandlerInterceptorThreadSafetyMultiThreadedTestCase extends MultithreadedTestCase {

    private EnvironmentVariablesHandlerInterceptor handlerInterceptor;

    private HttpServletRequest mockHttpRequestOne;
    private HttpServletRequest mockHttpRequestTwo;

    @Override
    public void initialize() {
      super.initialize();

      final Map<String, String> requestParametersOne = new HashMap<>(3);

      requestParametersOne.put("param", "one");
      requestParametersOne.put(createEnvironmentVariable("STAGE"), "test");
      requestParametersOne.put(createEnvironmentVariable("GEMFIRE"), "/path/to/gemfire/700");

      mockHttpRequestOne = mockContext.mock(HttpServletRequest.class, "testHandlerInterceptorThreadSafety.HttpServletRequest.1");

      mockContext.checking(new Expectations() {{
        oneOf(mockHttpRequestOne).getParameterNames();
        will(returnValue(enumeration(requestParametersOne.keySet().iterator())));
        oneOf(mockHttpRequestOne).getParameter(with(equal(createEnvironmentVariable("STAGE"))));
        will(returnValue(requestParametersOne.get(createEnvironmentVariable("STAGE"))));
        oneOf(mockHttpRequestOne).getParameter(with(equal(createEnvironmentVariable("GEMFIRE"))));
        will(returnValue(requestParametersOne.get(createEnvironmentVariable("GEMFIRE"))));
      }});

      mockHttpRequestTwo = mockContext.mock(HttpServletRequest.class, "testHandlerInterceptorThreadSafety.HttpServletRequest.2");

      final Map<String, String> requestParametersTwo = new HashMap<>(3);

      requestParametersTwo.put("parameter", "two");
      requestParametersTwo.put(createEnvironmentVariable("HOST"), "localhost");
      requestParametersTwo.put(createEnvironmentVariable("GEMFIRE"), "/path/to/gemfire/75");

      mockContext.checking(new Expectations() {{
        oneOf(mockHttpRequestTwo).getParameterNames();
        will(returnValue(enumeration(requestParametersTwo.keySet().iterator())));
        oneOf(mockHttpRequestTwo).getParameter(with(equal(createEnvironmentVariable("HOST"))));
        will(returnValue(requestParametersTwo.get(createEnvironmentVariable("HOST"))));
        oneOf(mockHttpRequestTwo).getParameter(with(equal(createEnvironmentVariable("GEMFIRE"))));
        will(returnValue(requestParametersTwo.get(createEnvironmentVariable("GEMFIRE"))));
      }});

      handlerInterceptor =  new EnvironmentVariablesHandlerInterceptor();
    }

    public void thread1() throws Exception {
      assertTick(0);
      Thread.currentThread().setName("HTTP Request Processing Thread 1");

      Map<String, String> env = EnvironmentVariablesHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertTrue(env.isEmpty());
      assertTrue(handlerInterceptor.preHandle(mockHttpRequestOne, null, null));

      env = EnvironmentVariablesHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertEquals(2, env.size());
      assertFalse(env.containsKey("param"));
      assertFalse(env.containsKey("parameter"));
      assertFalse(env.containsKey("HOST"));
      assertEquals("test", env.get("STAGE"));
      assertEquals("/path/to/gemfire/700", env.get("GEMFIRE"));

      waitForTick(2);

      env = EnvironmentVariablesHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertEquals(2, env.size());
      assertFalse(env.containsKey("param"));
      assertFalse(env.containsKey("parameter"));
      assertFalse(env.containsKey("HOST"));
      assertEquals("test", env.get("STAGE"));
      assertEquals("/path/to/gemfire/700", env.get("GEMFIRE"));

      waitForTick(4);

      env = EnvironmentVariablesHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertEquals(2, env.size());
      assertFalse(env.containsKey("param"));
      assertFalse(env.containsKey("parameter"));
      assertFalse(env.containsKey("HOST"));
      assertEquals("test", env.get("STAGE"));
      assertEquals("/path/to/gemfire/700", env.get("GEMFIRE"));

      handlerInterceptor.afterCompletion(mockHttpRequestOne, null, null, null);

      env = EnvironmentVariablesHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertTrue(env.isEmpty());
    }

    public void thread2() throws Exception {
      assertTick(0);
      Thread.currentThread().setName("HTTP Request Processing Thread 2");
      waitForTick(1);

      Map<String, String> env = EnvironmentVariablesHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertTrue(env.isEmpty());
      assertTrue(handlerInterceptor.preHandle(mockHttpRequestTwo, null, null));

      env = EnvironmentVariablesHandlerInterceptor.getEnvironment();

      assertNotNull(env);
      assertEquals(2, env.size());
      assertFalse(env.containsKey("parameter"));
      assertFalse(env.containsKey("param"));
      assertFalse(env.containsKey("STAGE"));
      assertEquals("localhost", env.get("HOST"));
      assertEquals("/path/to/gemfire/75", env.get("GEMFIRE"));

      waitForTick(3);

      handlerInterceptor.afterCompletion(mockHttpRequestTwo, null, null, null);

      env = EnvironmentVariablesHandlerInterceptor.getEnvironment();

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
