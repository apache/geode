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
package org.apache.geode.modules.session.internal.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.mockrunner.mock.web.MockHttpServletRequest;
import com.mockrunner.mock.web.MockHttpServletResponse;
import com.mockrunner.mock.web.MockServletContext;
import org.junit.Test;

import org.apache.geode.modules.session.filter.SessionCachingFilter;

/**
 * This servlet tests the effects of the downstream SessionCachingFilter filter. When these tests
 * are performed, the filter would already have taken effect.
 */
public abstract class CommonTests extends SessionCookieConfigServletTestCaseAdapter {
  static final String CONTEXT_PATH = "/test";

  @Test
  public void testGetSession1() {
    doFilter();
    HttpSession session1 = ((HttpServletRequest) getFilteredRequest()).getSession();
    HttpSession session2 = ((HttpServletRequest) getFilteredRequest()).getSession();

    assertSame("Session should be the same", session1, session2);
  }

  @Test
  public void testGetSession2() {
    doFilter();

    HttpSession session1 = ((HttpServletRequest) getFilteredRequest()).getSession();

    MockHttpServletResponse response = getWebMockObjectFactory().getMockResponse();
    Cookie cookie = (Cookie) response.getCookies().get(0);
    getWebMockObjectFactory().getMockRequest().addCookie(cookie);

    doFilter();

    HttpSession session2 = ((HttpServletRequest) getFilteredRequest()).getSession();

    assertEquals("Session objects across requests should be the same", session1, session2);
  }

  @Test
  public void testGetAttributeRequest1() {
    doFilter();

    getFilteredRequest().setAttribute("foo", "bar");

    assertEquals("bar", getFilteredRequest().getAttribute("foo"));
    assertNull("Unknown attribute should be null", getFilteredRequest().getAttribute("baz"));
  }

  @Test
  public void testGetAttributeRequest2() {
    // Setup
    CallbackServlet s = (CallbackServlet) getServlet();
    s.setCallback((request, response) -> request.setAttribute("foo", "bar"));
    doFilter();

    assertEquals("bar", getFilteredRequest().getAttribute("foo"));
    assertNull("Unknown attribute should be null", getFilteredRequest().getAttribute("baz"));
  }

  @Test
  public void testGetAttributeSession1() {
    doFilter();

    ((HttpServletRequest) getFilteredRequest()).getSession().setAttribute("foo", "bar");

    HttpServletRequest request = (HttpServletRequest) getFilteredRequest();
    assertEquals("bar", request.getSession().getAttribute("foo"));
  }

  /**
   * Are attributes preserved across client requests?
   */
  @Test
  public void testGetAttributeSession2() {
    doFilter();

    ((HttpServletRequest) getFilteredRequest()).getSession().setAttribute("foo", "bar");

    MockHttpServletResponse response = getWebMockObjectFactory().getMockResponse();
    Cookie cookie = (Cookie) response.getCookies().get(0);
    getWebMockObjectFactory().getMockRequest().addCookie(cookie);

    doFilter();
    HttpServletRequest request = (HttpServletRequest) getFilteredRequest();

    assertEquals("bar", request.getSession().getAttribute("foo"));
  }

  /**
   * Setting a session attribute to null should remove it
   */
  @Test
  public void testSetAttributeNullSession1() {
    // Setup
    CallbackServlet s = (CallbackServlet) getServlet();
    s.setCallback(new Callback() {
      private boolean called = false;

      @Override
      public void call(HttpServletRequest request, HttpServletResponse response) {
        if (called) {
          request.getSession().setAttribute("foo", null);
        } else {
          request.getSession().setAttribute("foo", "bar");
          called = true;
        }
      }
    });

    doFilter();
    doFilter();

    HttpSession session = ((HttpServletRequest) getFilteredRequest()).getSession();

    String attr = (String) session.getAttribute("foo");
    assertNull("Attribute should be null but is " + attr, attr);
  }


  /**
   * Test that various methods throw the appropriate exception when the session is invalid.
   */
  @Test
  public void testInvalidate1() {
    doFilter();

    HttpSession session = ((HttpServletRequest) getFilteredRequest()).getSession();
    session.invalidate();

    try {
      session.getAttribute("foo");
      fail("Session should be invalid and an exception should be thrown");
    } catch (IllegalStateException iex) {
      // Pass
    }
  }

  @Test
  public void testInvalidate2() {
    doFilter();

    HttpSession session = ((HttpServletRequest) getFilteredRequest()).getSession();
    session.invalidate();

    try {
      session.getAttributeNames();
      fail("Session should be invalid and an exception should be thrown");
    } catch (IllegalStateException iex) {
      // Pass
    }
  }

  @Test
  public void testInvalidate3() {
    doFilter();

    HttpSession session = ((HttpServletRequest) getFilteredRequest()).getSession();
    session.invalidate();

    try {
      session.getCreationTime();
      fail("Session should be invalid and an exception should be thrown");
    } catch (IllegalStateException iex) {
      // Pass
    }
  }

  @Test
  public void testInvalidate4() {
    doFilter();

    HttpSession session = ((HttpServletRequest) getFilteredRequest()).getSession();
    session.invalidate();

    try {
      session.getId();
    } catch (Exception iex) {
      fail("Exception should not be thrown");
    }
  }

  @Test
  public void testInvalidate5() {
    doFilter();

    HttpSession session = ((HttpServletRequest) getFilteredRequest()).getSession();
    session.invalidate();

    try {
      session.getLastAccessedTime();
      fail("Session should be invalid and an exception should be thrown");
    } catch (IllegalStateException iex) {
      // Pass
    }
  }

  @Test
  public void testInvalidate6() {
    doFilter();

    HttpSession session = ((HttpServletRequest) getFilteredRequest()).getSession();
    session.invalidate();

    try {
      session.getMaxInactiveInterval();
    } catch (Exception ex) {
      fail("Exception should not be thrown");
    }
  }

  @Test
  public void testInvalidate7() {
    doFilter();

    HttpSession session = ((HttpServletRequest) getFilteredRequest()).getSession();
    session.invalidate();

    try {
      session.getServletContext();
    } catch (Exception ex) {
      fail("Exception should not be thrown");
    }
  }

  @Test
  public void testInvalidate8() {
    doFilter();

    HttpSession session = ((HttpServletRequest) getFilteredRequest()).getSession();
    session.invalidate();

    try {
      session.isNew();
      fail("Session should be invalid and an exception should be thrown");
    } catch (IllegalStateException iex) {
      // Pass
    }
  }

  @Test
  public void testInvalidate9() {
    doFilter();

    HttpSession session = ((HttpServletRequest) getFilteredRequest()).getSession();
    session.invalidate();

    try {
      session.removeAttribute("foo");
      fail("Session should be invalid and an exception should be thrown");
    } catch (IllegalStateException iex) {
      // Pass
    }
  }

  @Test
  public void testInvalidate10() {
    doFilter();

    HttpSession session = ((HttpServletRequest) getFilteredRequest()).getSession();
    session.invalidate();

    try {
      session.setAttribute("foo", "bar");
      fail("Session should be invalid and an exception should be thrown");
    } catch (IllegalStateException iex) {
      // Pass
    }
  }

  @Test
  public void testInvalidate11() {
    doFilter();

    HttpSession session = ((HttpServletRequest) getFilteredRequest()).getSession();
    session.invalidate();

    try {
      session.setMaxInactiveInterval(1);
    } catch (Exception ex) {
      fail("Exception should not be thrown");
    }
  }

  @Test
  public void testGetId1() {
    doFilter();

    assertNotNull("Session Id should not be null",
        ((HttpServletRequest) getFilteredRequest()).getSession().getId());
  }

  /**
   * Test that multiple calls from the same client return the same session id
   */
  @Test
  public void testGetId2() {
    doFilter();

    String sessionId = ((HttpServletRequest) getFilteredRequest()).getSession().getId();

    MockHttpServletResponse response = getWebMockObjectFactory().getMockResponse();
    Cookie cookie = (Cookie) response.getCookies().get(0);
    getWebMockObjectFactory().getMockRequest().addCookie(cookie);

    doFilter();

    assertEquals("Session Ids should be the same", sessionId,
        ((HttpServletRequest) getFilteredRequest()).getSession().getId());
  }

  @Test
  public void testGetCreationTime1() {
    doFilter();

    HttpServletRequest request = (HttpServletRequest) getFilteredRequest();
    assertTrue("Session should have a non-zero creation time",
        request.getSession().getCreationTime() > 0);
  }


  /**
   * Test that multiple calls from the same client don't change the creation time.
   */
  @Test
  public void testGetCreationTime2() {
    doFilter();

    long creationTime = ((HttpServletRequest) getFilteredRequest()).getSession().getCreationTime();

    MockHttpServletResponse response = getWebMockObjectFactory().getMockResponse();
    Cookie cookie = (Cookie) response.getCookies().get(0);
    getWebMockObjectFactory().getMockRequest().addCookie(cookie);

    doFilter();

    assertEquals("Session creation time should be the same", creationTime,
        ((HttpServletRequest) getFilteredRequest()).getSession().getCreationTime());
  }

  @Test
  public void testResponseContainsRequestedSessionId1() {
    Cookie cookie = new Cookie("JSESSIONID", "999-GF");
    getWebMockObjectFactory().getMockRequest().addCookie(cookie);

    doFilter();

    HttpServletRequest request = (HttpServletRequest) getFilteredRequest();

    assertEquals("Request does not contain requested session ID", "999-GF",
        request.getRequestedSessionId());
  }

  @Test
  public void testGetLastAccessedTime1() {
    doFilter();

    HttpServletRequest request = (HttpServletRequest) getFilteredRequest();
    assertTrue("Session should have a non-zero last access time",
        request.getSession().getLastAccessedTime() > 0);
  }


  /**
   * Test that repeated accesses update the last accessed time
   */
  @Test
  public void testGetLastAccessedTime2() throws Exception {
    // Setup
    CallbackServlet s = (CallbackServlet) getServlet();
    s.setCallback((request, response) -> request.getSession());

    doFilter();

    HttpServletRequest request = (HttpServletRequest) getFilteredRequest();
    long lastAccess = request.getSession().getLastAccessedTime();
    assertTrue("Session should have a non-zero last access time", lastAccess > 0);

    MockHttpServletResponse response = getWebMockObjectFactory().getMockResponse();
    Cookie cookie = (Cookie) response.getCookies().get(0);

    MockHttpServletRequest mRequest = getWebMockObjectFactory().createMockRequest();
    mRequest.setRequestURL("/test/foo/bar");
    mRequest.setContextPath(CONTEXT_PATH);
    mRequest.addCookie(cookie);
    getWebMockObjectFactory().addRequestWrapper(mRequest);

    Thread.sleep(50);
    doFilter();

    assertTrue("Last access time should be changing",
        request.getSession().getLastAccessedTime() > lastAccess);
  }

  @Test
  public void testGetSetMaxInactiveInterval() {
    doFilter();

    HttpServletRequest request = (HttpServletRequest) getFilteredRequest();
    request.getSession().setMaxInactiveInterval(50);

    assertEquals(50, request.getSession().getMaxInactiveInterval());
  }

  @Test
  public void testCookieSecure() {

    boolean secure = true;
    asMyMockServlet(getWebMockObjectFactory().getMockServletContext()).getSessionCookieConfig()
        .setSecure(secure);

    doFilter();
    ((HttpServletRequest) getFilteredRequest()).getSession();

    MockHttpServletResponse response = getWebMockObjectFactory().getMockResponse();
    Cookie cookie = (Cookie) response.getCookies().get(0);

    assertEquals(secure, cookie.getSecure());
  }

  @Test
  public void testCookieHttpOnly() {

    boolean httpOnly = true;
    asMyMockServlet(getWebMockObjectFactory().getMockServletContext()).getSessionCookieConfig()
        .setHttpOnly(httpOnly);

    doFilter();
    ((HttpServletRequest) getFilteredRequest()).getSession();

    MockHttpServletResponse response = getWebMockObjectFactory().getMockResponse();
    Cookie cookie = (Cookie) response.getCookies().get(0);

    assertEquals(httpOnly, cookie.isHttpOnly());
  }

  @Test
  public void testIsNew1() {
    doFilter();

    HttpServletRequest request = (HttpServletRequest) getFilteredRequest();
    assertTrue("Session should be new", request.getSession().isNew());
  }

  /**
   * Subsequent calls should not return true
   */
  @Test
  public void testIsNew2() {
    // Setup
    CallbackServlet s = (CallbackServlet) getServlet();
    s.setCallback((request, response) -> request.getSession());

    doFilter();

    HttpServletRequest request = (HttpServletRequest) getFilteredRequest();
    request.getSession();

    MockHttpServletResponse response = getWebMockObjectFactory().getMockResponse();
    Cookie cookie = (Cookie) response.getCookies().get(0);

    MockHttpServletRequest mRequest = getWebMockObjectFactory().createMockRequest();
    mRequest.setRequestURL("/test/foo/bar");
    mRequest.setContextPath(CONTEXT_PATH);
    mRequest.addCookie(cookie);
    getWebMockObjectFactory().addRequestWrapper(mRequest);

    doFilter();

    request = (HttpServletRequest) getFilteredRequest();

    assertFalse("Subsequent isNew() calls should be false", request.getSession().isNew());
  }

  @Test
  public void testIsRequestedSessionIdFromCookie() {
    MockHttpServletRequest mRequest = getWebMockObjectFactory().getMockRequest();
    Cookie c = new Cookie("JSESSIONID", "1-GF");
    mRequest.addCookie(c);

    doFilter();
    HttpServletRequest request = (HttpServletRequest) getFilteredRequest();
    request.getSession();

    assertTrue(request.isRequestedSessionIdFromCookie());
  }

  @Test
  public void testIsRequestedSessionIdFromURL() {
    MockHttpServletRequest mRequest = getWebMockObjectFactory().getMockRequest();
    mRequest.setRequestURL("/foo/bar;jsessionid=1");

    doFilter();
    HttpServletRequest request = (HttpServletRequest) getFilteredRequest();
    request.getSession();

    assertFalse("Session ID should not be from cookie", request.isRequestedSessionIdFromCookie());
    assertTrue("Session ID should be from URL", request.isRequestedSessionIdFromURL());
  }

  @Test
  public void testOnlyOneSessionWhenSecondFilterWrapsRequest() {
    createFilter(RequestWrappingFilter.class);
    createFilter(SessionCachingFilter.class);
    doFilter();
    HttpServletRequest request = (HttpServletRequest) getFilteredRequest();
    HttpSession originalSession = (HttpSession) request.getAttribute("original_session");
    assertEquals(originalSession, request.getSession());
  }

  public static class RequestWrappingFilter implements Filter {

    @Override
    public void init(final FilterConfig filterConfig) {}

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
      final HttpServletRequest httpRequest = (HttpServletRequest) request;
      httpRequest.getSession();
      httpRequest.setAttribute("original_session", httpRequest.getSession());
      request = new HttpServletRequestWrapper(httpRequest);
      chain.doFilter(request, response);

    }

    @Override
    public void destroy() {}
  }

  private MyMockServletContext asMyMockServlet(final MockServletContext mockServletContext) {
    return (MyMockServletContext) mockServletContext;
  }

}
