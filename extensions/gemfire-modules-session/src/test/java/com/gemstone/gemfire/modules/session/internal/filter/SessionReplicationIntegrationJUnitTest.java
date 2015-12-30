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

package com.gemstone.gemfire.modules.session.internal.filter;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.modules.session.junit.PerTestClassLoaderRunner;
import com.gemstone.gemfire.modules.session.filter.SessionCachingFilter;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.apache.jasper.servlet.JspServlet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.http.HttpTester;

import static org.junit.Assert.*;

/**
 * In-container testing using Jetty. This allows us to test context listener
 * events as well as dispatching actions.
 */
@Category(IntegrationTest.class)
@RunWith(PerTestClassLoaderRunner.class)
public class SessionReplicationIntegrationJUnitTest {

  private MyServletTester tester;

  private HttpTester.Request request;

  private HttpTester.Response response;

  private ServletHolder servletHolder;

  private FilterHolder filterHolder;

  private static final File tmpdir;

  private static final String gemfire_log;

  static {
    // Create a per-user scratch directory
    tmpdir = new File(System.getProperty("java.io.tmpdir"),
        "gemfire_modules-" + System.getProperty("user.name"));
    tmpdir.mkdirs();
    tmpdir.deleteOnExit();

    gemfire_log = tmpdir.getPath() +
        System.getProperty("file.separator") + "gemfire_modules.log";
  }

  @Before
  public void setUp() throws Exception {
    request = HttpTester.newRequest();

    tester = new MyServletTester();
    tester.setContextPath("/test");

    filterHolder = tester.addFilter(SessionCachingFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST));
    filterHolder.setInitParameter("gemfire.property.mcast-port", "0");
    filterHolder.setInitParameter("gemfire.property.log-file", gemfire_log);
    filterHolder.setInitParameter("cache-type", "peer-to-peer");

    servletHolder = tester.addServlet(BasicServlet.class, "/hello");
    servletHolder.setInitParameter("test.callback", "callback_1");

    /**
     * This starts the servlet. Our wrapped servlets *must* start
     * immediately otherwise the ServletContext is not captured correctly.
     */
    servletHolder.setInitOrder(0);
  }

  @After
  public void tearDown() throws Exception {
//    if (tester.isStarted()) {
//      ContextManager.getInstance().removeContext(
//          servletHolder.getServlet().getServletConfig().getServletContext());
//    }
    tester.stop();
  }

  @Test
  public void testSanity() throws Exception {
    Callback c = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        PrintWriter out = response.getWriter();
        out.write("Hello World");
      }
    };

    tester.setAttribute("callback_1", c);
    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals(200, response.getStatus());
    assertEquals("Hello World", response.getContent());
  }

  @Test
  public void testSessionGenerated() throws Exception {
    Callback c = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        PrintWriter out = response.getWriter();
        out.write(request.getSession().getId());
      }
    };

    tester.setAttribute("callback_1", c);
    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertTrue("Not a correctly generated session id",
        response.getContent().endsWith("-GF"));

    List<Cookie> cookies = getCookies(response);
    assertEquals("Session id != JSESSIONID from cookie",
        response.getContent(), cookies.get(0).getValue());

    Region r = getRegion();
    assertNotNull("Session not found in region",
        r.get(cookies.get(0).getValue()));
  }


  /**
   * Test that getSession(false) does not create a new session
   */
  @Test
  public void testSessionNotGenerated() throws Exception {
    Callback c = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        String output = "OK";
        HttpSession s = request.getSession(false);
        if (s != null) {
          output = s.getId();
        }
        PrintWriter out = response.getWriter();
        out.write(output);
      }
    };

    tester.setAttribute("callback_1", c);
    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("Session should not have been created", "OK",
        response.getContent());
  }


  @Test
  public void testUnknownAttributeIsNull() throws Exception {
    Callback c = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        Object o = request.getSession().getAttribute("unknown");
        PrintWriter out = response.getWriter();
        if (o == null) {
          out.write("null");
        } else {
          out.write(o.toString());
        }
      }
    };

    tester.setAttribute("callback_1", c);
    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("Unknown attribute should be null", "null",
        response.getContent());
  }


  @Test
  public void testSessionRemains1() throws Exception {
    Callback c = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        String output = "null";
        HttpSession session = request.getSession();
        if (session.isNew()) {
          output = "new";
          session.setAttribute("foo", output);
        } else {
          output = (String) session.getAttribute("foo");
          if (output != null) {
            output = "old";
          }
        }
        PrintWriter out = response.getWriter();
        out.write(output);
      }
    };

    tester.setAttribute("callback_1", c);
    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("Session should be new", "new", response.getContent());

    List<Cookie> cookies = getCookies(response);
    request.setHeader("Cookie", "JSESSIONID=" + cookies.get(0).getValue());

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("Session should be old", "old", response.getContent());

    List<Cookie> cookies2 = getCookies(response);
    assertEquals("Session IDs should be the same", cookies.get(0).getValue(),
        cookies2.get(0).getValue());

    Region r = getRegion();
    assertNotNull("Session object should exist in region",
        r.get(cookies.get(0).getValue()));
  }

  /**
   * Test that attributes are updated on the backend
   */
  @Test
  public void testAttributesUpdatedInRegion() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        request.getSession().setAttribute("foo", "bar");
      }
    };

    // This is the callback used to invalidate the session
    Callback c_2 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        request.getSession().setAttribute("foo", "baz");
      }
    };

    tester.setAttribute("callback_1", c_1);
    tester.setAttribute("callback_2", c_2);

    servletHolder.setInitParameter("test.callback", "callback_1");

    ServletHolder sh2 = tester.addServlet(BasicServlet.class, "/request2");
    sh2.setInitParameter("test.callback", "callback_2");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    List<Cookie> cookies = getCookies(response);

    Region r = getRegion();
    assertEquals("bar",
        ((HttpSession) r.get(cookies.get(0).getValue())).getAttribute("foo"));

    request.setHeader("Cookie", "JSESSIONID=" + cookies.get(0).getValue());
    request.setURI("/test/request2");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("baz",
        ((HttpSession) r.get(cookies.get(0).getValue())).getAttribute(
            "foo"));
  }

  /**
   * Test setting an attribute to null deletes it
   */
  @Test
  public void testSetAttributeNullDeletesIt() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        request.getSession().setAttribute("foo", "bar");
      }
    };

    // This is the callback used to invalidate the session
    Callback c_2 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        request.getSession().setAttribute("foo", null);
      }
    };

    tester.setAttribute("callback_1", c_1);
    tester.setAttribute("callback_2", c_2);

    servletHolder.setInitParameter("test.callback", "callback_1");

    ServletHolder sh2 = tester.addServlet(BasicServlet.class, "/request2");
    sh2.setInitParameter("test.callback", "callback_2");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    List<Cookie> cookies = getCookies(response);

    Region r = getRegion();
    assertEquals("bar",
        ((HttpSession) r.get(cookies.get(0).getValue())).getAttribute(
            "foo"));

    request.setHeader("Cookie", "JSESSIONID=" + cookies.get(0).getValue());
    request.setURI("/test/request2");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertNull(
        ((HttpSession) r.get(cookies.get(0).getValue())).getAttribute(
            "foo"));
  }

// Don't see how to do this currently as the SessionListener needs a full
// web context to work in.

//    /**
//     * Test that sessions expire correctly
//     */
//    public void testSessionExpiration() throws Exception {
//        Callback c_1 = new Callback() {
//            @Override
//            public void call(HttpServletRequest request, HttpServletResponse response)
//                    throws IOException, ServletException {
//                HttpSession s = request.getSession();
//                s.setAttribute("foo", "bar");
//                s.setMaxInactiveInterval(1);
//
//                PrintWriter out = response.getWriter();
//                out.write(s.getId());
//            }
//        };
//
//        // This is the callback used to check if the session is still there
//        Callback c_2 = new Callback() {
//            @Override
//            public void call(HttpServletRequest request, HttpServletResponse response)
//                    throws IOException, ServletException {
//                HttpSession s = request.getSession(false);
//                String output;
//                if (s == null) {
//                    output = "null";
//                } else {
//                    output = s.getId();
//                }
//
//                PrintWriter out = response.getWriter();
//                out.write(output);
//            }
//        };
//
//        tester.addEventListener(new SessionListener());
//        tester.setAttribute("callback_1", c_1);
//        tester.setAttribute("callback_2", c_2);
//
//        servletHolder.setInitParameter("test.callback", "callback_1");
//
//        ServletHolder sh2 = tester.addServlet(BasicServlet.class, "/request2");
//        sh2.setInitParameter("test.callback", "callback_2");
//
//        tester.start();
//        ContextManager.getInstance().putContext(
//                servletHolder.getServlet().getServletConfig().getServletContext());
//
//        request.setMethod("GET");
//        request.setURI("/test/hello");
//        request.setHeader("Host", "tester");
//        request.setVersion("HTTP/1.0");
//        response.parse(tester.getResponses(request.generate()));
//
//        String id = response.getContent();
//
//        // Wait for the session to expire
//        Thread.sleep(2000);
//
//        request.setHeader("Cookie", "JSESSIONID=" + id);
//        request.setURI("/test/request2");
//        response.parse(tester.getResponses(request.generate()));
//
//        assertEquals("null", response.getContent());
//
//        Region r = getRegion();
//        assertNull("Region should not contain session", r.get(id));
//    }

  /**
   * Test that invalidating a session destroys it as well as the backend
   * object.
   */
  @Test
  public void testInvalidateSession1() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        request.getSession().setAttribute("foo", "bar");
      }
    };

    // This is the callback used to invalidate the session
    Callback c_2 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        request.getSession(false).invalidate();
      }
    };

    tester.setAttribute("callback_1", c_1);
    tester.setAttribute("callback_2", c_2);

    servletHolder.setInitParameter("test.callback", "callback_1");

    ServletHolder sh2 = tester.addServlet(BasicServlet.class, "/request2");
    sh2.setInitParameter("test.callback", "callback_2");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    List<Cookie> cookies = getCookies(response);
    Region r = getRegion();
    assertEquals("bar",
        ((HttpSession) r.get(cookies.get(0).getValue())).getAttribute("foo"));

    request.setHeader("Cookie", "JSESSIONID=" + cookies.get(0).getValue());
    request.setURI("/test/request2");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertNull("Region should not contain session",
        r.get(cookies.get(0).getValue()));
  }

  /**
   * Test that invalidating a session throws an exception on subsequent access.
   */
  @Test
  public void testInvalidateSession2() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        s.invalidate();
        PrintWriter out = response.getWriter();
        try {
          s.getAttribute("foo");
        } catch (IllegalStateException iex) {
          out.write("OK");
        }
      }
    };

    tester.setAttribute("callback_1", c_1);

    servletHolder.setInitParameter("test.callback", "callback_1");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("OK", response.getContent());
  }

  /**
   * Test that invalidating a session throws an exception on subsequent access.
   */
  @Test
  public void testInvalidateSession3() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        s.invalidate();
        PrintWriter out = response.getWriter();
        try {
          s.getAttributeNames();
        } catch (IllegalStateException iex) {
          out.write("OK");
        }
      }
    };

    tester.setAttribute("callback_1", c_1);

    servletHolder.setInitParameter("test.callback", "callback_1");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("OK", response.getContent());
  }

  /**
   * Test that invalidating a session throws an exception on subsequent access.
   */
  @Test
  public void testInvalidateSession4() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        s.invalidate();
        PrintWriter out = response.getWriter();
        try {
          s.getCreationTime();
        } catch (IllegalStateException iex) {
          out.write("OK");
        }
      }
    };

    tester.setAttribute("callback_1", c_1);

    servletHolder.setInitParameter("test.callback", "callback_1");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("OK", response.getContent());
  }

  /**
   * Test that invalidating a session does not throw an exception for subsequent
   * getId calls.
   */
  @Test
  public void testInvalidateSession5() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        s.invalidate();
        s.getId();
        PrintWriter out = response.getWriter();
        out.write("OK");
      }
    };

    tester.setAttribute("callback_1", c_1);

    servletHolder.setInitParameter("test.callback", "callback_1");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("OK", response.getContent());
  }

  /**
   * Test that invalidating a session throws an exception on subsequent access.
   */
  @Test
  public void testInvalidateSession6() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        s.invalidate();
        PrintWriter out = response.getWriter();
        try {
          s.getLastAccessedTime();
        } catch (IllegalStateException iex) {
          out.write("OK");
        }
      }
    };

    tester.setAttribute("callback_1", c_1);

    servletHolder.setInitParameter("test.callback", "callback_1");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("OK", response.getContent());
  }

  /**
   * Test that invalidating a session does not throw an exception for
   * subsequent getMaxInactiveInterval calls.
   */

// I've commented this out for now as Jetty seems to want to throw an
// Exception here where the HttpServlet api doesn't specify that.
  @Test
  public void testInvalidateSession7() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request,
          HttpServletResponse response) throws IOException {
        HttpSession s = request.getSession();
        s.invalidate();
        s.getMaxInactiveInterval();
        PrintWriter out = response.getWriter();
        out.write("OK");
      }
    };

    tester.setAttribute("callback_1", c_1);

    servletHolder.setInitParameter("test.callback", "callback_1");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("OK", response.getContent());
  }

  /**
   * Test that invalidating a session does not throw an exception for subsequent
   * getServletContext calls.
   */
  @Test
  public void testInvalidateSession8() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        s.invalidate();
        s.getServletContext();
        PrintWriter out = response.getWriter();
        out.write("OK");
      }
    };

    tester.setAttribute("callback_1", c_1);

    servletHolder.setInitParameter("test.callback", "callback_1");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("OK", response.getContent());
  }

  /**
   * Test that invalidating a session throws an exception on subsequent access.
   */
  @Test
  public void testInvalidateSession9() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        s.invalidate();
        PrintWriter out = response.getWriter();
        try {
          s.isNew();
        } catch (IllegalStateException iex) {
          out.write("OK");
        }
      }
    };

    tester.setAttribute("callback_1", c_1);

    servletHolder.setInitParameter("test.callback", "callback_1");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("OK", response.getContent());
  }

  /**
   * Test that invalidating a session throws an exception on subsequent access.
   */
  @Test
  public void testInvalidateSession10() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        s.invalidate();
        PrintWriter out = response.getWriter();
        try {
          s.removeAttribute("foo");
        } catch (IllegalStateException iex) {
          out.write("OK");
        }
      }
    };

    tester.setAttribute("callback_1", c_1);

    servletHolder.setInitParameter("test.callback", "callback_1");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("OK", response.getContent());
  }

  /**
   * Test that invalidating a session throws an exception on subsequent access.
   */
  @Test
  public void testInvalidateSession11() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        s.invalidate();
        PrintWriter out = response.getWriter();
        try {
          s.setAttribute("foo", "bar");
        } catch (IllegalStateException iex) {
          out.write("OK");
        }
      }
    };

    tester.setAttribute("callback_1", c_1);

    servletHolder.setInitParameter("test.callback", "callback_1");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("OK", response.getContent());
  }

  /**
   * Test that invalidating a session does not throw an exception for subsequent
   * setMaxInactiveInterval calls.
   */
  @Test
  public void testInvalidateSession12() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        s.invalidate();
        s.setMaxInactiveInterval(1);
        PrintWriter out = response.getWriter();
        out.write("OK");
      }
    };

    tester.setAttribute("callback_1", c_1);

    servletHolder.setInitParameter("test.callback", "callback_1");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("OK", response.getContent());
  }

  /**
   * Test that invalidating a session results in null being returned on
   * subsequent getSession(false) calls.
   */
  @Test
  public void testInvalidateSession13() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        s.invalidate();
        s = request.getSession(false);
        PrintWriter out = response.getWriter();
        if (s == null) {
          out.write("OK");
        } else {
          out.write(s.toString());
        }
      }
    };

    tester.setAttribute("callback_1", c_1);

    servletHolder.setInitParameter("test.callback", "callback_1");

    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("OK", response.getContent());
  }


  /**
   * Test that we can invalidate and then recreate a new session
   */
  @Test
  public void testInvalidateAndRecreateSession() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {

        PrintWriter out = response.getWriter();
        out.write(request.getSession().getId());
      }
    };

    Callback c_2 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        s.invalidate();

        PrintWriter out = response.getWriter();
        out.write(request.getSession().getId());
      }
    };

    tester.setAttribute("callback_1", c_1);
    tester.setAttribute("callback_2", c_2);

    ServletHolder sh = tester.addServlet(BasicServlet.class, "/dispatch");
    sh.setInitParameter("test.callback", "callback_2");

    tester.start();
//    ContextManager.getInstance().putContext(
//        sh.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));
    String session1 = response.getContent();

    request.setHeader("Cookie", "JSESSIONID=" + session1);
    request.setURI("/test/request2");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    String session12 = response.getContent();
    assertFalse("First and subsequent session ids must not be the same",
        session1.equals(session12));
  }


  /**
   * Test that creation time does not change on subsequent access
   */
  @Test
  public void testGetCreationTime() throws Exception {
    Callback c = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession session = request.getSession();
        PrintWriter out = response.getWriter();
        out.write(Long.toString(session.getCreationTime()));
      }
    };

    tester.setAttribute("callback_1", c);
    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    long time1 = Long.parseLong(response.getContent());
    assertTrue("Creation time should be positive", time1 > 0);

    List<Cookie> cookies = getCookies(response);
    request.setHeader("Cookie", "JSESSIONID=" + cookies.get(0).getValue());

    try {
      Thread.sleep(1000);
    } catch (Exception ex) {
    }

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));
    long time2 = Long.parseLong(response.getContent());
    assertTrue("Creation time should be the same across requests",
        time1 == time2);
  }

  /**
   * Test that the last accessed time is updated on subsequent access
   */
  @Test
  public void testGetLastAccessedTime() throws Exception {
    Callback c = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession session = request.getSession();
        PrintWriter out = response.getWriter();
        out.write(Long.toString(session.getLastAccessedTime()));
      }
    };

    tester.setAttribute("callback_1", c);
    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    long time1 = Long.parseLong(response.getContent());
//        assertTrue("Last accessed time should be positive", time1 > 0);

    List<Cookie> cookies = getCookies(response);
    request.setHeader("Cookie", "JSESSIONID=" + cookies.get(0).getValue());

    Thread.sleep(1000);

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));
    long time2 = Long.parseLong(response.getContent());
    assertTrue("Last accessed time should be increasing across requests",
        time2 > time1);
  }

  /**
   * Test that the underlying native session remains the same across requests
   */
  @Test
  public void testNativeSessionRemainsUnchanged() throws Exception {
    Callback c = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        GemfireHttpSession session = (GemfireHttpSession) request.getSession();
        PrintWriter out = response.getWriter();
        out.write(session.getNativeSession().getId());
      }
    };

    tester.setAttribute("callback_1", c);
    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));
    String nativeSessionId = response.getContent();

    List<Cookie> cookies = getCookies(response);
    String sessionId = cookies.get(0).getValue();
    Region r = getRegion();

    assertEquals(
        "Cached native session id does not match servlet returned native session id",
        nativeSessionId,
        ((GemfireHttpSession) r.get(sessionId)).getNativeSession().getId());

    request.setHeader("Cookie", "JSESSIONID=" + cookies.get(0).getValue());
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals(
        "Underlying native sessions must remain the same across requests",
        nativeSessionId,
        ((GemfireHttpSession) r.get(sessionId)).getNativeSession().getId());
  }

  /**
   * Test session id embedded in the URL
   */
  @Test
  public void testSessionIdEmbeddedInUrl() throws Exception {
    Callback c = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        GemfireHttpSession session = (GemfireHttpSession) request.getSession();
        PrintWriter out = response.getWriter();
        out.write(session.getId());
      }
    };

    tester.setAttribute("callback_1", c);
    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));
    List<Cookie> cookies = getCookies(response);
    String sessionId = response.getContent();
    assertEquals("Session ids should be the same", sessionId,
        cookies.get(0).getValue());

    request.setURI("/test/hello;jsessionid=" + sessionId);
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));
    cookies = getCookies(response);

    assertEquals("Session ids should be the same", sessionId,
        cookies.get(0).getValue());
  }


  /**
   * Test that request forward dispatching works
   */
  @Test
  public void testDispatchingForward1() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
        RequestDispatcher dispatcher = request.getRequestDispatcher("dispatch");
        dispatcher.forward(request, response);

        // This should not appear in the output
        PrintWriter out = response.getWriter();
        out.write("bang");
      }
    };

    // This is the callback used by the forward servlet
    Callback c_2 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
        PrintWriter out = response.getWriter();
        out.write("dispatched");
      }
    };

    tester.setAttribute("callback_1", c_1);
    tester.setAttribute("callback_2", c_2);

    ServletHolder sh = tester.addServlet(BasicServlet.class, "/dispatch");
    sh.setInitParameter("test.callback", "callback_2");

    tester.start();
//    ContextManager.getInstance().putContext(
//        sh.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));
    assertEquals("dispatched", response.getContent());

//    ContextManager.getInstance().removeContext(
//        sh.getServlet().getServletConfig().getServletContext());
  }


  /**
   * Test that request include dispatching works
   */
  @Test
  public void testDispatchingInclude() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
        RequestDispatcher dispatcher = request.getRequestDispatcher("dispatch");
        dispatcher.include(request, response);

        // This *should* appear in the output
        PrintWriter out = response.getWriter();
        out.write("_bang");
      }
    };

    // This is the callback used by the include servlet
    Callback c_2 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
        PrintWriter out = response.getWriter();
        out.write("dispatched");
      }
    };

    tester.setAttribute("callback_1", c_1);
    tester.setAttribute("callback_2", c_2);

    ServletHolder sh = tester.addServlet(BasicServlet.class, "/dispatch");
    sh.setInitParameter("test.callback", "callback_2");

    tester.start();
//    ContextManager.getInstance().putContext(
//        sh.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));
    assertEquals("dispatched_bang", response.getContent());

//    ContextManager.getInstance().removeContext(
//        sh.getServlet().getServletConfig().getServletContext());
  }


  /**
   * Test to try and simulate a failover scenario
   */
  @Test
  public void testFailover1() throws Exception {
    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
        HttpSession s = request.getSession();
        s.setAttribute("foo", "bar");

        PrintWriter out = response.getWriter();
        out.write(request.getSession().getId());
      }
    };

    Callback c_2 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
        HttpSession s = request.getSession();

        PrintWriter out = response.getWriter();
        out.write((String) s.getAttribute("foo"));
      }
    };

    tester.setAttribute("callback_1", c_1);
    tester.setAttribute("callback_2", c_2);

    ServletHolder sh = tester.addServlet(BasicServlet.class, "/request2");
    sh.setInitParameter("test.callback", "callback_2");

    tester.start();
//    ContextManager.getInstance().putContext(
//        sh.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));
    String id = response.getContent();

    // Now we simulate the failover by removing the native session from
    // the stored session
    Region r = getRegion();
    GemfireHttpSession sessObj = (GemfireHttpSession) r.get(id);
    sessObj.setNativeSession(null);

    r.put(id, sessObj);

    request.setHeader("Cookie", "JSESSIONID=" + id);
    request.setURI("/test/request2");
    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals("bar", response.getContent());
  }

  @Test
  public void testHttpSessionListener1() throws Exception {
    HttpSessionListenerImpl listener = new HttpSessionListenerImpl();
    tester.getContext().getSessionHandler().addEventListener(listener);

    Callback c = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        // This is set in HttpSessionListenerImpl
        String result = (String) s.getAttribute("gemfire-session-id");
        response.getWriter().write(result);
      }
    };

    tester.setAttribute("callback_1", c);
    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals(200, response.getStatus());

    List<Cookie> cookies = getCookies(response);

//        AbstractListener listener = RendezvousManager.getListener();
    tester.stop();

    assertTrue("Timeout waiting for events",
        listener.await(1, TimeUnit.SECONDS));
    assertEquals(ListenerEventType.SESSION_CREATED,
        listener.events.get(0));
    assertEquals(ListenerEventType.SESSION_DESTROYED,
        listener.events.get(1));
    assertEquals(cookies.get(0).getValue(), response.getContent());
  }

  @Test
  public void testHttpSessionListener2() throws Exception {
    HttpSessionListenerImpl2 listener = new HttpSessionListenerImpl2();
    tester.getContext().getSessionHandler().addEventListener(listener);

    Callback c = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession s = request.getSession();
        s.setAttribute("test01", "test01");
        s = request.getSession(false);
        s.invalidate();
        response.getWriter().write(s.getId());
      }
    };

    tester.setAttribute("callback_1", c);
    tester.start();
//    ContextManager.getInstance().putContext(
//        servletHolder.getServlet().getServletConfig().getServletContext());

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals(200, response.getStatus());

    List<Cookie> cookies = getCookies(response);

    tester.stop();

    assertTrue("Timeout waiting for events",
        listener.await(1, TimeUnit.SECONDS));
    assertEquals(ListenerEventType.SESSION_CREATED,
        listener.events.get(0));
    assertEquals(ListenerEventType.SESSION_DESTROYED,
        listener.events.get(1));
    assertEquals(cookies.get(0).getValue(), response.getContent());
  }




//  @Test
  public void testJsp() throws Exception {
    tester.setResourceBase("target/test-classes");
    ServletHolder jspHolder = tester.addServlet(JspServlet.class, "/test/*");
    jspHolder.setInitOrder(1);

    jspHolder.setInitParameter("scratchdir", tmpdir.getPath());

    Callback c_1 = new Callback() {
      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
        request.getSession().setAttribute("foo", "bar");
        request.setAttribute("foo", "baz");
        RequestDispatcher dispatcher = request.getRequestDispatcher(
            "pagecontext.jsp");
        dispatcher.forward(request, response);
      }
    };

    tester.getContext().setClassLoader(Thread.currentThread().getContextClassLoader());
    tester.setAttribute("callback_1", c_1);

    tester.start();

    request.setMethod("GET");
    request.setURI("/test/hello");
    request.setHeader("Host", "tester");
    request.setVersion("HTTP/1.0");

    response = HttpTester.parseResponse(tester.getResponses(request.generate()));

    assertEquals(200, response.getStatus());
    assertEquals("baz", response.getContent().trim());
  }


  ////////////////////////////////////////////////////////////////////
  // Private methods

  /**
   * Why doesn't HttpTester do this already??
   */
  private List<Cookie> getCookies(HttpTester.Response response) {
    List<Cookie> cookies = new ArrayList<Cookie>();

    Enumeration e = response.getValues("Set-Cookie");

    while (e != null && e.hasMoreElements()) {
      String header = (String) e.nextElement();
      Cookie c = null;

      StringTokenizer st = new StringTokenizer(header, ";");
      while (st.hasMoreTokens()) {
        String[] split = st.nextToken().split("=");
        String param = split[0].trim();
        String value = null;
        if (split.length > 1) {
          value = split[1].trim();
        }
        if ("version".equalsIgnoreCase(param)) {
          c.setVersion(Integer.parseInt(value));
        } else if ("comment".equalsIgnoreCase(param)) {
          c.setComment(value);
        } else if ("domain".equalsIgnoreCase(param)) {
          c.setDomain(value);
        } else if ("max-age".equalsIgnoreCase(param)) {
          c.setMaxAge(Integer.parseInt(value));
        } else if ("discard".equalsIgnoreCase(param)) {
          c.setMaxAge(-1);
        } else if ("path".equalsIgnoreCase(param)) {
          c.setPath(value);
        } else if ("secure".equalsIgnoreCase(param)) {
          c.setSecure(true);
        } else if ("httponly".equalsIgnoreCase(param)) {
          // Ignored??
        } else {
          if (c == null) {
            c = new Cookie(param, value);
          } else {
            throw new IllegalStateException("Unknown cookie param: " + param);
          }
        }
      }

      if (c != null) {
        cookies.add(c);
      }
    }

    return cookies;
  }

  private Region getRegion() {
    // Yuck...
    return ((GemfireSessionManager) ((SessionCachingFilter) filterHolder.getFilter()).getSessionManager()).getCache().getCache().getRegion(
        "gemfire_modules_sessions");
  }
}
