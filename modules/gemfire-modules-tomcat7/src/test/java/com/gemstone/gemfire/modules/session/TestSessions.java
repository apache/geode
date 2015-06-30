/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.modules.session;

import junit.framework.TestCase;

import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.GetMethodWebRequest;
import com.meterware.httpunit.WebConversation;
import com.meterware.httpunit.WebResponse;

import java.beans.PropertyChangeEvent;

import static junit.framework.Assert.*;

/**
 *
 */
public class TestSessions extends TestCase {

  /**
   * Reset some data
   */
  public void setup() throws Exception {
    AllTests.sessionManager.setMaxInactiveInterval(30);
    AllTests.region.clear();
  }

  /**
   * Check that the basics are working
   */
  public void testSanity() throws Exception {
    WebConversation wc = new WebConversation();
    WebResponse response = wc.getResponse("http://localhost:7890/test");

    assertEquals("JSESSIONID", response.getNewCookieNames()[0]);
  }

  /**
   * Test callback functionality. This is here really just as an example.
   * Callbacks are useful to implement per test actions which can be defined
   * within the actual test method instead of in a separate servlet class.
   */
  public void testCallback() throws Exception {
    final String helloWorld = "Hello World";
    Callback c = new Callback() {

      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        PrintWriter out = response.getWriter();
        out.write(helloWorld);
      }
    };
    AllTests.servlet.getServletContext().setAttribute("callback", c);

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest("http://localhost:7890/test");

    req.setParameter("cmd", QueryCommand.CALLBACK.name());
    req.setParameter("param", "callback");
    WebResponse response = wc.getResponse(req);

    assertEquals(helloWorld, response.getText());
  }

  /**
   * Test that calling session.isNew() works for the initial as well as
   * subsequent requests.
   */
  public void testIsNew() throws Exception {
    Callback c = new Callback() {

      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession session = request.getSession();
        response.getWriter().write(Boolean.toString(session.isNew()));
      }
    };
    AllTests.servlet.getServletContext().setAttribute("callback", c);

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest("http://localhost:7890/test");

    req.setParameter("cmd", QueryCommand.CALLBACK.name());
    req.setParameter("param", "callback");
    WebResponse response = wc.getResponse(req);

    assertEquals("true", response.getText());
    response = wc.getResponse(req);

    assertEquals("false", response.getText());
  }

  /**
   * Check that our session persists. The values we pass in as query params are
   * used to set attributes on the session.
   */
  public void testSessionPersists1() throws Exception {
    String key = "value_testSessionPersists1";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest("http://localhost:7890/test");
    req.setParameter("cmd", QueryCommand.SET.name());
    req.setParameter("param", key);
    req.setParameter("value", value);
    WebResponse response = wc.getResponse(req);
    String sessionId = response.getNewCookieValue("JSESSIONID");

    assertNotNull("No apparent session cookie", sessionId);

    // The request retains the cookie from the prior response...
    req.setParameter("cmd", QueryCommand.GET.name());
    req.setParameter("param", key);
    req.removeParameter("value");
    response = wc.getResponse(req);

    assertEquals(value, response.getText());
  }

  /**
   * Check that our session persists beyond the container restarting.
   */
//    public void testSessionPersists2() throws Exception {
//        String key = "value_testSessionPersists2";
//        String value = "Foo";
//
//        WebConversation wc = new WebConversation();
//        WebRequest req = new GetMethodWebRequest("http://localhost:7890/test");
//        req.setParameter("cmd", QueryCommand.SET.name());
//        req.setParameter("param", key);
//        req.setParameter("value", value);
//        WebResponse response = wc.getResponse(req);
//        String sessionId = response.getNewCookieValue("JSESSIONID");
//
//        assertNotNull("No apparent session cookie", sessionId);
//
//        // Restart the container
//        AllTests.teardownClass();
//        AllTests.setupClass();
//
//        // The request retains the cookie from the prior response...
//        req.setParameter("cmd", QueryCommand.GET.name());
//        req.setParameter("param", key);
//        req.removeParameter("value");
//        response = wc.getResponse(req);
//
//        assertEquals(value, response.getText());
//    }

  /**
   * Test that invalidating a session makes it's attributes inaccessible.
   */
  public void testInvalidate() throws Exception {
    String key = "value_testInvalidate";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest("http://localhost:7890/test");

    // Set an attribute
    req.setParameter("cmd", QueryCommand.SET.name());
    req.setParameter("param", key);
    req.setParameter("value", value);
    WebResponse response = wc.getResponse(req);

    // Invalidate the session
    req.removeParameter("param");
    req.removeParameter("value");
    req.setParameter("cmd", QueryCommand.INVALIDATE.name());
    wc.getResponse(req);

    // The attribute should not be accessible now...
    req.setParameter("cmd", QueryCommand.GET.name());
    req.setParameter("param", key);
    response = wc.getResponse(req);

    assertEquals("", response.getText());
  }

  /**
   * Test setting the session expiration
   */
  public void testSessionExpiration1() throws Exception {
    // TestSessions only live for a second
    AllTests.sessionManager.setMaxInactiveInterval(1);

    String key = "value_testSessionExpiration1";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest("http://localhost:7890/test");

    // Set an attribute
    req.setParameter("cmd", QueryCommand.SET.name());
    req.setParameter("param", key);
    req.setParameter("value", value);
    WebResponse response = wc.getResponse(req);

    // Sleep a while
    Thread.sleep(2000);

    // The attribute should not be accessible now...
    req.setParameter("cmd", QueryCommand.GET.name());
    req.setParameter("param", key);
    response = wc.getResponse(req);

    assertEquals("", response.getText());
  }

  /**
   * Test setting the session expiration via a property change as would happen
   * under normal deployment conditions.
   */
  public void testSessionExpiration2() throws Exception {
    // TestSessions only live for a minute
    AllTests.sessionManager.propertyChange(
        new PropertyChangeEvent(AllTests.server.getRootContext(),
            "sessionTimeout",
            new Integer(30), new Integer(1)));

    // Check that the value has been set to 60 seconds
    assertEquals(60, AllTests.sessionManager.getMaxInactiveInterval());
  }

  /**
   * Test that removing a session attribute also removes it from the region
   */
  public void testRemoveAttribute() throws Exception {
    String key = "value_testRemoveAttribute";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest("http://localhost:7890/test");

    // Set an attribute
    req.setParameter("cmd", QueryCommand.SET.name());
    req.setParameter("param", key);
    req.setParameter("value", value);
    WebResponse response = wc.getResponse(req);
    String sessionId = response.getNewCookieValue("JSESSIONID");

    // Implicitly remove the attribute
    req.removeParameter("value");
    wc.getResponse(req);

    // The attribute should not be accessible now...
    req.setParameter("cmd", QueryCommand.GET.name());
    req.setParameter("param", key);
    response = wc.getResponse(req);

    assertEquals("", response.getText());
    assertNull(AllTests.region.get(sessionId).getAttribute(key));
  }

  /**
   * Test that a session attribute gets set into the region too.
   */
  public void testBasicRegion() throws Exception {
    String key = "value_testBasicRegion";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest("http://localhost:7890/test");

    // Set an attribute
    req.setParameter("cmd", QueryCommand.SET.name());
    req.setParameter("param", key);
    req.setParameter("value", value);
    WebResponse response = wc.getResponse(req);
    String sessionId = response.getNewCookieValue("JSESSIONID");

    assertEquals(value, AllTests.region.get(sessionId).getAttribute(key));
  }

  /**
   * Test that a session attribute gets removed from the region when the session
   * is invalidated.
   */
  public void testRegionInvalidate() throws Exception {
    String key = "value_testRegionInvalidate";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest("http://localhost:7890/test");

    // Set an attribute
    req.setParameter("cmd", QueryCommand.SET.name());
    req.setParameter("param", key);
    req.setParameter("value", value);
    WebResponse response = wc.getResponse(req);
    String sessionId = response.getNewCookieValue("JSESSIONID");

    // Invalidate the session
    req.removeParameter("param");
    req.removeParameter("value");
    req.setParameter("cmd", QueryCommand.INVALIDATE.name());
    wc.getResponse(req);

    assertNull("The region should not have an entry for this session",
        AllTests.region.get(sessionId));
  }

  /**
   * Test that multiple attribute updates, within the same request result in
   * only the latest one being effective.
   */
  public void testMultipleAttributeUpdates() throws Exception {
    final String key = "value_testMultipleAttributeUpdates";
    Callback c = new Callback() {

      @Override
      public void call(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        HttpSession session = request.getSession();
        for (int i = 0; i < 1000; i++) {
          session.setAttribute(key, Integer.toString(i));
        }
      }
    };
    AllTests.servlet.getServletContext().setAttribute("callback", c);

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest("http://localhost:7890/test");

    // Execute the callback
    req.setParameter("cmd", QueryCommand.CALLBACK.name());
    req.setParameter("param", "callback");
    WebResponse response = wc.getResponse(req);

    String sessionId = response.getNewCookieValue("JSESSIONID");

    assertEquals("999", AllTests.region.get(sessionId).getAttribute(key));
  }
}
