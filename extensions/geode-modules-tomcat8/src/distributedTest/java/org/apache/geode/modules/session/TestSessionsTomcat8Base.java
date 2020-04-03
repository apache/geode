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
package org.apache.geode.modules.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.beans.PropertyChangeEvent;
import java.io.PrintWriter;
import java.io.Serializable;

import javax.servlet.http.HttpSession;

import com.meterware.httpunit.GetMethodWebRequest;
import com.meterware.httpunit.WebConversation;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;
import org.apache.catalina.core.StandardWrapper;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.modules.session.catalina.DeltaSessionManager;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

public abstract class TestSessionsTomcat8Base implements Serializable {

  @ClassRule
  public static DistributedRule distributedTestRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();
  protected Logger logger = LogService.getLogger();

  int port;
  EmbeddedTomcat8 server;
  StandardWrapper servlet;
  Region<String, HttpSession> region;
  DeltaSessionManager sessionManager;

  public void basicConnectivityCheck() throws Exception {
    WebConversation wc = new WebConversation();
    assertThat(wc).describedAs("WebConversation was").isNotNull();
    logger.debug("Sending request to http://localhost:{}/test", port);
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));
    assertThat(req).describedAs("WebRequest was").isNotNull();
    req.setParameter("cmd", QueryCommand.GET.name());
    req.setParameter("param", "null");
    WebResponse response = wc.getResponse(req);
    assertThat(response).describedAs("WebResponse was").isNotNull();
    assertThat(response.getNewCookieNames()[0]).describedAs("SessionID was")
        .isEqualTo("JSESSIONID");
  }

  /**
   * Test callback functionality. This is here really just as an example. Callbacks are useful to
   * implement per test actions which can be defined within the actual test method instead of in a
   * separate servlet class.
   */
  @Test
  public void testCallback() throws Exception {
    final String helloWorld = "Hello World";
    Callback c = (request, response) -> {
      PrintWriter out = response.getWriter();
      out.write(helloWorld);
    };
    servlet.getServletContext().setAttribute("callback", c);

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));
    req.setParameter("cmd", QueryCommand.CALLBACK.name());
    req.setParameter("param", "callback");

    WebResponse response = wc.getResponse(req);
    assertThat(response.getText()).isEqualTo(helloWorld);
  }

  /**
   * Test that calling session.isNew() works for the initial as well as subsequent requests.
   */
  @Test
  public void testIsNew() throws Exception {
    Callback c = (request, response) -> {
      HttpSession session = request.getSession();
      response.getWriter().write(Boolean.toString(session.isNew()));
    };
    servlet.getServletContext().setAttribute("callback", c);

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));

    req.setParameter("cmd", QueryCommand.CALLBACK.name());
    req.setParameter("param", "callback");

    WebResponse response = wc.getResponse(req);
    assertThat(response.getText()).isEqualTo("true");
    response = wc.getResponse(req);
    assertThat(response.getText()).isEqualTo("false");
  }

  /**
   * Check that our session persists. The values we pass in as query params are used to set
   * attributes on the session.
   */
  @Test
  public void testSessionPersists1() throws Exception {
    String key = "value_testSessionPersists1";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));
    req.setParameter("cmd", QueryCommand.SET.name());
    req.setParameter("param", key);
    req.setParameter("value", value);
    WebResponse response = wc.getResponse(req);

    String sessionId = response.getNewCookieValue("JSESSIONID");
    assertThat(sessionId).as("No apparent session cookie").isNotNull();

    // The request retains the cookie from the prior response...
    req.setParameter("cmd", QueryCommand.GET.name());
    req.setParameter("param", key);
    req.removeParameter("value");

    response = wc.getResponse(req);
    assertThat(response.getText()).isEqualTo(value);
  }

  /**
   * Test that invalidating a session makes it's attributes inaccessible.
   */
  @Test
  public void testInvalidate() throws Exception {
    String key = "value_testInvalidate";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));

    // Set an attribute
    req.setParameter("cmd", QueryCommand.SET.name());
    req.setParameter("param", key);
    req.setParameter("value", value);
    wc.getResponse(req);

    // Invalidate the session
    req.removeParameter("param");
    req.removeParameter("value");
    req.setParameter("cmd", QueryCommand.INVALIDATE.name());
    wc.getResponse(req);

    // The attribute should not be accessible now...
    req.setParameter("cmd", QueryCommand.GET.name());
    req.setParameter("param", key);

    WebResponse response = wc.getResponse(req);
    assertThat(response.getText()).isEmpty();
  }

  /**
   * Test setting the session expiration
   */
  @Test
  public void testSessionExpiration1() throws Exception {
    // TestSessions only live for a second
    sessionManager.setMaxInactiveInterval(1);

    String key = "value_testSessionExpiration1";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));

    // Set an attribute
    req.setParameter("cmd", QueryCommand.SET.name());
    req.setParameter("param", key);
    req.setParameter("value", value);
    wc.getResponse(req);

    // Sleep a while
    Thread.sleep(65000);

    // The attribute should not be accessible now...
    req.setParameter("cmd", QueryCommand.GET.name());
    req.setParameter("param", key);

    WebResponse response = wc.getResponse(req);
    assertThat(response.getText()).isEmpty();
  }

  /**
   * Test setting the session expiration via a property change as would happen under normal
   * deployment conditions.
   */
  @Test
  public void testSessionExpiration2() {
    // TestSessions only live for a minute
    sessionManager
        .propertyChange(new PropertyChangeEvent(server.getRootContext(), "sessionTimeout", 30, 1));

    // Check that the value has been set to 60 seconds
    assertThat(sessionManager.getMaxInactiveInterval()).isEqualTo(60);
  }

  /**
   * Test expiration of a session by the tomcat container, rather than gemfire expiration
   */
  @Test
  public void testSessionExpirationByContainer() throws Exception {
    String key = "value_testSessionExpiration1";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));

    // Set an attribute
    req.setParameter("cmd", QueryCommand.SET.name());
    req.setParameter("param", key);
    req.setParameter("value", value);
    wc.getResponse(req);

    // Set the session timeout of this one session.
    req.setParameter("cmd", QueryCommand.SET_MAX_INACTIVE.name());
    req.setParameter("value", "1");
    wc.getResponse(req);

    // Wait until the session should expire
    Thread.sleep(2000);

    // Do a request, which should cause the session to be expired
    req.setParameter("cmd", QueryCommand.GET.name());
    req.setParameter("param", key);

    WebResponse response = wc.getResponse(req);
    assertThat(response.getText()).isEmpty();
  }

  /**
   * Test that removing a session attribute also removes it from the region
   */
  @Test
  public void testRemoveAttribute() throws Exception {
    String key = "value_testRemoveAttribute";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));

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
    assertThat(response.getText()).isEmpty();
    assertThat(region.get(sessionId).getAttribute(key)).isNull();
  }

  /**
   * Test that a session attribute gets set into the region too.
   */
  @Test
  public void testBasicRegion() throws Exception {
    String key = "value_testBasicRegion";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));

    // Set an attribute
    req.setParameter("cmd", QueryCommand.SET.name());
    req.setParameter("param", key);
    req.setParameter("value", value);
    WebResponse response = wc.getResponse(req);

    String sessionId = response.getNewCookieValue("JSESSIONID");
    assertThat(region.get(sessionId).getAttribute(key)).isEqualTo(value);
  }

  /**
   * Test that a session attribute gets removed from the region when the session is invalidated.
   */
  @Test
  public void testRegionInvalidate() throws Exception {
    String key = "value_testRegionInvalidate";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));

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
    assertThat(region.get(sessionId)).as("The region should not have an entry for this session")
        .isNull();
  }

  /**
   * Test that multiple attribute updates, within the same request result in only the latest one
   * being effective.
   */
  @Test
  public void testMultipleAttributeUpdates() throws Exception {
    final String key = "value_testMultipleAttributeUpdates";
    Callback c = (request, response) -> {
      HttpSession session = request.getSession();
      for (int i = 0; i < 1000; i++) {
        session.setAttribute(key, Integer.toString(i));
      }
    };
    servlet.getServletContext().setAttribute("callback", c);

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));

    // Execute the callback
    req.setParameter("cmd", QueryCommand.CALLBACK.name());
    req.setParameter("param", "callback");
    WebResponse response = wc.getResponse(req);

    String sessionId = response.getNewCookieValue("JSESSIONID");
    assertThat(region.get(sessionId).getAttribute(key)).isEqualTo("999");
  }

  /**
   * Test for issue #38 CommitSessionValve throws exception on invalidated sessions
   */
  @Test
  public void testCommitSessionValveInvalidSession() throws Exception {
    Callback c = (request, response) -> {
      HttpSession session = request.getSession();
      session.invalidate();
      response.getWriter().write("done");
    };
    servlet.getServletContext().setAttribute("callback", c);

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));

    // Execute the callback
    req.setParameter("cmd", QueryCommand.CALLBACK.name());
    req.setParameter("param", "callback");

    WebResponse response = wc.getResponse(req);
    assertThat(response.getText()).isEqualTo("done");
  }

  /**
   * Test for issue #45 Sessions are being created for every request
   */
  @Test
  public void testExtraSessionsNotCreated() throws Exception {
    Callback c = (request, response) -> {
      // Do nothing with sessions
      response.getWriter().write("done");
    };
    servlet.getServletContext().setAttribute("callback", c);

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));

    // Execute the callback
    req.setParameter("cmd", QueryCommand.CALLBACK.name());
    req.setParameter("param", "callback");

    WebResponse response = wc.getResponse(req);
    assertThat(response.getText()).isEqualTo("done");
    assertThat(region.size()).as("The region should contain one entry").isEqualTo(1);
  }

  /**
   * Test for issue #46 lastAccessedTime is not updated at the start of the request, but only at the
   * end.
   */
  @Test
  public void testLastAccessedTime() throws Exception {
    Callback c = (request, response) -> {
      HttpSession session = request.getSession();
      // Hack to expose the session to our test context
      session.getServletContext().setAttribute("session", session);
      session.setAttribute("lastAccessTime", session.getLastAccessedTime());
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {
      }
      session.setAttribute("somethingElse", 1);
      request.getSession();
      response.getWriter().write("done");
    };
    servlet.getServletContext().setAttribute("callback", c);

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));

    // Execute the callback
    req.setParameter("cmd", QueryCommand.CALLBACK.name());
    req.setParameter("param", "callback");
    wc.getResponse(req);

    HttpSession session = (HttpSession) servlet.getServletContext().getAttribute("session");
    Long lastAccess = (Long) session.getAttribute("lastAccessTime");
    assertThat(lastAccess <= session.getLastAccessedTime())
        .as("Last access time not set correctly: " + lastAccess + " not <= "
            + session.getLastAccessedTime())
        .isTrue();
  }
}
