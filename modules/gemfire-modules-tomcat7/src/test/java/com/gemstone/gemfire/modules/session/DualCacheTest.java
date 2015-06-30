/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.gemstone.gemfire.modules.session;

import junit.framework.TestCase;
import com.meterware.httpunit.GetMethodWebRequest;
import com.meterware.httpunit.WebConversation;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;

import static junit.framework.Assert.*;

/**
 *
 */
public class DualCacheTest extends TestCase {

  /**
   * Check that our session persists. The values we pass in as query params are
   * used to set attributes on the session.
   */
  public void testSessionFailover() throws Exception {
    String key = "value_testSessionFailover";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req1 = new GetMethodWebRequest("http://localhost:7890/test");
    req1.setParameter("cmd", QueryCommand.SET.name());
    req1.setParameter("param", key);
    req1.setParameter("value", value);
    WebResponse response = wc.getResponse(req1);
    String sessionId = response.getNewCookieValue("JSESSIONID");

    assertNotNull("No apparent session cookie", sessionId);

    WebRequest req2 = new GetMethodWebRequest("http://localhost:7891/test");
    req2.setHeaderField("Cookie", "JSESSIONID=" + sessionId);
    req2.setParameter("cmd", QueryCommand.GET.name());
    req2.setParameter("param", key);
    response = wc.getResponse(req2);
    sessionId = response.getNewCookieValue("JSESSIONID");

    assertEquals(value, response.getText());
    assertTrue("The sessionId does not contain the correct JVM route value",
        sessionId.contains("JVM-2"));
  }
}
