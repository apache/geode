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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.gemstone.gemfire.modules.session;

import com.meterware.httpunit.GetMethodWebRequest;
import com.meterware.httpunit.WebConversation;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;
import junit.framework.TestCase;

/**
 *
 */
public class DualCacheTest extends TestCase {

  /**
   * Check that our session persists. The values we pass in as query params are used to set attributes on the session.
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
    assertTrue("The sessionId does not contain the correct JVM route value", sessionId.contains("JVM-2"));
  }
}
