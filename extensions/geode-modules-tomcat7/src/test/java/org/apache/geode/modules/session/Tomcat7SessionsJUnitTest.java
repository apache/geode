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
package org.apache.geode.modules.session;

import static org.junit.Assert.assertEquals;

import org.apache.geode.modules.session.catalina.Tomcat7DeltaSessionManager;
import org.apache.geode.test.junit.categories.IntegrationTest;

import com.meterware.httpunit.GetMethodWebRequest;
import com.meterware.httpunit.WebConversation;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class Tomcat7SessionsJUnitTest extends TestSessionsBase {

  // Set up the session manager we need
  @BeforeClass
  public static void setupClass() throws Exception {
    setupServer(new Tomcat7DeltaSessionManager());
  }

  /**
   * Test setting the session expiration
   */
  @Test
  @Override
  public void testSessionExpiration1() throws Exception {
    // TestSessions only live for a minute
    sessionManager.getTheContext().setSessionTimeout(1);

    String key = "value_testSessionExpiration1";
    String value = "Foo";

    WebConversation wc = new WebConversation();
    WebRequest req = new GetMethodWebRequest(String.format("http://localhost:%d/test", port));

    // Set an attribute
    req.setParameter("cmd", QueryCommand.SET.name());
    req.setParameter("param", key);
    req.setParameter("value", value);
    WebResponse response = wc.getResponse(req);

    // Sleep a while
    Thread.sleep(65000);

    // The attribute should not be accessible now...
    req.setParameter("cmd", QueryCommand.GET.name());
    req.setParameter("param", key);
    response = wc.getResponse(req);

    assertEquals("", response.getText());
  }
}
