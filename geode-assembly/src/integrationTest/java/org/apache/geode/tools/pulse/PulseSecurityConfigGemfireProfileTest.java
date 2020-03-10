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

package org.apache.geode.tools.pulse;

import static org.apache.geode.test.junit.rules.HttpResponseAssert.assertResponse;

import org.apache.http.HttpResponse;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.test.junit.rules.GeodeHttpClientRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

@Category({PulseTest.class})
public class PulseSecurityConfigGemfireProfileTest {
  @ClassRule
  public static LocatorStarterRule locator =
      new LocatorStarterRule().withHttpService()
          .withSecurityManager(SimpleSecurityManager.class)
          .withAutoStart();

  @Rule
  public GeodeHttpClientRule client = new GeodeHttpClientRule(locator::getHttpPort);

  @Test
  public void testLogin() throws Exception {
    HttpResponse response = client.loginToPulse("admin", "wrongPassword");
    assertResponse(response).hasStatusCode(302).hasHeaderValue("Location")
        .contains("/pulse/login.html?error=BAD_CREDS");
    client.loginToPulseAndVerify("cluster", "cluster");
  }

  @Test
  public void dataBrowser() throws Exception {
    client.loginToPulseAndVerify("cluster", "cluster");
    HttpResponse httpResponse = client.get("/pulse/dataBrowser.html");
    assertResponse(httpResponse).hasStatusCode(403)
        .hasResponseBody()
        .contains("You don't have permissions to access this resource.");
  }

  @Test
  public void getQueryStatisticsGridModel() throws Exception {
    client.loginToPulseAndVerify("cluster", "cluster");
    HttpResponse httpResponse = client.get("/pulse/getQueryStatisticsGridModel");
    assertResponse(httpResponse).hasStatusCode(403)
        .hasResponseBody()
        .contains("You don't have permissions to access this resource.");

    client.logoutFromPulse();

    client.loginToPulseAndVerify("cluster,data", "cluster,data");
    httpResponse = client.get("/pulse/getQueryStatisticsGridModel");
    assertResponse(httpResponse).hasStatusCode(200);
  }

  @Test
  public void loginPage() throws Exception {
    HttpResponse response = client.get("/pulse/login.html");
    assertResponse(response).hasStatusCode(200).hasResponseBody().contains("<html>");
  }

  @Test
  public void authenticateUser() throws Exception {
    HttpResponse response = client.get("/pulse/authenticateUser");
    assertResponse(response).hasStatusCode(200).hasResponseBody()
        .isEqualTo("{\"isUserLoggedIn\":false}");
  }

  @Test
  public void dataBrowserRegions() throws Exception {
    HttpResponse response = client.get("/pulse/dataBrowserRegions");
    // get a restricted page will result in login page
    assertResponse(response).hasStatusCode(200).hasResponseBody()
        .contains(
            "<form method=\"POST\" action=\"login\" name=\"loginForm\" id=\"loginForm\" autocomplete=\"off\">");
  }

  @Test
  public void pulseVersion() throws Exception {
    HttpResponse response = client.get("/pulse/pulseVersion");
    assertResponse(response).hasStatusCode(200).hasResponseBody().contains("{\"pulseVersion");
  }
}
