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
 *
 */

package org.apache.geode.tools.pulse.tests;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.Locator;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.tools.pulse.tests.rules.WebDriverRule;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.cookie.ClientCookie;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.openqa.selenium.Cookie;

import java.net.URISyntaxException;
import java.util.Properties;

@Category(DistributedTest.class)
public class PulseDataExportTest extends JUnit4DistributedTestCase {

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  private Locator locator;
  private org.apache.geode.test.dunit.rules.Server server;
  private GfshShellConnectionRule gfshConnector;
  private HttpClient httpClient;


  @Before
  public void before() throws Throwable {
    IgnoredException
        .addIgnoredException("Failed to properly release resources held by the HTTP service:");
    IgnoredException.addIgnoredException("!STOPPED");

    locator = lsRule.startLocatorVMWithPulse(0, new Properties());

    gfshConnector = new GfshShellConnectionRule(locator);
    gfshConnector.connect();
    assertThat(gfshConnector.isConnected()).isTrue();

    server = lsRule.startServerVM(1, locator.getPort());

    gfshConnector.executeAndVerifyCommand("create region --name=regionA --type=REPLICATE");
    org.apache.geode.cache.Region<String, String> region = new ClientCacheFactory()
        .addPoolLocator("localhost", locator.getPort()).create()
        .<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY).create("regionA");

    region.put("key1", "value1");
    region.put("key2", "value2");
    region.put("key3", "value3");

    httpClient = HttpClientBuilder.create().build();
  }


  @Test
  public void dataBrowserExportWorksAsExpected() throws Throwable {
    HttpGet dataExportGET = buildDataExportRequest();
    HttpContext authenticatedHttpContext = buildAuthenticatedHttpContext();

    HttpResponse response = httpClient.execute(dataExportGET, authenticatedHttpContext);
    assertThat(response.getStatusLine().getStatusCode()).describedAs(response.toString())
        .isEqualTo(200);

    String responseBody = EntityUtils.toString(response.getEntity(), "UTF-8");
    assertThat(responseBody).isEqualTo(
        "{\"result\":[[\"java.lang.String\",\"value1\"],[\"java.lang.String\",\"value2\"],[\"java.lang.String\",\"value3\"]]}");
  }

  private HttpGet buildDataExportRequest() throws URISyntaxException {
    URIBuilder builder = new URIBuilder();
    builder.setScheme("http").setHost("localhost").setPort(7070).setPath("/pulse/dataBrowserExport")
        .setParameter("query", "select * from /regionA a order by a");
    return new HttpGet(builder.build());
  }

  private HttpContext buildAuthenticatedHttpContext() throws Throwable {
    HttpContext localContext = new BasicHttpContext();
    CookieStore cookieStore = new BasicCookieStore();
    BasicClientCookie cookie = new BasicClientCookie("JSESSIONID", getAuthenticatedJSESSIONID());
    cookie.setDomain("localhost");
    cookie.setAttribute(ClientCookie.DOMAIN_ATTR, "true");
    cookie.setPath("/");
    cookieStore.addCookie(cookie);
    localContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);

    return localContext;
  }

  public String getAuthenticatedJSESSIONID() throws Throwable {
    WebDriverRule webDriverRule =
        new WebDriverRule("admin", "admin", "http://localhost:7070/pulse/");
    webDriverRule.before();

    Cookie loginCookie = webDriverRule.getDriver().manage().getCookieNamed("JSESSIONID");
    webDriverRule.after();

    assertThat(loginCookie.getValue()).isNotEmpty();
    return loginCookie.getValue();
  }

}
