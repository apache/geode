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

package org.apache.geode.tools.pulse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.Locator;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Category(DistributedTest.class)
public class PulseDataExportTest {

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  private Locator locator;
  private org.apache.geode.test.dunit.rules.Server server;
  @Rule
  public GfshShellConnectionRule gfshConnector = new GfshShellConnectionRule();
  private HttpClient httpClient;
  private CookieStore cookieStore;

  @Before
  public void before() throws Exception {
    IgnoredException
        .addIgnoredException("Failed to properly release resources held by the HTTP service:");
    IgnoredException.addIgnoredException("!STOPPED");

    locator = lsRule.startLocatorVMWithPulse(0, new Properties());

    gfshConnector.connect(locator);
    assertThat(gfshConnector.isConnected()).isTrue();

    server = lsRule.startServerVM(1, locator.getPort());

    gfshConnector.executeAndVerifyCommand("create region --name=regionA --type=REPLICATE");
    org.apache.geode.cache.Region<String, String> region = new ClientCacheFactory()
        .addPoolLocator("localhost", locator.getPort()).create()
        .<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY).create("regionA");

    region.put("key1", "value1");
    region.put("key2", "value2");
    region.put("key3", "value3");
    cookieStore = new BasicCookieStore();
    httpClient = HttpClientBuilder.create().setDefaultCookieStore(cookieStore).build();

    await().atMost(2, TimeUnit.MINUTES).until(this::pulseServerHasStarted);
  }

  @Category(FlakyTest.class) // GEODE-2395
  @Test
  public void dataBrowserExportWorksAsExpected() throws Exception {
    getAuthenticatedJSESSIONID();
    HttpContext authenticatedHttpContext = buildAuthenticatedHttpContext();

    HttpGet dataExportGET = buildDataExportGET();

    HttpResponse response = httpClient.execute(dataExportGET, authenticatedHttpContext);
    assertThat(response.getStatusLine().getStatusCode()).describedAs(response.toString())
        .isEqualTo(200);

    String responseBody = EntityUtils.toString(response.getEntity(), "UTF-8");
    assertThat(responseBody).describedAs(response.toString()).isEqualTo(
        "{\"result\":[[\"java.lang.String\",\"value1\"],[\"java.lang.String\",\"value2\"],[\"java.lang.String\",\"value3\"]]}");
  }

  private HttpPost buildLoginPOST() {
    HttpPost httpPost = new HttpPost("http://localhost:7070/pulse/login");

    List<NameValuePair> formData = new ArrayList<>();
    formData.add(new BasicNameValuePair("username", "admin"));
    formData.add(new BasicNameValuePair("password", "admin"));

    httpPost.setEntity(new UrlEncodedFormEntity(formData, Consts.UTF_8));

    return httpPost;
  }

  private HttpGet buildDataExportGET() throws URISyntaxException {
    URIBuilder builder = new URIBuilder();
    builder.setScheme("http").setHost("localhost").setPort(7070).setPath("/pulse/dataBrowserExport")
        .setParameter("query", "select * from /regionA a order by a");
    return new HttpGet(builder.build());
  }

  private HttpContext buildAuthenticatedHttpContext() {
    HttpContext localContext = new BasicHttpContext();
    localContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);

    return localContext;
  }

  private void getAuthenticatedJSESSIONID() throws IOException {
    HttpResponse loginResponse = httpClient.execute(buildLoginPOST());
    assertThat(loginResponse.getStatusLine().getStatusCode()).describedAs(loginResponse.toString())
        .isEqualTo(302);

    String JSESSIONIDFromSetCookieHeader = Arrays.stream(loginResponse.getHeaders("SET-COOKIE"))
        .map(Header::getValue).filter(setCookie -> setCookie.contains("JSESSIONID")).findAny()
        .orElseThrow(() -> new AssertionError(
            "No JSESSIONID cookie was set in the login response: " + loginResponse.toString()));

    Cookie JESSIONIDFromCookieStore = cookieStore.getCookies().stream()
        .filter(cookie -> cookie.getName().equalsIgnoreCase("JSESSIONID")).findFirst()
        .orElseThrow(() -> new AssertionError("No JSESSIONID cookie was set in the cookie store"));

    assertThat(JSESSIONIDFromSetCookieHeader).contains(JESSIONIDFromCookieStore.getValue());
  }

  private boolean pulseServerHasStarted() {
    try {
      httpClient.execute(buildLoginPOST());
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }

    return true;
  }

}
