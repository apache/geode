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

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


@Category(IntegrationTest.class)
public class PulseVerificationTest {

  @ClassRule
  public static LocatorStarterRule locatorStarterRule = new LocatorStarterRule();

  @ClassRule
  public static RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private static int httpPort = AvailablePortHelper.getRandomAvailableTCPPort();

  // use a random port when fixing GEODE-2671
  private static int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();

  private static HttpHost host;

  private HttpClient httpClient;
  private HttpContext context;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(HTTP_SERVICE_PORT, httpPort + "");
    properties.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    properties.setProperty(JMX_MANAGER_PORT, jmxPort + "");

    locatorStarterRule.startLocator(properties);
    host = new HttpHost("localhost", httpPort);
  }

  @Before
  public void before() throws Exception {
    httpClient = HttpClients.createDefault();
    context = new BasicHttpContext();
  }

  @Test
  public void loginWithIncorrectPassword() throws Exception {
    HttpPost request = new HttpPost("/pulse/login");
    List<NameValuePair> nvps = new ArrayList<>();
    nvps.add(new BasicNameValuePair("username", "data"));
    nvps.add(new BasicNameValuePair("password", "wrongPassword"));
    request.setEntity(new UrlEncodedFormEntity(nvps));
    HttpResponse response = httpClient.execute(host, request, context);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(302);
    assertThat(response.getFirstHeader("Location").getValue())
        .contains("/pulse/login.html?error=BAD_CREDS");
  }

  @Test
  public void loginWithDataOnly() throws Exception {
    HttpPost post = new HttpPost("/pulse/login");
    List<NameValuePair> nvps = new ArrayList<>();
    nvps.add(new BasicNameValuePair("username", "data"));
    nvps.add(new BasicNameValuePair("password", "data"));
    post.setEntity(new UrlEncodedFormEntity(nvps));

    HttpResponse response = httpClient.execute(host, post, context);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(302);
    assertThat(response.getFirstHeader("Location").getValue())
        .contains("/pulse/clusterDetail.html");

    // this would requiest cluster permission
    HttpGet get = new HttpGet("/pulse/clusterDetail.html");
    response = httpClient.execute(host, get);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(403);

    // this would require both cluster and data permission
    get = new HttpGet("/pulse/dataBrowser.html");
    response = httpClient.execute(host, get);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(403);
  }


  @Test
  public void loginAllAccess() throws Exception {
    HttpPost post = new HttpPost("/pulse/login");
    List<NameValuePair> nvps = new ArrayList<>();
    nvps.add(new BasicNameValuePair("username", "CLUSTER,DATA"));
    nvps.add(new BasicNameValuePair("password", "CLUSTER,DATA"));
    post.setEntity(new UrlEncodedFormEntity(nvps));

    HttpResponse response = httpClient.execute(host, post, context);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(302);
    assertThat(response.getFirstHeader("Location").getValue())
        .contains("/pulse/clusterDetail.html");

    HttpGet get = new HttpGet("/pulse/clusterDetail.html");
    response = httpClient.execute(host, get);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

    get = new HttpGet("/pulse/dataBrowser.html");
    response = httpClient.execute(host, get);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
  }

  @Test
  public void loginWithClusterOnly() throws Exception {
    HttpPost post = new HttpPost("/pulse/login");
    List<NameValuePair> nvps = new ArrayList<>();
    nvps.add(new BasicNameValuePair("username", "cluster"));
    nvps.add(new BasicNameValuePair("password", "cluster"));
    post.setEntity(new UrlEncodedFormEntity(nvps));

    HttpResponse response = httpClient.execute(host, post, context);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(302);
    assertThat(response.getFirstHeader("Location").getValue())
        .contains("/pulse/clusterDetail.html");

    HttpGet get = new HttpGet("/pulse/clusterDetail.html");
    response = httpClient.execute(host, get);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

    // accessing data browser will be denied
    get = new HttpGet("/pulse/dataBrowser.html");
    response = httpClient.execute(host, get);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(403);
  }


}
