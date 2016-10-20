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
package org.apache.geode.rest.internal.web;

import static org.apache.geode.distributed.ConfigurationProperties.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONTokener;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.templates.SampleSecurityManager;
import org.apache.geode.test.dunit.rules.ServerStarter;

public class AbstractRestTest {

  public final static String PROTOCOL = "http";
  public final static String HOSTNAME = "localhost";
  public final static String CONTEXT = "/geode/v1";
  protected static final String REGION_NAME = "AuthRegion";

  protected static int restPort = AvailablePortHelper.getRandomAvailableTCPPort();

  static Properties properties = new Properties() {{
    setProperty(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/management/internal/security/clientServer.json");
    setProperty(SECURITY_MANAGER, SampleSecurityManager.class.getName());
    setProperty(START_DEV_REST_API, "true");
    setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");
    setProperty(HTTP_SERVICE_PORT, restPort + "");
  }};

  @ClassRule
  public static ServerStarter serverStarter = new ServerStarter(properties);

  @BeforeClass
  public static void before() throws Exception {
    serverStarter.startServer();
    serverStarter.cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
  }

  public static JSONTokener getResponseBodyJson(HttpResponse response) throws IOException {
    return new JSONTokener(getResponseBodyText(response));
  }

  public static String getResponseBodyText(HttpResponse response) throws IOException {
    HttpEntity entity = response.getEntity();
    InputStream content = entity.getContent();
    BufferedReader reader = new BufferedReader(new InputStreamReader(content));
    String line;
    StringBuilder str = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      str.append(line);
    }
    return str.toString();
  }

  public static HttpResponse doRequest(HttpRequestBase request, String username, String password) throws IOException {
    HttpHost targetHost = new HttpHost(HOSTNAME, restPort, PROTOCOL);
    CloseableHttpClient httpclient = HttpClients.custom().build();
    HttpClientContext clientContext = HttpClientContext.create();
    // if username is null, do not put in authentication
    if (username != null) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()), new UsernamePasswordCredentials(username, password));
      httpclient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
      AuthCache authCache = new BasicAuthCache();
      BasicScheme basicAuth = new BasicScheme();
      authCache.put(targetHost, basicAuth);
      clientContext.setCredentialsProvider(credsProvider);
      clientContext.setAuthCache(authCache);
    }

    return httpclient.execute(targetHost, request, clientContext);
  }

  public static HttpResponse doHEAD(String query, String username, String password) throws IOException {
    HttpHead httpHead = new HttpHead(CONTEXT + query);
    return doRequest(httpHead, username, password);
  }


  public static HttpResponse doPost(String query, String username, String password, String body) throws IOException {
    HttpPost httpPost = new HttpPost(CONTEXT + query);
    httpPost.addHeader("content-type", "application/json");
    httpPost.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(httpPost, username, password);
  }


  public static HttpResponse doPut(String query, String username, String password, String body) throws IOException {
    HttpPut httpPut = new HttpPut(CONTEXT + query);
    httpPut.addHeader("content-type", "application/json");
    httpPut.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(httpPut, username, password);
  }

  public static HttpResponse doGet(String uri, String username, String password) throws IOException {
    HttpGet getRequest = new HttpGet(CONTEXT + uri);
    return doRequest(getRequest, username, password);
  }

  public static HttpResponse doDelete(String uri, String username, String password) throws IOException {
    HttpDelete httpDelete = new HttpDelete(CONTEXT + uri);
    return doRequest(httpDelete, username, password);
  }

  /**
   * Retrieve the status code of the HttpResponse
   * @param response The HttpResponse message received from the server
   *
   * @return a numeric value
   */
  public static int getCode(HttpResponse response) {
    return response.getStatusLine().getStatusCode();
  }

  /**
   * Check the HTTP status of the response and return if it's within the OK range
   * @param response The HttpResponse message received from the server
   *
   * @return true if the status code is a 2XX-type code (200-299), otherwise false
   */
  protected boolean isOK(HttpResponse response) {
    int returnCode = response.getStatusLine().getStatusCode();
    return (returnCode < 300 && returnCode >= 200);
  }

}
