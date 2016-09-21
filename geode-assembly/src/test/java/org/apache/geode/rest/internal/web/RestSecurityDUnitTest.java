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

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.AbstractSecureServerDUnitTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONTokener;
import org.junit.experimental.categories.Category;


@Category({ DistributedTest.class, SecurityTest.class })
public class RestSecurityDUnitTest extends AbstractSecureServerDUnitTest {

  public final static String PROTOCOL = "http";
  public final static String HOSTNAME = "localhost";
  public final static String CONTEXT = "/geode/v1";

  private final String endPoint;
  private final URL url;

  public RestSecurityDUnitTest() throws MalformedURLException {
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    this.jmxPort = ports[0];
    this.restPort = ports[1];
    endPoint = PROTOCOL + "://" + HOSTNAME + ":" + restPort + CONTEXT;
    url = new URL(endPoint);
  }

  protected HttpResponse doHEAD(String query, String username, String password) throws MalformedURLException {
    HttpHost targetHost = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
    HttpClientContext clientContext = HttpClientContext.create();
    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()), new UsernamePasswordCredentials(username, password));
    CloseableHttpClient httpclient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
    AuthCache authCache = new BasicAuthCache();
    BasicScheme basicAuth = new BasicScheme();
    authCache.put(targetHost, basicAuth);
    clientContext.setCredentialsProvider(credsProvider);
    clientContext.setAuthCache(authCache);

    HttpHead httpHead = new HttpHead(CONTEXT + query);
    try {
      return httpclient.execute(targetHost, httpHead, clientContext);
    } catch (ClientProtocolException e) {
      e.printStackTrace();
      fail("Rest HEAD should not have thrown ClientProtocolException!");
    } catch (IOException e) {
      e.printStackTrace();
      fail("Rest HEAD Request should not have thrown IOException!");
    }
    return null;
  }

  protected HttpResponse doGet(String query, String username, String password) throws MalformedURLException {
    HttpHost targetHost = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
    CloseableHttpClient httpclient = HttpClients.custom().build();
    HttpClientContext clientContext = HttpClientContext.create();
//    // if username or password are null or empty, do not put in authentication
//    if (!(username == null
//          || password == null
//          || !username.isEmpty()
//          || !password.isEmpty())) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()), new UsernamePasswordCredentials(username, password));
      httpclient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
      AuthCache authCache = new BasicAuthCache();
      BasicScheme basicAuth = new BasicScheme();
      authCache.put(targetHost, basicAuth);
      clientContext.setCredentialsProvider(credsProvider);
      clientContext.setAuthCache(authCache);
//    }

    HttpGet getRequest = new HttpGet(CONTEXT + query);
    try {
      return httpclient.execute(targetHost, getRequest, clientContext);
    } catch (ClientProtocolException e) {
      e.printStackTrace();
      fail("Rest GET should not have thrown ClientProtocolException!");
    } catch (IOException e) {
      e.printStackTrace();
      fail("Rest GET Request should not have thrown IOException!");
    }
    return null;
  }

  protected HttpResponse doDelete(String query, String username, String password) throws MalformedURLException {
    HttpHost targetHost = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()), new UsernamePasswordCredentials(username, password));
    CloseableHttpClient httpclient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
    AuthCache authCache = new BasicAuthCache();
    BasicScheme basicAuth = new BasicScheme();
    authCache.put(targetHost, basicAuth);

    HttpClientContext clientContext = HttpClientContext.create();
    clientContext.setCredentialsProvider(credsProvider);
    clientContext.setAuthCache(authCache);

    HttpDelete httpDelete = new HttpDelete(CONTEXT + query);
    try {
      return httpclient.execute(targetHost, httpDelete, clientContext);
    } catch (ClientProtocolException e) {
      e.printStackTrace();
      fail("Rest DELETE Request should not have thrown ClientProtocolException!");
    } catch (IOException e) {
      e.printStackTrace();
      fail("Rest DELETE Request should not have thrown IOException!");
    }
    return null;
  }

  protected HttpResponse doPost(String query, String username, String password, String body) throws MalformedURLException {
    HttpHost targetHost = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()), new UsernamePasswordCredentials(username, password));
    CloseableHttpClient httpclient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
    AuthCache authCache = new BasicAuthCache();
    BasicScheme basicAuth = new BasicScheme();
    authCache.put(targetHost, basicAuth);

    HttpClientContext clientContext = HttpClientContext.create();
    clientContext.setCredentialsProvider(credsProvider);
    clientContext.setAuthCache(authCache);

    HttpPost httpPost = new HttpPost(CONTEXT + query);
    httpPost.addHeader("content-type", "application/json");
    httpPost.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    try {
      return httpclient.execute(targetHost, httpPost, clientContext);
    } catch (ClientProtocolException e) {
      e.printStackTrace();
      fail("Rest POST Request should not have thrown ClientProtocolException!");
    } catch (IOException e) {
      e.printStackTrace();
      fail("Rest POST Request should not have thrown IOException!");
    }
    return null;
  }
  protected HttpResponse doPut(String query, String username, String password, String body) throws MalformedURLException {
    HttpHost targetHost = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()), new UsernamePasswordCredentials(username, password));
    CloseableHttpClient httpclient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
    AuthCache authCache = new BasicAuthCache();
    BasicScheme basicAuth = new BasicScheme();
    authCache.put(targetHost, basicAuth);

    HttpClientContext clientContext = HttpClientContext.create();
    clientContext.setCredentialsProvider(credsProvider);
    clientContext.setAuthCache(authCache);

    HttpPut httpPut = new HttpPut(CONTEXT + query);
    httpPut.addHeader("content-type", "application/json");
    httpPut.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    try {
      return httpclient.execute(targetHost, httpPut, clientContext);
    } catch (ClientProtocolException e) {
      e.printStackTrace();
      fail("Rest PUT Request should not have thrown ClientProtocolException!");
    } catch (IOException e) {
      e.printStackTrace();
      fail("Rest PUT Request should not have thrown IOException!");
    }
    return null;
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

  /**
   * Check the HTTP status of the response and return true if a 401
   * @param response The HttpResponse message received from the server
   *
   * @return true if the status code is 401, otherwise false
   */
  protected boolean isUnauthorized(HttpResponse response) {
    int returnCode = response.getStatusLine().getStatusCode();
    return returnCode == 401;
  }

  /**
   * Retrieve the status code of the HttpResponse
   * @param response The HttpResponse message received from the server
   *
   * @return a numeric value
   */
  protected int getCode(HttpResponse response) {
    return response.getStatusLine().getStatusCode();
  }

  protected JSONTokener getResponseBody(HttpResponse response) throws IOException {
    HttpEntity entity = response.getEntity();
    InputStream content = entity.getContent();
    BufferedReader reader = new BufferedReader(new InputStreamReader(
      content));
    String line;
    StringBuilder str = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      str.append(line);
    }
    return new JSONTokener(str.toString());
  }
}
