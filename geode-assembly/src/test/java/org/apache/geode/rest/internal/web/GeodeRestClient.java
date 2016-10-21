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

package org.apache.geode.rest.internal.web;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;

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
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONTokener;
import org.junit.Assert;

public class GeodeRestClient {

  public final static String PROTOCOL = "http";
  public final static String HOSTNAME = "localhost";
  public final static String CONTEXT = "/geode/v1";

  private int restPort = 0;

  public GeodeRestClient(int restPort) {
    this.restPort = restPort;
  }

  public HttpResponse doHEAD(String query, String username, String password)
      throws MalformedURLException {
    HttpHead httpHead = new HttpHead(CONTEXT + query);
    return doRequest(httpHead, username, password);
  }

  public HttpResponse doPost(String query, String username, String password, String body)
      throws MalformedURLException {
    HttpPost httpPost = new HttpPost(CONTEXT + query);
    httpPost.addHeader("content-type", "application/json");
    httpPost.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(httpPost, username, password);
  }

  public HttpResponse doPut(String query, String username, String password, String body)
      throws MalformedURLException {
    HttpPut httpPut = new HttpPut(CONTEXT + query);
    httpPut.addHeader("content-type", "application/json");
    httpPut.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(httpPut, username, password);
  }

  public HttpResponse doGet(String uri, String username, String password)
      throws MalformedURLException {
    HttpGet getRequest = new HttpGet(CONTEXT + uri);
    return doRequest(getRequest, username, password);
  }

  public HttpResponse doGet(String uri) throws MalformedURLException {
    return doGet(uri, null, null);
  }

  public HttpResponse doDelete(String uri, String username, String password)
      throws MalformedURLException {
    HttpDelete httpDelete = new HttpDelete(CONTEXT + uri);
    return doRequest(httpDelete, username, password);
  }

  public static String getContentType(HttpResponse response) {
    return response.getEntity().getContentType().getValue();
  }

  /**
   * Retrieve the status code of the HttpResponse
   *
   * @param response The HttpResponse message received from the server
   *
   * @return a numeric value
   */
  public static int getCode(HttpResponse response) {
    return response.getStatusLine().getStatusCode();
  }

  public static JSONTokener getResponseBody(HttpResponse response) throws IOException {
    HttpEntity entity = response.getEntity();
    InputStream content = entity.getContent();
    BufferedReader reader = new BufferedReader(new InputStreamReader(content));
    String line;
    StringBuilder str = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      str.append(line);
    }
    return new JSONTokener(str.toString());
  }

  private HttpResponse doRequest(HttpRequestBase request, String username, String password)
      throws MalformedURLException {
    HttpHost targetHost = new HttpHost(HOSTNAME, restPort, PROTOCOL);
    CloseableHttpClient httpclient = HttpClients.custom().build();
    HttpClientContext clientContext = HttpClientContext.create();
    // if username is null, do not put in authentication
    if (username != null) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()),
          new UsernamePasswordCredentials(username, password));
      httpclient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
      AuthCache authCache = new BasicAuthCache();
      BasicScheme basicAuth = new BasicScheme();
      authCache.put(targetHost, basicAuth);
      clientContext.setCredentialsProvider(credsProvider);
      clientContext.setAuthCache(authCache);
    }

    try {
      return httpclient.execute(targetHost, request, clientContext);
    } catch (ClientProtocolException e) {
      e.printStackTrace();
      Assert.fail("Rest GET should not have thrown ClientProtocolException!");
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail("Rest GET Request should not have thrown IOException!");
    }
    return null;
  }
}
