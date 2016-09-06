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
package com.gemstone.gemfire.rest.internal.web;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;

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
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.security.AbstractSecureServerDUnitTest;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class})
public class RestSecurityDUnitTest extends AbstractSecureServerDUnitTest {

  public final static String PROTOCOL = "http";
  public final static String HOSTNAME = "localhost";
  public final static String CONTEXT = "/gemfire-api/v1";


  public RestSecurityDUnitTest() throws MalformedURLException {
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    this.jmxPort = ports[0];
    this.restPort = ports[1];
  }

  @Test
  public void test(){
    client1.invoke(()->{
      HttpResponse response = doGet("/servers", null, null);
      assertEquals(getCode(response), 200);
      assertEquals(getResponseBody(response), "[ \""+"http://localhost:"+this.restPort+"\" ]");
    });
  }

  protected HttpResponse doGet(String uri, String username, String password) throws MalformedURLException {
    HttpGet getRequest = new HttpGet(CONTEXT + uri);
    return doRequest(getRequest, username, password);
  }

  protected HttpResponse doDelete(String uri, String username, String password) throws MalformedURLException {
    HttpDelete httpDelete = new HttpDelete(CONTEXT + uri);
    return doRequest(httpDelete, username, password);
  }

  private HttpResponse doRequest(HttpRequestBase request, String username, String password) throws MalformedURLException {
    HttpHost targetHost = new HttpHost(HOSTNAME, this.restPort, PROTOCOL);
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

    try {
      return httpclient.execute(targetHost, request, clientContext);
    } catch (ClientProtocolException e) {
      e.printStackTrace();
      fail("Rest GET should not have thrown ClientProtocolException!");
    } catch (IOException e) {
      e.printStackTrace();
      fail("Rest GET Request should not have thrown IOException!");
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

  protected String getResponseBody(HttpResponse response) throws IOException {
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
}
