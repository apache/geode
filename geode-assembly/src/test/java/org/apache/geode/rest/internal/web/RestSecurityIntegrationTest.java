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
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

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
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.templates.SampleSecurityManager;
import org.apache.geode.test.dunit.rules.ServerStarter;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;


@Category({ IntegrationTest.class, SecurityTest.class })
public class RestSecurityIntegrationTest {

  protected static final String REGION_NAME = "AuthRegion";

  public final static String PROTOCOL = "http";
  public final static String HOSTNAME = "localhost";
  public final static String CONTEXT = "/geode/v1";

  private static int restPort = AvailablePortHelper.getRandomAvailableTCPPort();
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

  @Test
  public void testFunctions() throws Exception {
    String json = "{\"@type\":\"double\",\"@value\":210}";
    HttpResponse response = doGet("/functions", "unknown-user", "1234567");
    assertEquals(401, getCode(response));
    response = doGet("/functions", "stranger", "1234567");
    assertEquals(403, getCode(response));
    response = doGet("/functions", "dataReader", "1234567");
    assertTrue(isOK(response));

    response = doPost("/functions/AddFreeItemsToOrder", "unknown-user", "1234567", json);
    assertEquals(401, getCode(response));
    response = doPost("/functions/AddFreeItemsToOrder", "dataReader", "1234567", json);
    assertEquals(403, getCode(response));
    response = doPost("/functions/AddFreeItemsToOrder?onRegion=" + REGION_NAME, "dataWriter", "1234567", json);
    // because we're only testing the security of the endpoint, not the endpoint functionality, a 500 is acceptable
    assertEquals(500, getCode(response));
  }

  @Test
  public void testQueries() throws Exception {
    HttpResponse response = doGet("/queries", "unknown-user", "1234567");
    assertEquals(401, getCode(response));
    response = doGet("/queries", "stranger", "1234567");
    assertEquals(403, getCode(response));
    response = doGet("/queries", "dataReader", "1234567");
    assertEquals(200, getCode(response));
  }

  @Test
  public void testAdhocQuery() throws Exception {
    HttpResponse response = doGet("/queries/adhoc?q=", "unknown-user", "1234567");
    assertEquals(401, getCode(response));
    response = doGet("/queries/adhoc?q=", "stranger", "1234567");
    assertEquals(403, getCode(response));
    response = doGet("/queries/adhoc?q=", "dataReader", "1234567");
    // because we're only testing the security of the endpoint, not the endpoint functionality, a 500 is acceptable
    assertEquals(500, getCode(response));
  }

  @Test
  public void testPostQuery() throws Exception {
    HttpResponse response = doPost("/queries?id=0&q=", "unknown-user", "1234567", "");
    assertEquals(401, getCode(response));
    response = doPost("/queries?id=0&q=", "stranger", "1234567", "");
    assertEquals(403, getCode(response));
    response = doPost("/queries?id=0&q=", "dataReader", "1234567", "");
    // because we're only testing the security of the endpoint, not the endpoint functionality, a 500 is acceptable
    assertEquals(500, getCode(response));
  }

  @Test
  public void testPostQuery2() throws Exception {
    HttpResponse response = doPost("/queries/id", "unknown-user", "1234567", "{\"id\" : \"foo\"}");
    assertEquals(401, getCode(response));
    response = doPost("/queries/id", "stranger", "1234567", "{\"id\" : \"foo\"}");
    assertEquals(403, getCode(response));
    response = doPost("/queries/id", "dataReader", "1234567", "{\"id\" : \"foo\"}");
    // because we're only testing the security of the endpoint, not the endpoint functionality, a 500 is acceptable
    assertEquals(500, getCode(response));
  }

  @Test
  public void testPutQuery() throws Exception {
    HttpResponse response = doPut("/queries/id", "unknown-user", "1234567", "{\"id\" : \"foo\"}");
    assertEquals(401, getCode(response));
    response = doPut("/queries/id", "stranger", "1234567", "{\"id\" : \"foo\"}");
    assertEquals(403, getCode(response));
    response = doPut("/queries/id", "dataReader", "1234567", "{\"id\" : \"foo\"}");
    // We should get a 404 because we're trying to update a query that doesn't exist
    assertEquals(404, getCode(response));
  }

  @Test
  public void testDeleteQuery() throws Exception {
    HttpResponse response = doDelete("/queries/id", "unknown-user", "1234567");
    assertEquals(401, getCode(response));
    response = doDelete("/queries/id", "stranger", "1234567");
    assertEquals(403, getCode(response));
    response = doDelete("/queries/id", "dataWriter", "1234567");
    // We should get a 404 because we're trying to delete a query that doesn't exist
    assertEquals(404, getCode(response));
  }

  @Test
  public void testServers() throws Exception {
    HttpResponse response = doGet("/servers", "unknown-user", "1234567");
    assertEquals(401, getCode(response));
    response = doGet("/servers", "stranger", "1234567");
    assertEquals(403, getCode(response));
    response = doGet("/servers", "super-user", "1234567");
    assertTrue(isOK(response));
  }

  /**
   * This test should always return an OK, whether the user is known or unknown.  A phishing script should not be
   * able to determine whether a user/password combination is good
   */
  @Test
  public void testPing() throws Exception {
    HttpResponse response = doHEAD("/ping", "stranger", "1234567");
    assertTrue(isOK(response));
    response = doGet("/ping", "stranger", "1234567");
    assertTrue(isOK(response));

    response = doHEAD("/ping", "super-user", "1234567");
    assertTrue(isOK(response));
    response = doGet("/ping", "super-user", "1234567");
    assertTrue(isOK(response));

    // TODO - invalid username/password should still respond, but doesn't
    //      response = doHEAD("/ping", "unknown-user", "badpassword");
    //      assertTrue(isOK(response));
    //      response = doGet("/ping", "unknown-user", "badpassword");
    //      assertTrue(isOK(response));

    // TODO - credentials are currently required and shouldn't be for this endpoint
    //      response = doHEAD("/ping", null, null);
    //      assertTrue(isOK(response));
    //      response = doGet("/ping", null, null);
    //      assertTrue(isOK(response));
  }

  /**
   * Test permissions on retrieving a list of regions.
   */
  @Test
  public void getRegions() throws Exception {
    HttpResponse response = doGet("", "dataReader", "1234567");
    assertEquals("A '200 - OK' was expected", 200, getCode(response));

    assertTrue(isOK(response));
    JSONObject jsonObject = new JSONObject(getResponseBody(response));
    JSONArray regions = jsonObject.getJSONArray("regions");
    assertNotNull(regions);
    assertTrue(regions.length() > 0);
    JSONObject region = regions.getJSONObject(0);
    assertEquals("AuthRegion", region.get("name"));
    assertEquals("REPLICATE", region.get("type"));

    // List regions with an unknown user - 401
    response = doGet("", "unknown-user", "badpassword");
    assertEquals(401, getCode(response));

    // list regions with insufficent rights - 403
    response = doGet("", "authRegionReader", "1234567");
    assertEquals(403, getCode(response));
  }

  /**
   * Test permissions on getting a region
   */
  @Test
  public void getRegion() throws Exception {
    // Test an unknown user - 401 error
    HttpResponse response = doGet("/" + REGION_NAME, "unknown-user", "1234567");
    assertEquals(401, getCode(response));

    // Test a user with insufficient rights - 403
    response = doGet("/" + REGION_NAME, "stranger", "1234567");
    assertEquals(403, getCode(response));

    // Test an authorized user - 200
    response = doGet("/" + REGION_NAME, "super-user", "1234567");
    assertTrue(isOK(response));
  }

  /**
   * Test permissions on HEAD region
   */
  @Test
  public void headRegion() throws Exception {
    // Test an unknown user - 401 error
    HttpResponse response = doHEAD("/" + REGION_NAME, "unknown-user", "1234567");
    assertEquals(401, getCode(response));

    // Test a user with insufficient rights - 403
    response = doHEAD("/" + REGION_NAME, "stranger", "1234567");
    assertEquals(403, getCode(response));

    // Test an authorized user - 200
    response = doHEAD("/" + REGION_NAME, "super-user", "1234567");
    assertTrue(isOK(response));
  }

  /**
   * Test permissions on deleting a region
   */
  @Test
  public void deleteRegion() throws Exception {
    // Test an unknown user - 401 error
    HttpResponse response = doDelete("/" + REGION_NAME, "unknown-user", "1234567");
    assertEquals(401, getCode(response));

    // Test a user with insufficient rights - 403
    response = doDelete("/" + REGION_NAME, "dataReader", "1234567");
    assertEquals(403, getCode(response));
  }

  /**
   * Test permissions on getting a region's keys
   */
  @Test
  public void getRegionKeys() throws Exception {
    // Test an authorized user
    HttpResponse response = doGet("/" + REGION_NAME + "/keys", "super-user", "1234567");
    assertTrue(isOK(response));
    // Test an unauthorized user
    response = doGet("/" + REGION_NAME + "/keys", "dataWriter", "1234567");
    assertEquals(403, getCode(response));
  }

  /**
   * Test permissions on retrieving a key from a region
   */
  @Test
  public void getRegionKey() throws Exception {
    // Test an authorized user
    HttpResponse response = doGet("/" + REGION_NAME + "/key1", "key1User", "1234567");
    assertTrue(isOK(response));
    // Test an unauthorized user
    response = doGet("/" + REGION_NAME + "/key1", "dataWriter", "1234567");
    assertEquals(403, getCode(response));
  }

  /**
   * Test permissions on deleting a region's key(s)
   */
  @Test
  public void deleteRegionKey() throws Exception {
    // Test an unknown user - 401 error
    HttpResponse response = doDelete("/" + REGION_NAME + "/key1", "unknown-user", "1234567");
    assertEquals(401, getCode(response));

    // Test a user with insufficient rights - 403
    response = doDelete("/" + REGION_NAME + "/key1", "dataReader", "1234567");
    assertEquals(403, getCode(response));

    // Test an authorized user - 200
    response = doDelete("/" + REGION_NAME + "/key1", "key1User", "1234567");
    assertTrue(isOK(response));
  }

  /**
   * Test permissions on deleting a region's key(s)
   */
  @Test
  public void postRegionKey() throws Exception {
    // Test an unknown user - 401 error
    HttpResponse response = doPost("/" + REGION_NAME + "?key9", "unknown", "1234567", "{ \"key9\" : \"foo\" }");
    assertEquals(401, getCode(response));

    // Test a user with insufficient rights - 403
    response = doPost("/" + REGION_NAME + "?key9", "dataReader", "1234567", "{ \"key9\" : \"foo\" }");
    assertEquals(403, getCode(response));

    // Test an authorized user - 200
    response = doPost("/" + REGION_NAME + "?key9", "dataWriter", "1234567", "{ \"key9\" : \"foo\" }");
    assertEquals(201, getCode(response));
    assertTrue(isOK(response));
  }

  /**
   * Test permissions on deleting a region's key(s)
   */
  @Test
  public void putRegionKey() throws Exception {

    String json = "{\"@type\":\"com.gemstone.gemfire.web.rest.domain.Order\",\"purchaseOrderNo\":1121,\"customerId\":1012,\"description\":\"Order for  XYZ Corp\",\"orderDate\":\"02/10/2014\",\"deliveryDate\":\"02/20/2014\",\"contact\":\"Jelly Bean\",\"email\":\"jelly.bean@example.com\",\"phone\":\"01-2048096\",\"items\":[{\"itemNo\":1,\"description\":\"Product-100\",\"quantity\":12,\"unitPrice\":5,\"totalPrice\":60}],\"totalPrice\":225}";
    String casJSON = "{\"@old\":{\"@type\":\"com.gemstone.gemfire.web.rest.domain.Order\",\"purchaseOrderNo\":1121,\"customerId\":1012,\"description\":\"Order for  XYZ Corp\",\"orderDate\":\"02/10/2014\",\"deliveryDate\":\"02/20/2014\",\"contact\":\"Jelly Bean\",\"email\":\"jelly.bean@example.com\",\"phone\":\"01-2048096\",\"items\":[{\"itemNo\":1,\"description\":\"Product-100\",\"quantity\":12,\"unitPrice\":5,\"totalPrice\":60}],\"totalPrice\":225},\"@new \":{\"@type\":\"com.gemstone.gemfire.web.rest.domain.Order\",\"purchaseOrderNo\":1121,\"customerId\":1013,\"description\":\"Order for  New Corp\",\"orderDate\":\"02/10/2014\",\"deliveryDate\":\"02/25/2014\",\"contact\":\"Vanilla Bean\",\"email\":\"vanillabean@example.com\",\"phone\":\"01-2048096\",\"items\":[{\"itemNo\":12345,\"description\":\"part 123\",\"quantity\":12,\"unitPrice\":29.99,\"totalPrice\":149.95}],\"totalPrice\":149.95}}";
    // Test an unknown user - 401 error
    HttpResponse response = doPut("/" + REGION_NAME + "/key1?op=PUT", "unknown-user", "1234567", "{ \"key9\" : \"foo\" }");
    assertEquals(401, getCode(response));

    response = doPut("/" + REGION_NAME + "/key1?op=CAS", "unknown-user", "1234567", "{ \"key9\" : \"foo\" }");
    assertEquals(401, getCode(response));
    response = doPut("/" + REGION_NAME + "/key1?op=REPLACE", "unknown-user", "1234567", "{ \"@old\" : \"value1\", \"@new\" : \"CASvalue\" }");
    assertEquals(401, getCode(response));

    response = doPut("/" + REGION_NAME + "/key1?op=PUT", "dataReader", "1234567", "{ \"key1\" : \"foo\" }");
    assertEquals(403, getCode(response));

    response = doPut("/" + REGION_NAME + "/key1?op=REPLACE", "dataReader", "1234567", "{ \"key1\" : \"foo\" }");
    assertEquals(403, getCode(response));

    response = doPut("/" + REGION_NAME + "/key1?op=CAS", "dataReader", "1234567", casJSON);
    assertEquals(403, getCode(response));

    response = doPut("/" + REGION_NAME + "/key1?op=PUT", "key1User", "1234567", "{ \"key1\" : \"foo\" }");
    assertEquals(200, getCode(response));
    assertTrue(isOK(response));

    response = doPut("/" + REGION_NAME + "/key1?op=REPLACE", "key1User", "1234567", json);
    assertEquals(200, getCode(response));
    assertTrue(isOK(response));
  }

  protected HttpResponse doHEAD(String query, String username, String password) throws MalformedURLException {
    HttpHead httpHead = new HttpHead(CONTEXT + query);
    return doRequest(httpHead, username, password);
  }


  protected HttpResponse doPost(String query, String username, String password, String body) throws MalformedURLException {
    HttpPost httpPost = new HttpPost(CONTEXT + query);
    httpPost.addHeader("content-type", "application/json");
    httpPost.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(httpPost, username, password);
  }


  protected HttpResponse doPut(String query, String username, String password, String body) throws MalformedURLException {
    HttpPut httpPut = new HttpPut(CONTEXT + query);
    httpPut.addHeader("content-type", "application/json");
    httpPut.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(httpPut, username, password);
  }

  protected HttpResponse doGet(String uri, String username, String password) throws MalformedURLException {
    HttpGet getRequest = new HttpGet(CONTEXT + uri);
    return doRequest(getRequest, username, password);
  }

  protected HttpResponse doDelete(String uri, String username, String password) throws MalformedURLException {
    HttpDelete httpDelete = new HttpDelete(CONTEXT + uri);
    return doRequest(httpDelete, username, password);
  }

  /**
   * Check the HTTP status of the response and return if it's within the OK range
   *
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
   *
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
   *
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
    BufferedReader reader = new BufferedReader(new InputStreamReader(content));
    String line;
    StringBuilder str = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      str.append(line);
    }
    return new JSONTokener(str.toString());
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
}
