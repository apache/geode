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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.http.HttpResponse;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.http.MediaType;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.rules.RequiresGeodeHome;

@Category({IntegrationTest.class, SecurityTest.class, RestAPITest.class})
public class RestSecurityIntegrationTest {

  protected static final String REGION_NAME = "AuthRegion";

  @ClassRule
  public static ServerStarterRule serverStarter = new ServerStarterRule()
      .withProperty(TestSecurityManager.SECURITY_JSON,
          "org/apache/geode/management/internal/security/clientServer.json")
      .withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName()).withRestService()
      .withAutoStart();

  private final GeodeRestClient restClient =
      new GeodeRestClient("localhost", serverStarter.getHttpPort());

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @BeforeClass
  public static void before() throws Exception {
    serverStarter.getCache().createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
  }

  @Test
  public void testFunctions() throws Exception {
    String json = "{\"@type\":\"double\",\"@value\":210}";

    HttpResponse response = restClient.doGet("/functions", "unknown-user", "1234567");
    assertEquals(401, GeodeRestClient.getCode(response));
    response = restClient.doGet("/functions", "stranger", "1234567");
    assertEquals(403, GeodeRestClient.getCode(response));
    response = restClient.doGet("/functions", "dataReader", "1234567");
    assertEquals(200, GeodeRestClient.getCode(response));
    response.getEntity();
    assertThat(GeodeRestClient.getContentType(response))
        .containsIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE);

    response = restClient.doPost("/functions/AddFreeItemsToOrder", "unknown-user", "1234567", json);
    assertEquals(401, GeodeRestClient.getCode(response));
    response = restClient.doPost("/functions/AddFreeItemsToOrder", "dataReader", "1234567", json);
    assertEquals(403, GeodeRestClient.getCode(response));
    response = restClient.doPost("/functions/AddFreeItemsToOrder?onRegion=" + REGION_NAME,
        "dataWriter", "1234567", json);
    // because we're only testing the security of the endpoint, not the endpoint functionality, a
    // 500 is acceptable
    assertEquals(500, GeodeRestClient.getCode(response));
  }

  @Test
  public void testQueries() throws Exception {
    HttpResponse response = restClient.doGet("/queries", "unknown-user", "1234567");
    assertEquals(401, GeodeRestClient.getCode(response));
    response = restClient.doGet("/queries", "stranger", "1234567");
    assertEquals(403, GeodeRestClient.getCode(response));
    response = restClient.doGet("/queries", "dataReader", "1234567");
    assertEquals(200, GeodeRestClient.getCode(response));
    assertThat(GeodeRestClient.getContentType(response))
        .containsIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE);
  }

  @Test
  public void testAdhocQuery() throws Exception {
    HttpResponse response = restClient.doGet("/queries/adhoc?q=", "unknown-user", "1234567");
    assertEquals(401, GeodeRestClient.getCode(response));
    response = restClient.doGet("/queries/adhoc?q=", "stranger", "1234567");
    assertEquals(403, GeodeRestClient.getCode(response));
    response = restClient.doGet("/queries/adhoc?q=", "dataReader", "1234567");
    // because we're only testing the security of the endpoint, not the endpoint functionality, a
    // 500 is acceptable
    assertEquals(500, GeodeRestClient.getCode(response));
  }

  @Test
  public void testPostQuery() throws Exception {
    HttpResponse response = restClient.doPost("/queries?id=0&q=", "unknown-user", "1234567", "");
    assertEquals(401, GeodeRestClient.getCode(response));
    response = restClient.doPost("/queries?id=0&q=", "stranger", "1234567", "");
    assertEquals(403, GeodeRestClient.getCode(response));
    response = restClient.doPost("/queries?id=0&q=", "dataReader", "1234567", "");
    // because we're only testing the security of the endpoint, not the endpoint functionality, a
    // 500 is acceptable
    assertEquals(500, GeodeRestClient.getCode(response));
  }

  @Test
  public void testPostQuery2() throws Exception {
    HttpResponse response =
        restClient.doPost("/queries/id", "unknown-user", "1234567", "{\"id\" : \"foo\"}");
    assertEquals(401, GeodeRestClient.getCode(response));
    response = restClient.doPost("/queries/id", "stranger", "1234567", "{\"id\" : \"foo\"}");
    assertEquals(403, GeodeRestClient.getCode(response));
    response = restClient.doPost("/queries/id", "dataReader", "1234567", "{\"id\" : \"foo\"}");
    // We should get a 404 because we're trying to update a query that doesn't exist
    assertEquals(404, GeodeRestClient.getCode(response));
  }

  @Test
  public void testPutQuery() throws Exception {
    HttpResponse response =
        restClient.doPut("/queries/id", "unknown-user", "1234567", "{\"id\" : \"foo\"}");
    assertEquals(401, GeodeRestClient.getCode(response));
    response = restClient.doPut("/queries/id", "stranger", "1234567", "{\"id\" : \"foo\"}");
    assertEquals(403, GeodeRestClient.getCode(response));
    response = restClient.doPut("/queries/id", "dataReader", "1234567", "{\"id\" : \"foo\"}");
    // We should get a 404 because we're trying to update a query that doesn't exist
    assertEquals(404, GeodeRestClient.getCode(response));
  }

  @Test
  public void testDeleteQuery() throws Exception {
    HttpResponse response = restClient.doDelete("/queries/id", "unknown-user", "1234567");
    assertEquals(401, GeodeRestClient.getCode(response));
    response = restClient.doDelete("/queries/id", "stranger", "1234567");
    assertEquals(403, GeodeRestClient.getCode(response));
    response = restClient.doDelete("/queries/id", "dataWriter", "1234567");
    // We should get a 404 because we're trying to delete a query that doesn't exist
    assertEquals(404, GeodeRestClient.getCode(response));
  }

  @Test
  public void testServers() throws Exception {
    HttpResponse response = restClient.doGet("/servers", "unknown-user", "1234567");
    assertEquals(401, GeodeRestClient.getCode(response));
    response = restClient.doGet("/servers", "stranger", "1234567");
    assertEquals(403, GeodeRestClient.getCode(response));
    response = restClient.doGet("/servers", "super-user", "1234567");
    assertEquals(200, GeodeRestClient.getCode(response));
    assertThat(GeodeRestClient.getContentType(response))
        .containsIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE);
  }

  /**
   * This test should always return an OK, whether the user is known or unknown. A phishing script
   * should not be able to determine whether a user/password combination is good
   */
  @Test
  public void testPing() throws Exception {
    HttpResponse response = restClient.doHEAD("/ping", "stranger", "1234567");
    assertEquals(200, GeodeRestClient.getCode(response));
    response = restClient.doGet("/ping", "stranger", "1234567");
    assertEquals(200, GeodeRestClient.getCode(response));

    response = restClient.doHEAD("/ping", "super-user", "1234567");
    assertEquals(200, GeodeRestClient.getCode(response));
    response = restClient.doGet("/ping", "super-user", "1234567");
    assertEquals(200, GeodeRestClient.getCode(response));
  }

  /**
   * Test permissions on retrieving a list of regions.
   */
  @Test
  public void getRegions() throws Exception {
    HttpResponse response = restClient.doGet("", "dataReader", "1234567");
    assertEquals("A '200 - OK' was expected", 200, GeodeRestClient.getCode(response));
    assertThat(GeodeRestClient.getContentType(response))
        .containsIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE);

    JSONObject jsonObject = GeodeRestClient.getJsonObject(response);
    JSONArray regions = jsonObject.getJSONArray("regions");
    assertNotNull(regions);
    assertTrue(regions.length() > 0);
    JSONObject region = regions.getJSONObject(0);
    assertEquals("AuthRegion", region.get("name"));
    assertEquals("REPLICATE", region.get("type"));

    // List regions with an unknown user - 401
    response = restClient.doGet("", "unknown-user", "badpassword");
    assertEquals(401, GeodeRestClient.getCode(response));

    // list regions with insufficent rights - 403
    response = restClient.doGet("", "authRegionReader", "1234567");
    assertEquals(403, GeodeRestClient.getCode(response));
  }

  /**
   * Test permissions on getting a region
   */
  @Test
  public void getRegion() throws Exception {
    // Test an unknown user - 401 error
    HttpResponse response = restClient.doGet("/" + REGION_NAME, "unknown-user", "1234567");
    assertEquals(401, GeodeRestClient.getCode(response));

    // Test a user with insufficient rights - 403
    response = restClient.doGet("/" + REGION_NAME, "stranger", "1234567");
    assertEquals(403, GeodeRestClient.getCode(response));

    // Test an authorized user - 200
    response = restClient.doGet("/" + REGION_NAME, "super-user", "1234567");
    assertEquals(200, GeodeRestClient.getCode(response));
    assertThat(GeodeRestClient.getContentType(response))
        .containsIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE);
  }

  /**
   * Test permissions on HEAD region
   */
  @Test
  public void headRegion() throws Exception {
    // Test an unknown user - 401 error
    HttpResponse response = restClient.doHEAD("/" + REGION_NAME, "unknown-user", "1234567");
    assertEquals(401, GeodeRestClient.getCode(response));

    // Test a user with insufficient rights - 403
    response = restClient.doHEAD("/" + REGION_NAME, "stranger", "1234567");
    assertEquals(403, GeodeRestClient.getCode(response));

    // Test an authorized user - 200
    response = restClient.doHEAD("/" + REGION_NAME, "super-user", "1234567");
    assertEquals(200, GeodeRestClient.getCode(response));
  }

  /**
   * Test permissions on deleting a region
   */
  @Test
  public void deleteRegion() throws Exception {
    // Test an unknown user - 401 error
    HttpResponse response = restClient.doDelete("/" + REGION_NAME, "unknown-user", "1234567");
    assertEquals(401, GeodeRestClient.getCode(response));

    // Test a user with insufficient rights - 403
    response = restClient.doDelete("/" + REGION_NAME, "dataReader", "1234567");
    assertEquals(403, GeodeRestClient.getCode(response));
  }

  /**
   * Test permissions on getting a region's keys
   */
  @Test
  public void getRegionKeys() throws Exception {
    // Test an authorized user
    HttpResponse response = restClient.doGet("/" + REGION_NAME + "/keys", "super-user", "1234567");
    assertEquals(200, GeodeRestClient.getCode(response));
    assertThat(GeodeRestClient.getContentType(response))
        .containsIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE);
    // Test an unauthorized user
    response = restClient.doGet("/" + REGION_NAME + "/keys", "dataWriter", "1234567");
    assertEquals(403, GeodeRestClient.getCode(response));
  }

  /**
   * Test permissions on retrieving a key from a region
   */
  @Test
  public void getRegionKey() throws Exception {
    // Test an authorized user
    HttpResponse response = restClient.doGet("/" + REGION_NAME + "/key1", "key1User", "1234567");
    assertEquals(200, GeodeRestClient.getCode(response));
    assertThat(GeodeRestClient.getContentType(response))
        .containsIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE);

    // Test an unauthorized user
    response = restClient.doGet("/" + REGION_NAME + "/key1", "dataWriter", "1234567");
    assertEquals(403, GeodeRestClient.getCode(response));
  }

  /**
   * Test permissions on deleting a region's key(s)
   */
  @Test
  public void deleteRegionKey() throws Exception {
    // Test an unknown user - 401 error
    HttpResponse response =
        restClient.doDelete("/" + REGION_NAME + "/key1", "unknown-user", "1234567");
    assertEquals(401, GeodeRestClient.getCode(response));

    // Test a user with insufficient rights - 403
    response = restClient.doDelete("/" + REGION_NAME + "/key1", "dataReader", "1234567");
    assertEquals(403, GeodeRestClient.getCode(response));

    // Test an authorized user - 200
    response = restClient.doDelete("/" + REGION_NAME + "/key1", "key1User", "1234567");
    assertEquals(200, GeodeRestClient.getCode(response));
  }

  /**
   * Test permissions on deleting a region's key(s)
   */
  @Test
  public void postRegionKey() throws Exception {
    // Test an unknown user - 401 error
    HttpResponse response = restClient.doPost("/" + REGION_NAME + "?key9", "unknown", "1234567",
        "{ \"key9\" : \"foo\" }");
    assertEquals(401, GeodeRestClient.getCode(response));

    // Test a user with insufficient rights - 403
    response = restClient.doPost("/" + REGION_NAME + "?key9", "dataReader", "1234567",
        "{ \"key9\" : \"foo\" }");
    assertEquals(403, GeodeRestClient.getCode(response));

    // Test an authorized user - 200
    response = restClient.doPost("/" + REGION_NAME + "?key9", "dataWriter", "1234567",
        "{ \"key9\" : \"foo\" }");
    assertEquals(201, GeodeRestClient.getCode(response));
  }

  /**
   * Test permissions on deleting a region's key(s)
   */
  @Test
  public void putRegionKey() throws Exception {

    String json =
        "{\"@type\":\"com.gemstone.gemfire.web.rest.domain.Order\",\"purchaseOrderNo\":1121,\"customerId\":1012,\"description\":\"Order for  XYZ Corp\",\"orderDate\":\"02/10/2014\",\"deliveryDate\":\"02/20/2014\",\"contact\":\"Jelly Bean\",\"email\":\"jelly.bean@example.com\",\"phone\":\"01-2048096\",\"items\":[{\"itemNo\":1,\"description\":\"Product-100\",\"quantity\":12,\"unitPrice\":5,\"totalPrice\":60}],\"totalPrice\":225}";
    String casJSON =
        "{\"@old\":{\"@type\":\"com.gemstone.gemfire.web.rest.domain.Order\",\"purchaseOrderNo\":1121,\"customerId\":1012,\"description\":\"Order for  XYZ Corp\",\"orderDate\":\"02/10/2014\",\"deliveryDate\":\"02/20/2014\",\"contact\":\"Jelly Bean\",\"email\":\"jelly.bean@example.com\",\"phone\":\"01-2048096\",\"items\":[{\"itemNo\":1,\"description\":\"Product-100\",\"quantity\":12,\"unitPrice\":5,\"totalPrice\":60}],\"totalPrice\":225},\"@new \":{\"@type\":\"com.gemstone.gemfire.web.rest.domain.Order\",\"purchaseOrderNo\":1121,\"customerId\":1013,\"description\":\"Order for  New Corp\",\"orderDate\":\"02/10/2014\",\"deliveryDate\":\"02/25/2014\",\"contact\":\"Vanilla Bean\",\"email\":\"vanillabean@example.com\",\"phone\":\"01-2048096\",\"items\":[{\"itemNo\":12345,\"description\":\"part 123\",\"quantity\":12,\"unitPrice\":29.99,\"totalPrice\":149.95}],\"totalPrice\":149.95}}";
    // Test an unknown user - 401 error
    HttpResponse response = restClient.doPut("/" + REGION_NAME + "/key1?op=PUT", "unknown-user",
        "1234567", "{ \"key9\" : \"foo\" }");
    assertEquals(401, GeodeRestClient.getCode(response));

    response = restClient.doPut("/" + REGION_NAME + "/key1?op=CAS", "unknown-user", "1234567",
        "{ \"key9\" : \"foo\" }");
    assertEquals(401, GeodeRestClient.getCode(response));
    response = restClient.doPut("/" + REGION_NAME + "/key1?op=REPLACE", "unknown-user", "1234567",
        "{ \"@old\" : \"value1\", \"@new\" : \"CASvalue\" }");
    assertEquals(401, GeodeRestClient.getCode(response));

    response = restClient.doPut("/" + REGION_NAME + "/key1?op=PUT", "dataReader", "1234567",
        "{ \"key1\" : \"foo\" }");
    assertEquals(403, GeodeRestClient.getCode(response));

    response = restClient.doPut("/" + REGION_NAME + "/key1?op=REPLACE", "dataReader", "1234567",
        "{ \"key1\" : \"foo\" }");
    assertEquals(403, GeodeRestClient.getCode(response));

    response =
        restClient.doPut("/" + REGION_NAME + "/key1?op=CAS", "dataReader", "1234567", casJSON);
    assertEquals(403, GeodeRestClient.getCode(response));

    response = restClient.doPut("/" + REGION_NAME + "/key1?op=PUT", "key1User", "1234567",
        "{ \"key1\" : \"foo\" }");
    assertEquals(200, GeodeRestClient.getCode(response));

    response =
        restClient.doPut("/" + REGION_NAME + "/key1?op=REPLACE", "key1User", "1234567", json);
    assertEquals(200, GeodeRestClient.getCode(response));
  }
}
