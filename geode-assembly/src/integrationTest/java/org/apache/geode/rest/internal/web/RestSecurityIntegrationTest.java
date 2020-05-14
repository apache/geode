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

import static org.apache.geode.test.junit.rules.HttpResponseAssert.assertResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.http.MediaType;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class, RestAPITest.class})
public class RestSecurityIntegrationTest {

  protected static final String REGION_NAME = "AuthRegion";

  @ClassRule
  public static ServerStarterRule serverStarter = new ServerStarterRule()
      .withSecurityManager(SimpleSecurityManager.class).withRestService().withAutoStart();

  private static GeodeDevRestClient restClient;

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @BeforeClass
  public static void before() {
    serverStarter.getCache().createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
    restClient =
        new GeodeDevRestClient("localhost", serverStarter.getHttpPort());
  }

  @Test
  public void testListFunctions() {
    assertResponse(restClient.doGet("/functions", "user", "wrongPswd")).hasStatusCode(401);
    assertResponse(restClient.doGet("/functions", "user", "user")).hasStatusCode(403);
    assertResponse(restClient.doGet("/functions", "dataRead", "dataRead"))
        .hasStatusCode(200)
        .hasContentType(MediaType.APPLICATION_JSON_VALUE);
  }

  @Test
  public void executeNotRegisteredFunction() {
    assertResponse(restClient.doPost("/functions/invalid-function-id", "user", "wrongPswd", ""))
        .hasStatusCode(401);
    assertResponse(restClient.doPost("/functions/invalid-function-id", "user", "user", ""))
        .hasStatusCode(404);
  }

  @Test
  public void testQueries() {
    restClient.doGetAndAssert("/queries", "user", "wrongPswd")
        .hasStatusCode(401);
    restClient.doGetAndAssert("/queries", "user", "user")
        .hasStatusCode(403);
    restClient.doGetAndAssert("/queries", "dataRead", "dataRead")
        .hasStatusCode(200)
        .hasContentType(MediaType.APPLICATION_JSON_VALUE);
  }

  @Test
  public void testAdhocQuery() {
    restClient.doGetAndAssert("/queries/adhoc?q=", "user", "wrongPswd")
        .hasStatusCode(401);
    restClient.doGetAndAssert("/queries/adhoc?q=", "user", "user")
        .hasStatusCode(403);
    // because we're only testing the security of the endpoint, not the endpoint functionality, a
    // 500 is acceptable
    restClient.doGetAndAssert("/queries/adhoc?q=", "dataRead", "dataRead")
        .hasStatusCode(500);
  }

  @Test
  public void testPostQuery() {
    assertResponse(restClient.doPost("/queries?id=0&q=", "user", "wrongPswd", ""))
        .hasStatusCode(401);
    assertResponse(restClient.doPost("/queries?id=0&q=", "user", "user", ""))
        .hasStatusCode(403);
    assertResponse(restClient.doPost("/queries?id=0&q=", "dataRead", "dataRead", ""))
        .hasStatusCode(500);
  }

  @Test
  public void testPostQuery2() {
    assertResponse(restClient.doPost("/queries/id", "user", "wrongPswd", "{\"id\" : \"foo\"}"))
        .hasStatusCode(401);
    assertResponse(restClient.doPost("/queries/id", "user", "user", "{\"id\" : \"foo\"}"))
        .hasStatusCode(403);
    assertResponse(restClient.doPost("/queries/id", "dataRead", "dataRead", "{\"id\" : \"foo\"}"))
        .hasStatusCode(404);
  }

  @Test
  public void testPutQuery() {
    assertResponse(restClient.doPut("/queries/id", "user", "wrongPswd", "{\"id\" : \"foo\"}"))
        .hasStatusCode(401);
    assertResponse(restClient.doPut("/queries/id", "user", "user", "{\"id\" : \"foo\"}"))
        .hasStatusCode(403);
    assertResponse(restClient.doPut("/queries/id", "dataRead", "dataRead", "{\"id\" : \"foo\"}"))
        .hasStatusCode(404);
  }

  @Test
  public void testDeleteQuery() {
    assertResponse(restClient.doDelete("/queries/id", "user", "wrongPswd"))
        .hasStatusCode(401);
    assertResponse(restClient.doDelete("/queries/id", "stranger", "stranger"))
        .hasStatusCode(403);
    assertResponse(restClient.doDelete("/queries/id", "dataWrite", "dataWrite"))
        .hasStatusCode(404);
  }

  @Test
  public void testServers() {
    assertResponse(restClient.doGet("/servers", "user", "wrongPswd"))
        .hasStatusCode(401);
    assertResponse(restClient.doGet("/servers", "stranger", "stranger"))
        .hasStatusCode(403);
    assertResponse(restClient.doGet("/servers", "cluster", "cluster"))
        .hasStatusCode(200)
        .hasContentType(MediaType.APPLICATION_JSON_VALUE);
  }

  /**
   * This test should always return an OK, whether the user is known or unknown. A phishing script
   * should not be able to determine whether a user/password combination is good
   */
  @Test
  public void testPing() {
    assertResponse(restClient.doHEAD("/ping", "stranger", "stranger"))
        .hasStatusCode(200);
    assertResponse(restClient.doGet("/ping", "stranger", "stranger"))
        .hasStatusCode(200);

    assertResponse(restClient.doHEAD("/ping", "data", "data"))
        .hasStatusCode(200);
    assertResponse(restClient.doGet("/ping", "data", "data"))
        .hasStatusCode(200);
  }

  /**
   * Test permissions on retrieving a list of regions.
   */
  @Test
  public void getRegions() throws IOException {
    JsonNode jsonObject = assertResponse(restClient.doGet("", "dataRead", "dataRead"))
        .hasStatusCode(200).hasContentType(MediaType.APPLICATION_JSON_VALUE)
        .getJsonObject();

    JsonNode regions = jsonObject.get("regions");
    assertNotNull(regions);
    assertTrue(regions.size() > 0);

    JsonNode region = regions.get(0);
    assertEquals("AuthRegion", region.get("name").asText());
    assertEquals("REPLICATE", region.get("type").asText());

    // List regions with an unknown user - 401
    assertResponse(restClient.doGet("", "user", "wrongPswd"))
        .hasStatusCode(401);

    // list regions with insufficent rights - 403
    assertResponse(restClient.doGet("", "dataReadAuthRegion", "dataReadAuthRegion"))
        .hasStatusCode(403);
  }

  /**
   * Test permissions on getting a region
   */
  @Test
  public void getRegion() {
    // Test an unknown user - 401 error
    assertResponse(restClient.doGet("/" + REGION_NAME, "user", "wrongPswd"))
        .hasStatusCode(401);

    // Test a user with insufficient rights - 403
    assertResponse(restClient.doGet("/" + REGION_NAME, "stranger", "stranger"))
        .hasStatusCode(403);

    // Test an authorized user - 200
    assertResponse(restClient.doGet("/" + REGION_NAME, "data", "data"))
        .hasStatusCode(200).hasContentType(MediaType.APPLICATION_JSON_VALUE);
  }

  /**
   * Test permissions on HEAD region
   */
  @Test
  public void headRegion() {
    // Test an unknown user - 401 error
    assertResponse(restClient.doHEAD("/" + REGION_NAME, "user", "wrongPswd"))
        .hasStatusCode(401);

    // Test a user with insufficient rights - 403
    assertResponse(restClient.doHEAD("/" + REGION_NAME, "stranger", "stranger"))
        .hasStatusCode(403);

    // Test an authorized user - 200
    assertResponse(restClient.doHEAD("/" + REGION_NAME, "data", "data"))
        .hasStatusCode(200);
  }

  /**
   * Test permissions on deleting a region
   */
  @Test
  public void deleteRegion() {
    // Test an unknown user - 401 error
    assertResponse(restClient.doDelete("/" + REGION_NAME, "user", "wrongPswd"))
        .hasStatusCode(401);

    // Test a user with insufficient rights - 403
    assertResponse(restClient.doDelete("/" + REGION_NAME, "dataRead", "dataRead"))
        .hasStatusCode(403);
  }

  /**
   * Test permissions on getting a region's keys
   */
  @Test
  public void getRegionKeys() {
    // Test an authorized user
    assertResponse(restClient.doGet("/" + REGION_NAME + "/keys", "data", "data"))
        .hasStatusCode(200).hasContentType(MediaType.APPLICATION_JSON_VALUE);
    // Test an unauthorized user
    assertResponse(restClient.doGet("/" + REGION_NAME + "/keys", "dataWrite", "dataWrite"))
        .hasStatusCode(403);
  }

  /**
   * Test permissions on retrieving a key from a region
   */
  @Test
  public void getRegionKey() {
    // Test an authorized user
    assertResponse(restClient.doGet("/" + REGION_NAME + "/key1", "dataReadAuthRegionKey1",
        "dataReadAuthRegionKey1"))
            .hasStatusCode(200).hasContentType(MediaType.APPLICATION_JSON_VALUE);

    // Test an unauthorized user
    assertResponse(restClient.doGet("/" + REGION_NAME + "/key1", "dataWrite", "dataWrite"))
        .hasStatusCode(403);
  }

  /**
   * Test permissions on deleting a region's key(s)
   */
  @Test
  public void deleteRegionKey() {
    // Test an unknown user - 401 error
    assertResponse(restClient.doDelete("/" + REGION_NAME + "/key1", "user", "wrongPswd"))
        .hasStatusCode(401);

    // Test a user with insufficient rights - 403
    assertResponse(restClient.doDelete("/" + REGION_NAME + "/key1", "dataRead", "dataRead"))
        .hasStatusCode(403);

    // Test an authorized user - 200
    assertResponse(restClient.doDelete("/" + REGION_NAME + "/key1", "dataWriteAuthRegionKey1",
        "dataWriteAuthRegionKey1"))
            .hasStatusCode(200);
  }

  /**
   * Test permissions on deleting a region's key(s)
   */
  @Test
  public void postRegionKey() {
    // Test an unknown user - 401 error
    assertResponse(restClient.doPost("/" + REGION_NAME + "?key9", "user", "wrongPswd",
        "{ \"key9\" : \"foo\" }"))
            .hasStatusCode(401);

    // Test a user with insufficient rights - 403
    assertResponse(restClient.doPost("/" + REGION_NAME + "?key9", "dataRead", "dataRead",
        "{ \"key9\" : \"foo\" }"))
            .hasStatusCode(403);

    // Test an authorized user - 201
    assertResponse(restClient.doPost("/" + REGION_NAME + "?key9", "dataWrite", "dataWrite",
        "{ \"key9\" : \"foo\" }"))
            .hasStatusCode(201);
  }

  /**
   * Test permissions on deleting a region's key(s)
   */
  @Test
  public void putRegionKey() {

    String json =
        "{\"@type\":\"com.gemstone.gemfire.web.rest.domain.Order\",\"purchaseOrderNo\":1121,\"customerId\":1012,\"description\":\"Order for  XYZ Corp\",\"orderDate\":\"02/10/2014\",\"deliveryDate\":\"02/20/2014\",\"contact\":\"Jelly Bean\",\"email\":\"jelly.bean@example.com\",\"phone\":\"01-2048096\",\"items\":[{\"itemNo\":1,\"description\":\"Product-100\",\"quantity\":12,\"unitPrice\":5,\"totalPrice\":60}],\"totalPrice\":225}";
    String casJSON =
        "{\"@old\":{\"@type\":\"com.gemstone.gemfire.web.rest.domain.Order\",\"purchaseOrderNo\":1121,\"customerId\":1012,\"description\":\"Order for  XYZ Corp\",\"orderDate\":\"02/10/2014\",\"deliveryDate\":\"02/20/2014\",\"contact\":\"Jelly Bean\",\"email\":\"jelly.bean@example.com\",\"phone\":\"01-2048096\",\"items\":[{\"itemNo\":1,\"description\":\"Product-100\",\"quantity\":12,\"unitPrice\":5,\"totalPrice\":60}],\"totalPrice\":225},\"@new \":{\"@type\":\"com.gemstone.gemfire.web.rest.domain.Order\",\"purchaseOrderNo\":1121,\"customerId\":1013,\"description\":\"Order for  New Corp\",\"orderDate\":\"02/10/2014\",\"deliveryDate\":\"02/25/2014\",\"contact\":\"Vanilla Bean\",\"email\":\"vanillabean@example.com\",\"phone\":\"01-2048096\",\"items\":[{\"itemNo\":12345,\"description\":\"part 123\",\"quantity\":12,\"unitPrice\":29.99,\"totalPrice\":149.95}],\"totalPrice\":149.95}}";
    // Test an unknown user - 401 error
    assertResponse(restClient.doPut("/" + REGION_NAME + "/key1?op=PUT", "user", "wrongPswd",
        "{ \"key9\" : \"foo\" }"))
            .hasStatusCode(401);

    assertResponse(restClient.doPut("/" + REGION_NAME + "/key1?op=CAS", "user", "wrongPswd",
        "{ \"key9\" : \"foo\" }"))
            .hasStatusCode(401);
    assertResponse(restClient.doPut("/" + REGION_NAME + "/key1?op=REPLACE", "user", "wrongPswd",
        "{ \"@old\" : \"value1\", \"@new\" : \"CASvalue\" }"))
            .hasStatusCode(401);

    assertResponse(restClient.doPut("/" + REGION_NAME + "/key1?op=PUT", "dataRead", "dataRead",
        "{ \"key1\" : \"foo\" }"))
            .hasStatusCode(403);

    assertResponse(restClient.doPut("/" + REGION_NAME + "/key1?op=REPLACE", "dataRead", "dataRead",
        "{ \"key1\" : \"foo\" }"))
            .hasStatusCode(403);

    assertResponse(
        restClient.doPut("/" + REGION_NAME + "/key1?op=CAS", "dataRead", "dataRead", casJSON))
            .hasStatusCode(403);

    assertResponse(restClient.doPut("/" + REGION_NAME + "/key1?op=PUT", "dataWriteAuthRegionKey1",
        "dataWriteAuthRegionKey1", "{ \"key1\" : \"foo\" }"))
            .hasStatusCode(200);

    assertResponse(restClient.doPut("/" + REGION_NAME + "/key1?op=REPLACE",
        "dataWriteAuthRegionKey1", "dataWriteAuthRegionKey1", json))
            .hasStatusCode(200);
  }
}
