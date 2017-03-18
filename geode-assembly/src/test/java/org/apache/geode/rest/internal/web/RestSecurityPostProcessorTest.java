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
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.rest.internal.web.GeodeRestClient.getCode;
import static org.apache.geode.rest.internal.web.GeodeRestClient.getJsonArray;
import static org.apache.geode.rest.internal.web.GeodeRestClient.getJsonObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.rest.internal.web.controllers.Customer;
import org.apache.geode.rest.internal.web.controllers.RedactingPostProcessor;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.http.HttpResponse;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.http.MediaType;

import java.net.URLEncoder;


@Category({IntegrationTest.class, SecurityTest.class})
public class RestSecurityPostProcessorTest {

  @ClassRule
  public static ServerStarterRule serverStarter = new ServerStarterRule()
      .withProperty(TestSecurityManager.SECURITY_JSON,
          "org/apache/geode/management/internal/security/clientServer.json")
      .withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
      .withProperty(SECURITY_POST_PROCESSOR, RedactingPostProcessor.class.getName())
      .withRestService().startServer();

  private final GeodeRestClient restClient =
      new GeodeRestClient("localhost", serverStarter.getHttpPort());

  @BeforeClass
  public static void before() throws Exception {
    Region region =
        serverStarter.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("customers");
    region.put("1", new Customer(1L, "John", "Doe", "555555555"));
    region.put("2", new Customer(2L, "Richard", "Roe", "222533554"));
    region.put("3", new Customer(3L, "Jane", "Doe", "555223333"));
    region.put("4", new Customer(4L, "Jane", "Roe", "555443333"));
  }

  /**
   * Test post-processing of a retrieved key from the server.
   */
  @Test
  public void getRegionKey() throws Exception {
    // Test a single key
    HttpResponse response = restClient.doGet("/customers/1", "dataReader", "1234567");
    assertEquals(200, getCode(response));
    assertThat(GeodeRestClient.getContentType(response))
        .containsIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE);

    // Ensure SSN is hidden
    JSONObject jsonObject = getJsonObject(response);
    assertEquals("*********", jsonObject.getString("socialSecurityNumber"));
    assertEquals(1L, jsonObject.getLong("customerId"));

    // Try with super-user
    response = restClient.doGet("/customers/1", "super-user", "1234567");
    assertEquals(200, getCode(response));
    assertThat(GeodeRestClient.getContentType(response))
        .containsIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE);

    // ensure SSN is readable
    jsonObject = getJsonObject(response);
    assertEquals("555555555", jsonObject.getString("socialSecurityNumber"));
    assertEquals(1L, jsonObject.getLong("customerId"));
  }

  // Test multiple keys
  @Test
  public void getMultipleRegionKeys() throws Exception {
    HttpResponse response = restClient.doGet("/customers/1,3", "dataReader", "1234567");
    assertEquals(200, getCode(response));
    assertThat(GeodeRestClient.getContentType(response))
        .containsIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE);

    JSONObject jsonObject = getJsonObject(response);
    JSONArray jsonArray = jsonObject.getJSONArray("customers");
    final int length = jsonArray.length();
    assertEquals(2, length);
    JSONObject customer = jsonArray.getJSONObject(0);
    assertEquals("*********", customer.getString("socialSecurityNumber"));
    assertEquals(1, customer.getLong("customerId"));
    customer = jsonArray.getJSONObject(1);
    assertEquals("*********", customer.getString("socialSecurityNumber"));
    assertEquals(3, customer.getLong("customerId"));
  }

  @Test
  public void getRegion() throws Exception {
    HttpResponse response = restClient.doGet("/customers", "dataReader", "1234567");
    assertEquals(200, getCode(response));
    assertThat(GeodeRestClient.getContentType(response))
        .containsIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE);

    JSONObject jsonObject = getJsonObject(response);
    JSONArray jsonArray = jsonObject.getJSONArray("customers");
    final int length = jsonArray.length();
    for (int index = 0; index < length; ++index) {
      JSONObject customer = jsonArray.getJSONObject(index);
      assertEquals("*********", customer.getString("socialSecurityNumber"));
      assertEquals((long) index + 1, customer.getLong("customerId"));
    }
  }

  @Test
  public void adhocQuery() throws Exception {
    String query = "/queries/adhoc?q="
        + URLEncoder.encode("SELECT * FROM /customers order by customerId", "UTF-8");
    HttpResponse response = restClient.doGet(query, "dataReader", "1234567");
    assertEquals(200, getCode(response));
    assertThat(GeodeRestClient.getContentType(response))
        .containsIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE);

    JSONArray jsonArray = getJsonArray(response);
    final int length = jsonArray.length();
    for (int index = 0; index < length; ++index) {
      JSONObject customer = jsonArray.getJSONObject(index);
      assertEquals("*********", customer.getString("socialSecurityNumber"));
      assertEquals((long) index + 1, customer.getLong("customerId"));
    }
  }

  @Test
  public void namedQuery() throws Exception {
    // Declare the named query
    String namedQuery = "SELECT c FROM /customers c WHERE c.customerId = $1";

    // Install the named query
    HttpResponse response =
        restClient.doPost("/queries?id=selectCustomer&q=" + URLEncoder.encode(namedQuery, "UTF-8"),
            "dataReader", "1234567", "");
    assertEquals(201, getCode(response));

    // Verify the query has been installed
    String query = "/queries";
    response = restClient.doGet(query, "dataReader", "1234567");
    assertEquals(200, getCode(response));
    assertThat(GeodeRestClient.getContentType(response))
        .containsIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE);

    // Execute the query
    response = restClient.doPost("/queries/selectCustomer", "dataReader", "1234567",
        "{" + "\"@type\": \"int\"," + "\"@value\": 1" + "}");
    assertEquals(200, getCode(response));

    // Validate the result
    JSONArray jsonArray = getJsonArray(response);
    assertTrue(jsonArray.length() == 1);
    JSONObject customer = jsonArray.getJSONObject(0);
    assertEquals("*********", customer.getString("socialSecurityNumber"));
    assertEquals(1L, customer.getLong("customerId"));
  }
}
