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

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.apache.geode.rest.internal.web.GeodeRestClient.getCode;
import static org.apache.geode.rest.internal.web.GeodeRestClient.getContentType;
import static org.apache.geode.rest.internal.web.GeodeRestClient.getJsonObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.io.IOUtils;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.templates.SamplePostProcessor;
import org.apache.geode.security.templates.SampleSecurityManager;
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

import java.util.Properties;


@Category({IntegrationTest.class, SecurityTest.class})
public class RestSecurityPostProcessorTest {

  static final String REGION_NAME = "AuthRegion";

  static int restPort = AvailablePortHelper.getRandomAvailableTCPPort();
  static Properties properties = new Properties() {
    {
      setProperty(SampleSecurityManager.SECURITY_JSON,
          "org/apache/geode/management/internal/security/clientServer.json");
      setProperty(SECURITY_MANAGER, SampleSecurityManager.class.getName());
      setProperty(START_DEV_REST_API, "true");
      setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");
      setProperty(HTTP_SERVICE_PORT, restPort + "");
      setProperty(SECURITY_POST_PROCESSOR, SamplePostProcessor.class.getName());
    }
  };

  @ClassRule
  public static ServerStarterRule serverStarter = new ServerStarterRule(properties);
  private final GeodeRestClient restClient = new GeodeRestClient("localhost", restPort);

  @BeforeClass
  public static void before() throws Exception {
    serverStarter.startServer();
    Region region =
        serverStarter.cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
    region.put("key1",
        "{\"@type\":\"com.gemstone.gemfire.web.rest.domain.Order\",\"purchaseOrderNo\":1121,\"customerId\":1012,\"description\":\"Order for  XYZ Corp\",\"orderDate\":\"02/10/2014\",\"deliveryDate\":\"02/20/2014\",\"contact\":\"Jelly Bean\",\"email\":\"jelly.bean@example.com\",\"phone\":\"01-2048096\",\"items\":[{\"itemNo\":1,\"description\":\"Product-100\",\"quantity\":12,\"unitPrice\":5,\"totalPrice\":60}],\"totalPrice\":225}");
    region.put("key2", "bar");
  }

  /**
   * Test post-processing of a retrieved key from the server.
   */
  @Test
  public void getRegionKey() throws Exception {

    // Test a single key
    HttpResponse response = restClient.doGet("/" + REGION_NAME + "/key1", "key1User", "1234567");
    assertEquals(200, getCode(response));
    assertEquals(MediaType.APPLICATION_JSON_UTF8_VALUE, getContentType(response));

    String body = IOUtils.toString(response.getEntity().getContent(), "UTF-8");
    assertTrue(body.startsWith("\"key1User/" + REGION_NAME + "/key1/"));

    // Test multiple keys
    response = restClient.doGet("/" + REGION_NAME + "/key1,key2", "dataReader", "1234567");
    assertEquals(200, getCode(response));
    assertEquals(MediaType.APPLICATION_JSON_UTF8_VALUE, getContentType(response));

    JSONObject jsonObject = getJsonObject(response);
    JSONArray jsonArray = jsonObject.getJSONArray(REGION_NAME);
    final int length = jsonArray.length();
    for (int index = 0; index < length; ++index) {
      String data = jsonArray.getString(index);
      assertTrue(data.contains("dataReader/" + REGION_NAME + "/"));
    }
  }

  @Test
  public void getRegion() throws Exception {
    HttpResponse response = restClient.doGet("/" + REGION_NAME, "dataReader", "1234567");
    assertEquals(200, getCode(response));
    assertEquals(MediaType.APPLICATION_JSON_UTF8_VALUE, getContentType(response));

    JSONObject jsonObject = getJsonObject(response);
    JSONArray jsonArray = jsonObject.getJSONArray(REGION_NAME);
    final int length = jsonArray.length();
    for (int index = 0; index < length; ++index) {
      String data = jsonArray.getString(index);
      assertTrue(data.contains("dataReader/" + REGION_NAME + "/"));
    }
  }
}
