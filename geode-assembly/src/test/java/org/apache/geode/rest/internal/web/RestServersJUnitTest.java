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
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;

import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.http.HttpResponse;
import org.json.JSONArray;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category({IntegrationTest.class, RestAPITest.class})
public class RestServersJUnitTest {

  private static int defaultPort = 7070;
  static Properties properties = new Properties() {
    {
      setProperty(START_DEV_REST_API, "true");
      setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");
    }
  };

  @ClassRule
  public static ServerStarterRule serverStarter = new ServerStarterRule();
  private static GeodeRestClient restClient;

  @BeforeClass
  public static void before() throws Exception {
    serverStarter.startServer(properties);
    restClient = new GeodeRestClient("localhost", defaultPort);
  }

  @Test
  public void testDefaultPort() throws Exception {
    // make sure the server is started on the default port and we can connect using the default port
    HttpResponse response = restClient.doGet("/", null, null);
    Assert.assertEquals(200, GeodeRestClient.getCode(response));
  }

  @Test
  public void testServers() throws Exception {
    HttpResponse response = restClient.doGet("/servers", null, null);
    JSONArray body = GeodeRestClient.getJsonArray(response);
    Assert.assertEquals(1, body.length());
    Assert.assertEquals("http://localhost:7070", body.getString(0));
  }
}
