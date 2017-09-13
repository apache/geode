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

import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_HTTP_SERVICE_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.json.JSONArray;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.AvailablePort;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.RestAPITest;

@Category({IntegrationTest.class, RestAPITest.class})
public class RestServersJUnitTest {

  @ClassRule
  public static ServerStarterRule serverStarter = new ServerStarterRule().withRestService(true);


  private static GeodeRestClient restClient;

  @BeforeClass
  public static void before() throws Exception {
    assumeTrue(
        "Default port was unavailable for testing.  Please ensure the testing environment is clean.",
        AvailablePort.isPortAvailable(DEFAULT_HTTP_SERVICE_PORT, AvailablePort.SOCKET));
    serverStarter.startServer();
    assertThat(serverStarter.getHttpPort()).isEqualTo(DEFAULT_HTTP_SERVICE_PORT);
    restClient = new GeodeRestClient("localhost", serverStarter.getHttpPort());
  }

  @Test
  public void testGet() throws Exception {
    HttpResponse response = restClient.doGet("/", null, null);
    assertThat(GeodeRestClient.getCode(response)).isEqualTo(HttpStatus.SC_OK);
  }

  @Test
  public void testServerStartedOnDefaultPort() throws Exception {
    HttpResponse response = restClient.doGet("/servers", null, null);
    assertThat(GeodeRestClient.getCode(response)).isEqualTo(HttpStatus.SC_OK);
    JSONArray body = GeodeRestClient.getJsonArray(response);
    assertThat(body.length()).isEqualTo(1);
    assertThat(body.getString(0)).isEqualTo("http://localhost:" + DEFAULT_HTTP_SERVICE_PORT);
  }
}
