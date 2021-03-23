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
import static org.apache.geode.test.junit.rules.HttpResponseAssert.assertResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpStatus;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.membership.utils.AvailablePort;
import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({RestAPITest.class})
public class RestServersIntegrationTest {

  private static GeodeDevRestClient restClient;

  @ClassRule
  public static ServerStarterRule serverStarter = new ServerStarterRule().withRestService(true);


  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @BeforeClass
  public static void before() throws Exception {
    assumeTrue(
        "Default port was unavailable for testing.  Please ensure the testing environment is clean.",
        AvailablePort.isPortAvailable(DEFAULT_HTTP_SERVICE_PORT, AvailablePort.SOCKET));
    serverStarter.startServer();
    assertThat(serverStarter.getHttpPort()).isEqualTo(DEFAULT_HTTP_SERVICE_PORT);
    restClient = new GeodeDevRestClient("localhost", serverStarter.getHttpPort());
  }

  @Test
  public void testGet() throws Exception {
    assertResponse(restClient.doGet("/", null, null))
        .hasStatusCode(HttpStatus.SC_OK);
  }

  @Test
  public void testGetOnInternalRegion() {
    assertResponse(
        restClient.doGet("/__PR", null, null))
            .hasStatusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void testServerStartedOnDefaultPort() throws Exception {
    JsonNode jsonArray = assertResponse(restClient.doGet("/servers", null, null))
        .hasStatusCode(HttpStatus.SC_OK).getJsonObject();
    assertThat(jsonArray.size()).isEqualTo(1);
    assertThat(jsonArray.get(0).asText())
        .isEqualTo("http://localhost:" + DEFAULT_HTTP_SERVICE_PORT);
  }
}
