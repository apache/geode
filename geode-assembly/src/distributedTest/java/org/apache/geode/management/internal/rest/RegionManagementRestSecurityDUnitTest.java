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
package org.apache.geode.management.internal.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;

public class RegionManagementRestSecurityDUnitTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator, server;

  private static GeodeDevRestClient restClient;

  private static Properties config;

  private static String json;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService()
        .withSecurityManager(SimpleSecurityManager.class));

    config = new Properties();
    config.setProperty("security-username", "cluster");
    config.setProperty("security-password", "cluster");

    server = cluster.startServerVM(1, config, locator.getPort());
    restClient =
        new GeodeDevRestClient("/geode-management/v2", "localhost", locator.getHttpPort(), false);

    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("customers");
    regionConfig.setType(RegionType.REPLICATE);
    ObjectMapper mapper = new ObjectMapper();
    json = mapper.writeValueAsString(regionConfig);
  }

  @Test
  public void createRegionWithoutCredentials_failsWithAuthenticationError() throws Exception {
    ClusterManagementResult<?> result =
        restClient.doPostAndAssert("/regions", json)
            .hasStatusCode(401)
            .getClusterManagementResult();

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode())
        .isEqualTo(ClusterManagementResult.StatusCode.UNAUTHENTICATED);
    assertThat(result.getStatusMessage()).contains("authentication is required");
  }

  @Test
  public void createRegionWithBadCredentials_failsWithAuthenticationError() throws Exception {
    ClusterManagementResult<?> result =
        restClient.doPostAndAssert("/regions", json, "baduser", "badpassword")
            .hasStatusCode(401)
            .getClusterManagementResult();

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode())
        .isEqualTo(ClusterManagementResult.StatusCode.UNAUTHENTICATED);
    assertThat(result.getStatusMessage()).contains("Authentication error");
  }

  @Test
  public void createRegionNotAuthorized_failsWithAuthorizationError() throws Exception {
    ClusterManagementResult<?> result =
        restClient.doPostAndAssert("/regions", json, "notauthorized", "notauthorized")
            .hasStatusCode(403)
            .getClusterManagementResult();

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.UNAUTHORIZED);
    assertThat(result.getStatusMessage()).contains("not authorized for DATA:MANAGE");
  }

  @Test
  public void createRegionWithCredentials_CreatesRegion() throws Exception {
    ClusterManagementResult<?> result =
        restClient.doPostAndAssert("/regions", json, "datamanage", "datamanage")
            .hasStatusCode(201)
            .getClusterManagementResult();

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getMemberStatuses()).containsKeys("server-1").hasSize(1);

    // make sure region is created
    server.invoke(() -> RegionManagementDunitTest.verifyRegionCreated("customers", "REPLICATE"));

    // make sure region is persisted
    locator.invoke(() -> RegionManagementDunitTest.verifyRegionPersisted("customers", "REPLICATE",
        "cluster"));

    // verify that additional server can be started with the cluster configuration
    cluster.startServerVM(2, config, locator.getPort());
  }
}
