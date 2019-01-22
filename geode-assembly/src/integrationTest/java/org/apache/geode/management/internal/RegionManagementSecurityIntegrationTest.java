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

package org.apache.geode.management.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.internal.api.ClusterManagementResult;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

public class RegionManagementSecurityIntegrationTest {

  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule().withHttpService()
      .withSecurityManager(SimpleTestSecurityManager.class).withAutoStart();

  @ClassRule
  public static RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  public static GeodeDevRestClient restClient;

  @BeforeClass
  public static void setUpClass() {
    restClient =
        new GeodeDevRestClient("/geode-management/v2", "localhost", locator.getHttpPort(), false);
  }

  private RegionConfig regionConfig;
  private String json;

  @Before
  public void before() throws JsonProcessingException {
    regionConfig = new RegionConfig();
    regionConfig.setName("customers");
    regionConfig.setRefid("REPLICATE");
    ObjectMapper mapper = new ObjectMapper();
    json = mapper.writeValueAsString(regionConfig);
  }

  @Test
  public void sanityCheck_not_authorized() throws Exception {
    ClusterManagementResult result =
        restClient.doPostAndAssert("/regions", json, "test", "test")
            .hasStatusCode(403)
            .getClusterManagementResult();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getPersistenceStatus().getMessage()).isEqualTo("Access is denied");
  }

  @Test
  public void sanityCheckWithNoCredentials() throws Exception {
    restClient.doPostAndAssert("/regions", json, null, null)
        .hasStatusCode(401);
  }

  @Test
  public void sanityCheckWithWrongCredentials() throws Exception {
    restClient.doPostAndAssert("/regions", json, "test", "invalid_pswd")
        .hasStatusCode(401);
  }

  @Test
  public void sanityCheck_success() throws Exception {
    ClusterManagementResult result =
        restClient.doPostAndAssert("/regions", json, "dataManage", "dataManage")
            .hasStatusCode(500)
            .getClusterManagementResult();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getPersistenceStatus().getMessage())
        .isEqualTo("no members found to create cache element");
  }
}
