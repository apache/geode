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

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

public class DisabledClusterConfigTest {
  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule();

  @ClassRule
  public static RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @Test
  public void disabledClusterConfig() throws Exception {
    locator.withProperty(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false")
        .withHttpService().startLocator();
    GeodeDevRestClient restClient =
        new GeodeDevRestClient("/geode-management/v2", "localhost", locator.getHttpPort(), false);

    ClusterManagementResult<?> result =
        restClient.doPostAndAssert("/regions", "{\"name\":\"test\"}")
            .hasStatusCode(500)
            .getClusterManagementResult();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage())
        .isEqualTo("Cluster configuration service needs to be enabled");
  }
}
