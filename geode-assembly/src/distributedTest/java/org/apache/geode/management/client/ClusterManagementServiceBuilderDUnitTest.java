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

package org.apache.geode.management.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.management.api.ConnectionConfig;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransport;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.MemberStarterRule;

public class ClusterManagementServiceBuilderDUnitTest {

  private static int locatorPort = -1;

  private static ConnectionConfig connectionConfig;

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(1);

  @BeforeClass
  public static void beforeClass() {

    MemberVM locator = cluster.startLocatorVM(0, MemberStarterRule::withHttpService);
    locatorPort = locator.getHttpPort();
    connectionConfig = new ConnectionConfig("localhost", locatorPort);
  }



  @Test
  public void buildWithTransportOnlyHavingConnectionConfig() {
    assertThat(new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(connectionConfig)).build().isConnected())
            .isTrue();
  }

  @Test
  public void buildWithPortOnly() {
    assertThat(new ClusterManagementServiceBuilder().setPort(locatorPort).build().isConnected())
        .isTrue();
  }

  @Test
  public void buildWithHostnameOnly() {
    assertThat(new ClusterManagementServiceBuilder().setHost("localhost").setPort(locatorPort)
        .build().isConnected())
            .isTrue();
  }

  @Test
  public void buildWithTransportOnlyHavingRestTemplate() {
    assertThatThrownBy(() -> new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(new RestTemplate())).build()
        .isConnected())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("URI is not absolute");
  }

  @Test
  public void buildWithTransportOnlyHavingRestTemplateAndConnectionConfig() {
    assertThat(new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(new RestTemplate(), connectionConfig))
        .build()
        .isConnected()).isTrue();
  }

  @Test
  public void buildWithTransportHavingRestTemplateAndConnectionConfig() {
    assertThat(new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(new RestTemplate(), connectionConfig))
        .build().isConnected())
            .isTrue();
  }
}
