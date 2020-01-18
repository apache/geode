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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.management.api.BaseConnectionConfig;
import org.apache.geode.management.api.ConnectionConfig;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransport;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.MemberStarterRule;

public class ClusterManagementServiceBuilderDUnitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(1);

  @BeforeClass
  public static void beforeClass() {

    MemberVM locator = cluster.startLocatorVM(0, MemberStarterRule::withHttpService);
    connectionConfig = new BaseConnectionConfig("localhost", locator.getHttpPort());
  }

  private static ConnectionConfig connectionConfig;

  @Test
  public void buildWithTransportOnlyHavingConnectionConfig() {
    assertThat(new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(connectionConfig)).build().isConnected())
            .isTrue();
  }

  @Test
  public void buildWithTransportOnlyHavingRestTemplate() {
    assertThat(new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(new RestTemplate())).build()
        .isConnected()).isFalse();
  }

  @Test
  public void buildWithTransportOnlyHavingRestTemplateAndConnectionConfig() {
    assertThat(new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(new RestTemplate(), connectionConfig))
        .build().isConnected()).isTrue();
  }

  @Test
  public void buildWithConnectionConfigOnly() {
    assertThat(new ClusterManagementServiceBuilder()
        .setConnectionConfig(connectionConfig).build().isConnected()).isTrue();
  }

  @Test
  public void buildWithTransportHavingRestTemplateAndConnectionConfig() {
    assertThat(new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(new RestTemplate()))
        .setConnectionConfig(connectionConfig).build().isConnected()).isTrue();
  }

  @Test
  public void buildWithTransportHavingConnectionConfigAndConnectionConfig() {
    assertThat(new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(connectionConfig))
        .setConnectionConfig(connectionConfig).build().isConnected()).isTrue();
  }

  @Test
  public void buildWithTransportHavingConnectionConfigAndRestTemplateAndConnectionConfig() {
    assertThat(new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(new RestTemplate(), connectionConfig))
        .setConnectionConfig(connectionConfig).build().isConnected()).isTrue();
  }

  @Test(expected = IllegalStateException.class)
  public void buildWithNothing() {
    new ClusterManagementServiceBuilder().build();
  }
}
