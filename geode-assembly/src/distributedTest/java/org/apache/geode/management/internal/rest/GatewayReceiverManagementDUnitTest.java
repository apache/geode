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

import static org.apache.geode.test.junit.assertions.ClusterManagementGetResultAssert.assertManagementGetResult;
import static org.apache.geode.test.junit.assertions.ClusterManagementListResultAssert.assertManagementListResult;
import static org.apache.geode.test.junit.assertions.ClusterManagementRealizationResultAssert.assertManagementResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.EntityGroupInfo;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.GatewayReceiver;
import org.apache.geode.management.runtime.GatewayReceiverInfo;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.ClusterManagementGetResultAssert;
import org.apache.geode.test.junit.assertions.ClusterManagementListResultAssert;
import org.apache.geode.test.junit.rules.MemberStarterRule;

public class GatewayReceiverManagementDUnitTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator;
  private GatewayReceiver receiver;

  @BeforeClass
  public static void beforeClass() {
    locator = cluster.startLocatorVM(0, MemberStarterRule::withHttpService);
    int locatorPort = locator.getPort();
    cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort)
        .withProperty("groups", "group1"));
  }

  @Before
  public void before() {
    receiver = new GatewayReceiver();
  }

  @Test
  public void createGWRAndList() {
    ClusterManagementService cms = new ClusterManagementServiceBuilder()
        .setPort(locator.getHttpPort())
        .setUsername("cluster")
        .setPassword("cluster")
        .build();

    receiver.setStartPort(5000);
    receiver.setGroup("group1");
    List<RealizationResult> results =
        assertManagementResult(cms.create(receiver)).isSuccessful()
            .containsStatusMessage("Successfully updated configuration for group1.")
            .getMemberStatus();
    assertThat(results).hasSize(1);
    assertThat(results.get(0).isSuccess()).isTrue();
    assertThat(results.get(0).getMemberName()).isEqualTo("server-1");

    // try create another GWR on the same group
    receiver.setStartPort(5001);
    receiver.setGroup("group1");
    assertThatThrownBy(() -> cms.create(receiver))
        .hasMessageContaining(
            "ENTITY_EXISTS: GatewayReceiver 'group1' already exists in group group1.");

    // try create another GWR on another group but has no server
    receiver.setStartPort(5002);
    receiver.setGroup("group2");
    assertManagementResult(cms.create(receiver)).isSuccessful()
        .containsStatusMessage("Successfully updated configuration for group2.")
        .hasMemberStatus().hasSize(0);

    // try create another GWR on another group but has a common member in another group
    receiver.setStartPort(5003);
    receiver.setGroup(null);
    assertThatThrownBy(() -> cms.create(receiver))
        .hasMessageContaining(
            "ENTITY_EXISTS: GatewayReceiver 'cluster' already exists on member(s) server-1.");

    ClusterManagementListResultAssert<GatewayReceiver, GatewayReceiverInfo> listAssert =
        assertManagementListResult(cms.list(new GatewayReceiver())).isSuccessful();
    List<EntityGroupInfo<GatewayReceiver, GatewayReceiverInfo>> listResult =
        listAssert.getResult();

    assertThat(listResult).hasSize(2);

    // verify that we have two configurations, but only group1 config has a running gwr
    for (EntityGroupInfo<GatewayReceiver, GatewayReceiverInfo> result : listResult) {
      if (result.getConfiguration().getGroup().equals("group1")) {
        assertThat(result.getRuntimeInfo()).hasSize(1);
        assertThat(result.getRuntimeInfo().get(0).getPort()).isGreaterThanOrEqualTo(5000);
        assertThat(result.getRuntimeInfo().get(0).getMemberName()).isEqualTo("server-1");
        assertThat(result.getRuntimeInfo().get(0).isRunning()).isEqualTo(true);
        assertThat(result.getRuntimeInfo().get(0).getSenderCount()).isEqualTo(0);
        assertThat(result.getRuntimeInfo().get(0).getConnectedSenders()).hasSize(0);
      } else {
        assertThat(result.getConfiguration().getGroup()).isEqualTo("group2");
        assertThat(result.getRuntimeInfo()).hasSize(0);
      }
    }

    GatewayReceiver filter1 = new GatewayReceiver();
    filter1.setGroup("group2");
    listAssert = assertManagementListResult(cms.list(filter1)).isSuccessful();
    listResult = listAssert.getResult();
    assertThat(listResult).hasSize(1);
    assertThat(listResult.get(0).getConfiguration().getGroup()).isEqualTo("group2");

    GatewayReceiver filter2 = new GatewayReceiver();
    filter2.setGroup("group3");
    listAssert = assertManagementListResult(cms.list(filter2)).isSuccessful();
    listResult = listAssert.getResult();
    assertThat(listResult).hasSize(0);

    GatewayReceiver filter = new GatewayReceiver();
    filter.setGroup("group2");
    ClusterManagementGetResultAssert<GatewayReceiver, GatewayReceiverInfo> getAssert =
        assertManagementGetResult(cms.get(filter)).isSuccessful();
    EntityGroupInfo<GatewayReceiver, GatewayReceiverInfo> getResult = getAssert.getResult();
    assertThat(getResult.getConfiguration().getId()).isEqualTo("group2");

    GatewayReceiver clusterFilter = new GatewayReceiver();
    assertThatThrownBy(() -> cms.get(clusterFilter))
        .hasMessageContaining(
            "ENTITY_NOT_FOUND: GatewayReceiver 'cluster' does not exist.");

    GatewayReceiver noMatchesFilter = new GatewayReceiver();
    noMatchesFilter.setGroup("groupNotFound");
    assertThatThrownBy(() -> cms.get(noMatchesFilter))
        .hasMessageContaining(
            "ENTITY_NOT_FOUND: GatewayReceiver 'groupNotFound' does not exist.");
  }
}
