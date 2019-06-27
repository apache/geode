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

import static org.apache.geode.test.junit.assertions.ClusterManagementResultAssert.assertManagementResult;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class GatewayReceiverManagementDUnitTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator, server;
  private static ClusterManagementService cms;
  private GatewayReceiverConfig receiver;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = cluster.startLocatorVM(0,
        l -> l.withSecurityManager(SimpleSecurityManager.class).withHttpService());
    int locatorPort = locator.getPort();
    server = cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort)
        .withProperty("groups", "group1")
        .withCredential("cluster", "cluster"));
  }

  @Before
  public void before() throws Exception {
    receiver = new GatewayReceiverConfig();
  }

  @Test
  public void withInsuffientCredential() throws Exception {
    cms = ClusterManagementServiceBuilder.buildWithHostAddress()
        .setHostAddress("localhost", locator.getHttpPort())
        .setCredentials("test", "test").build();

    assertManagementResult(cms.create(receiver)).failed()
        .hasStatusCode(ClusterManagementResult.StatusCode.UNAUTHORIZED)
        .containsStatusMessage("test not authorized for CLUSTER:MANAGE");
  }

  @Test
  public void createGWR() throws Exception {
    cms = ClusterManagementServiceBuilder.buildWithHostAddress()
        .setHostAddress("localhost", locator.getHttpPort())
        .setCredentials("cluster", "cluster").build();

    receiver.setStartPort("5000");
    receiver.setGroup("group1");
    List<RealizationResult> results =
        assertManagementResult(cms.create(receiver)).isSuccessful()
            .containsStatusMessage("Successfully updated config for group1")
            .getMemberStatus();
    assertThat(results).hasSize(1);
    assertThat(results.get(0).isSuccess()).isTrue();
    assertThat(results.get(0).getMemberName()).isEqualTo("server-1");

    // try create another GWR on the same group
    receiver.setStartPort("5001");
    receiver.setGroup("group1");
    assertManagementResult(cms.create(receiver)).failed()
        .hasStatusCode(ClusterManagementResult.StatusCode.ENTITY_EXISTS)
        .containsStatusMessage("Member(s) server-1 already has this element created");

    // try create another GWR on another group but has no server
    receiver.setStartPort("5002");
    receiver.setGroup("group2");
    assertManagementResult(cms.create(receiver)).isSuccessful()
        .containsStatusMessage("Successfully updated config for group2")
        .hasMemberStatus().hasSize(0);

    // try create another GWR on another group but has a common member in another goup
    receiver.setStartPort("5003");
    receiver.setGroup(null);
    assertManagementResult(cms.create(receiver)).failed()
        .hasStatusCode(ClusterManagementResult.StatusCode.ENTITY_EXISTS)
        .containsStatusMessage("Member(s) server-1 already has this element created");

    assertManagementResult(cms.list(new GatewayReceiverConfig())).isSuccessful()
        .hasListResult().hasSize(2);
  }
}
