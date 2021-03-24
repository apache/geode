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


import static org.apache.geode.test.junit.assertions.ClusterManagementRealizationResultAssert.assertManagementResult;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.EntityGroupInfo;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransport;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.ClassName;
import org.apache.geode.management.configuration.GatewayReceiver;
import org.apache.geode.management.runtime.GatewayReceiverInfo;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class GatewayManagementIntegrationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any BaseLocatorContextLoader
  private LocatorWebContext context;

  private ClusterManagementService client;

  private GatewayReceiver receiver;

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
    client = new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(
            new RestTemplate(context.getRequestFactory())))
        .build();
    receiver = new GatewayReceiver();
  }

  @Test
  public void listEmptyGatewayReceivers() {
    ClusterManagementListResult<GatewayReceiver, GatewayReceiverInfo> result =
        client.list(receiver);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getEntityGroupInfo().size()).isEqualTo(0);
  }

  @Test
  public void listExisting() {
    LocatorStarterRule locator =
        ((PlainLocatorContextLoader) context.getLocator()).getLocatorStartupRule();
    InternalConfigurationPersistenceService cps =
        locator.getLocator().getConfigurationPersistenceService();

    // manually create a gateway receiver in cluster group
    cps.updateCacheConfig("cluster", cacheConfig -> {
      GatewayReceiverConfig receiver = new GatewayReceiverConfig();
      receiver.setBindAddress("localhost");
      receiver.setManualStart(false);
      receiver.setStartPort("5000");
      cacheConfig.setGatewayReceiver(receiver);
      return cacheConfig;
    });

    ClusterManagementListResult<GatewayReceiver, GatewayReceiverInfo> results =
        client.list(receiver);
    assertThat(results.isSuccessful()).isTrue();
    List<EntityGroupInfo<GatewayReceiver, GatewayReceiverInfo>> receivers =
        results.getEntityGroupInfo();
    assertThat(receivers.size()).isEqualTo(1);
    GatewayReceiver result = receivers.get(0).getConfiguration();
    assertThat(result.isManualStart()).isFalse();
    assertThat(result.getStartPort()).isEqualTo(5000);

    // manually removing the GWR so that it won't pollute other tests
    cps.updateCacheConfig("cluster", cacheConfig -> {
      cacheConfig.setGatewayReceiver(null);
      return cacheConfig;
    });
  }

  @Test
  public void createGatewayReceiver() {
    GatewayReceiver gatewayReceiver = new GatewayReceiver();
    gatewayReceiver.setGroup("group");
    gatewayReceiver.setStartPort(1000);
    gatewayReceiver.setEndPort(1000);
    gatewayReceiver.setManualStart(true);
    gatewayReceiver.setMaximumTimeBetweenPings(200);
    gatewayReceiver
        .setGatewayTransportFilters(
            Collections.singletonList(new ClassName("className", "{\"key1\": \"value1\"}")));

    assertManagementResult(client.create(gatewayReceiver))
        .hasStatusCode(ClusterManagementResult.StatusCode.OK);

    // manually removing the GWR so that it won't pollute other tests
    LocatorStarterRule locator =
        ((PlainLocatorContextLoader) context.getLocator()).getLocatorStartupRule();
    InternalConfigurationPersistenceService cps =
        locator.getLocator().getConfigurationPersistenceService();
    cps.updateCacheConfig("group", cacheConfig -> {
      cacheConfig.setGatewayReceiver(null);
      return cacheConfig;
    });
  }
}
