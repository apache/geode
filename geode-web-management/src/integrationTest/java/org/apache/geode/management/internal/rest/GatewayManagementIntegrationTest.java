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
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
public class GatewayManagementIntegrationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any BaseLocatorContextLoader
  private LocatorWebContext context;

  private ClusterManagementService client;

  private GatewayReceiverConfig receiver;

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
    client = ClusterManagementServiceBuilder.buildWithRequestFactory()
        .setRequestFactory(context.getRequestFactory()).build();
    receiver = new GatewayReceiverConfig();
  }

  @Test
  public void listEmptyGatewayReceivers() {
    ClusterManagementResult<GatewayReceiverConfig> result = client.list(receiver);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getResult().size()).isEqualTo(0);
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

    ClusterManagementResult<GatewayReceiverConfig> results = client.list(receiver);
    assertThat(results.isSuccessful()).isTrue();
    List<GatewayReceiverConfig> receivers = results.getResult();
    assertThat(receivers.size()).isEqualTo(1);
    GatewayReceiverConfig result = receivers.get(0);
    assertThat(result.getBindAddress()).isEqualTo("localhost");
    assertThat(result.isManualStart()).isFalse();
    assertThat(result.getStartPort()).isEqualTo("5000");

    // manually removing the GWR so that it won't pollute other tests
    cps.updateCacheConfig("cluster", cacheConfig -> {
      cacheConfig.setGatewayReceiver(null);
      return cacheConfig;
    });
  }

  @Test
  public void createWithBindAddress() throws Exception {
    receiver.setBindAddress("test-mbpro");
    assertManagementResult(client.create(receiver)).failed()
        .hasStatusCode(ClusterManagementResult.StatusCode.ILLEGAL_ARGUMENT)
        .containsStatusMessage("");
  }

  @Test
  public void createWithHostName() throws Exception {
    receiver.setHostnameForSenders("test-mbpro");
    assertManagementResult(client.create(receiver)).failed()
        .hasStatusCode(ClusterManagementResult.StatusCode.ILLEGAL_ARGUMENT)
        .containsStatusMessage("");
  }
}
