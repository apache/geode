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
package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;


@Category({GfshTest.class})
public class ListClientCommandDUnitTest {
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(6);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static final String REGION_NAME = "stocks";

  public static final int locatorID = 0, server1ID = 1, server2ID = 2, client1ID = 3, client2ID = 4;

  private static MemberVM locator, server1, server2;

  private static ClientVM client1, client2;

  @Before
  public void setup() throws Exception {
    locator = cluster.startLocatorVM(locatorID);

    gfsh.connectAndVerify(locator);
  }

  private void startServers(MemberVM locator) {
    server1 = cluster.startServerVM(server1ID,
        r -> r.withRegion(RegionShortcut.REPLICATE, REGION_NAME)
            .withConnectionToLocator(locator.getPort()));
    server2 = cluster.startServerVM(server2ID,
        r -> r.withRegion(RegionShortcut.REPLICATE, REGION_NAME)
            .withConnectionToLocator(locator.getPort()));
  }

  @Test
  public void noResultIsSuccess() {
    startServers(locator);
    gfsh.executeAndAssertThat("list clients").statusIsSuccess();
  }

  @Test
  public void noMembersIsSuccess() {
    gfsh.executeAndAssertThat("list clients").statusIsSuccess();
  }

  @Test
  public void testTwoClientsConnectToOneServer() throws Exception {
    startServers(locator);
    int server1port = server1.getPort();
    Properties client1props = new Properties();
    client1props.setProperty("name", "client-1");
    client1 = cluster.startClientVM(client1ID, client1props, cf -> {
      cf.addPoolServer("localhost", server1port);
      cf.setPoolSubscriptionEnabled(true);
    });
    Properties client2props = new Properties();
    client2props.setProperty("name", "client-2");
    client2 = cluster.startClientVM(client2ID, client2props, cf -> {
      cf.addPoolServer("localhost", server1port);
      cf.setPoolSubscriptionEnabled(true);
    });

    MemberVM.invokeInEveryMember(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<Object, Object> regionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.LOCAL)
              .setPoolName(clientCache.getDefaultPool().getName());
      Region<Object, Object> dataRegion = regionFactory.create(REGION_NAME);
      assertNotNull(dataRegion);
      dataRegion.put("k1", "v1");
      dataRegion.put("k2", "v2");
    }, client1, client2);

    locator.waitTillClientsAreReadyOnServers("server-1", server1port, 2);

    Map<String, List<String>> clientMap =
        gfsh.executeAndAssertThat("list clients").statusIsSuccess()
            .hasTableSection("clientList")
            .hasRowSize(2).getActual().getContent();

    try {
      assertThat(clientMap.get("Client Name / ID").get(0)).contains("client-1");
      assertThat(clientMap.get("Client Name / ID").get(1)).contains("client-2");
    } catch (AssertionError e) {
      assertThat(clientMap.get("Client Name / ID").get(0)).contains("client-2");
      assertThat(clientMap.get("Client Name / ID").get(1)).contains("client-1");
    }
    assertThat(clientMap.get("Server Name / ID"))
        .containsExactlyInAnyOrder("member=server-1,port=" + server1port,
            "member=server-1,port=" + server1port);

    // shutdown the clients
    cluster.stop(client1ID);
    cluster.stop(client2ID);
  }

  @Test
  public void oneClientConnectToTwoServers() throws Exception {
    startServers(locator);
    int server1port = server1.getPort();
    int server2port = server2.getPort();
    Properties client1props = new Properties();
    client1props.setProperty("name", "client-1");
    client1 = cluster.startClientVM(client1ID, client1props, cf -> {
      cf.addPoolServer("localhost", server1port);
      cf.setPoolSubscriptionEnabled(true);
    });

    client1.invoke(() -> {
      String poolName = "new_pool_" + System.currentTimeMillis();
      try {
        PoolImpl p = (PoolImpl) PoolManager.createFactory()
            .addServer("localhost", server2port)
            .setMinConnections(1).setSubscriptionEnabled(true).setPingInterval(1)
            .setStatisticInterval(1).setMinConnections(1).setSubscriptionRedundancy(1)
            .create(poolName);
        assertNotNull(p);
      } catch (Exception eee) {
        System.err.println("Exception in creating pool " + poolName + "    Exception =="
            + ExceptionUtils.getStackTrace(eee));
      }

      // create the region
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<Object, Object> regionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.LOCAL)
              .setPoolName(clientCache.getDefaultPool().getName());
      Region<Object, Object> dataRegion = regionFactory.create(REGION_NAME);
      assertNotNull(dataRegion);
      dataRegion.put("k1", "v1");
      dataRegion.put("k2", "v2");
    });

    locator.waitTillClientsAreReadyOnServers("server-1", server1port, 1);
    locator.waitTillClientsAreReadyOnServers("server-2", server2port, 1);

    Map<String, List<String>> content = gfsh.executeAndAssertThat("list clients").statusIsSuccess()
        .hasTableSection("clientList").getActual().getContent();
    assertThat(content.get("Client Name / ID")).hasSize(1);
    assertThat(content.get("Client Name / ID").get(0)).containsSequence("client-1");

    assertThat(content.get("Server Name / ID")).hasSize(1);
    assertThat(content.get("Server Name / ID").get(0)).containsSequence("server-2")
        .containsSequence("server-1");

    // shutdown the clients
    cluster.stop(client1ID);
  }
}
