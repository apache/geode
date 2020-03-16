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
package org.apache.geode.cache.wan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.AutoConnectionSourceImpl;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;

@SuppressWarnings("deprecation")
@Category({WanTest.class})
public class GatewayReceiverAutoConnectionSourceDUnitTest
    extends org.apache.geode.cache.client.internal.LocatorTestBase {

  public GatewayReceiverAutoConnectionSourceDUnitTest() {
    super();
  }

  @Test
  public void testBridgeServerAndGatewayReceiverClientAndServerWithoutGroup() throws Exception {
    runBridgeServerAndGatewayReceiverTest(null, null, true);
  }

  @Test
  public void testBridgeServerAndGatewayReceiverClientAndServerWithGroup() throws Exception {
    String groupName = "group1";
    runBridgeServerAndGatewayReceiverTest(new String[] {groupName}, groupName, true);
  }

  @Test
  public void testBridgeServerAndGatewayReceiverClientWithoutGroupServerWithGroup()
      throws Exception {
    String groupName = "group1";
    runBridgeServerAndGatewayReceiverTest(new String[] {groupName}, null, true);
  }

  @Test
  public void testBridgeServerAndGatewayReceiverClientWithGroupServerWithoutGroup()
      throws Exception {
    String groupName = "group1";
    runBridgeServerAndGatewayReceiverTest(null, groupName, false);
  }

  private void runBridgeServerAndGatewayReceiverTest(String[] serverGroups, String clientGroup,
      boolean oneServerExpected) throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    String hostName = NetworkUtils.getServerHostName();
    int locatorPort = startLocatorInVM(vm0, hostName, "");

    String locators = getLocatorString(hostName, locatorPort);

    int serverPort = startBridgeServerInVM(vm1, serverGroups, locators, true);

    addGatewayReceiverToVM(vm1);

    startBridgeClientInVM(vm2, clientGroup, NetworkUtils.getServerHostName(vm0.getHost()),
        locatorPort);

    // Verify getAllServers returns a valid number of servers
    verifyGetAllServers(vm2, REGION_NAME, serverPort, oneServerExpected);
  }

  private void addGatewayReceiverToVM(VM vm) {
    vm.invoke(new SerializableRunnable("add GatewayReceiver") {
      @Override
      public void run() {
        Cache cache = (Cache) remoteObjects.get(CACHE_KEY);
        GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
        GatewayReceiver receiver = fact.create();
        assertTrue(receiver.isRunning());
      }
    });
  }

  private void verifyGetAllServers(VM vm, final String regionName, final int serverPort,
      final boolean oneServerExpected) {
    vm.invoke(new SerializableRunnable("verify getAllServers") {
      @Override
      public void run() {
        Cache cache = (Cache) remoteObjects.get(CACHE_KEY);
        Region<?, ?> region = cache.getRegion(regionName);
        PoolImpl pool = (PoolImpl) PoolManager.find(region);
        AutoConnectionSourceImpl connectionSource =
            (AutoConnectionSourceImpl) pool.getConnectionSource();
        List<ServerLocation> allServers = connectionSource.getAllServers();
        if (oneServerExpected) {
          // One server is expected. Assert one was returned, and its port matches the input
          // serverPort.
          assertEquals(1, allServers.size());
          ServerLocation serverLocation = allServers.get(0);
          assertEquals(serverPort, serverLocation.getPort());
        } else {
          // No servers are expected. Assert none were returned.
          assertNull(allServers);
        }
      }
    });
  }
}
