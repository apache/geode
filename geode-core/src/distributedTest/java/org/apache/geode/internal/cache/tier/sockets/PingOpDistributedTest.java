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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPortsForDUnitSite;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.PingOp;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class PingOpDistributedTest implements Serializable {

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedRule distributedRule = new DistributedRule(2);

  @Rule
  public SerializableTemporaryFolder folder = new SerializableTemporaryFolder();

  private VM client;
  private VM server1, server2;
  private int server1Port, server2Port;

  private void initServer(int serverPort) throws IOException {
    cacheRule.createCache();
    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(serverPort);

    // "Disable" the auto-ping for the duration of this test.
    cacheServer.setMaximumTimeBetweenPings((int) GeodeAwaitility.getTimeout().toMillis());
    cacheServer.start();
  }

  private void initClient(String poolName, List<Integer> serverPorts) {
    final ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
    clientCacheFactory.create();

    PoolFactory poolFactory = PoolManager.createFactory();
    serverPorts.forEach(serverPort -> poolFactory.addServer("localhost", serverPort));

    // "Disable" the auto-ping for the duration of this test.
    poolFactory.setPingInterval((int) GeodeAwaitility.getTimeout().toMillis());
    poolFactory.create(poolName);
  }

  @Before
  public void setUp() throws IOException {
    int[] ports = getRandomAvailableTCPPortsForDUnitSite(2);

    client = getVM(0);
    server1 = getVM(1);
    server2 = getVM(2);
    server1Port = ports[0];
    server2Port = ports[1];
    server1.invoke(() -> initServer(server1Port));
    server2.invoke(() -> initServer(server2Port));
  }

  void parametrizedSetUp(String poolName, List<Integer> serverPorts) {
    client.invoke(() -> initClient(poolName, serverPorts));
  }

  @Test
  public void memberShouldNotRedirectPingMessageWhenClientCachedViewIdIsWrong() throws IOException {
    final String poolName = testName.getMethodName();
    parametrizedSetUp(poolName, Collections.singletonList(server1Port));
    InternalDistributedMember distributedMember1 = (InternalDistributedMember) server1
        .invoke(() -> cacheRule.getCache().getDistributedSystem().getDistributedMember());

    client.invoke(() -> {
      PoolImpl poolImpl = (PoolImpl) PoolManager.find(poolName);
      distributedMember1.setVmViewId(distributedMember1.getVmViewId() + 1);
      assertThatThrownBy(() -> {
        PingOp.execute(poolImpl, new ServerLocation("localhost", server1Port), distributedMember1);
      }).isInstanceOf(ServerOperationException.class);
    });

  }
}
