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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@SuppressWarnings("serial")
public class ClientServerTransactionDistributedTest implements Serializable {

  private String hostName;
  private String uniqueName;
  private String regionName;
  private VM server1;
  private int port1;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    server1 = getVM(0);
    hostName = getHostName();
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
  }

  @Test
  public void clientTransactionIDAboveIntegerMaxValue() {
    port1 = server1.invoke(() -> createServerRegion());

    // Test that transaction ID overflow to zero
    TXManagerImpl.INITIAL_UNIQUE_ID_VALUE = Integer.MAX_VALUE;
    createClientRegion(true, port1);

    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();

    txManager.begin();
    TXStateProxyImpl txStateProxy = (TXStateProxyImpl) txManager.getTXState();
    int transactionID = ((TXId) txStateProxy.getTransactionId()).getUniqId();

    int numOfOperations = 5;
    putData(numOfOperations);
    txManager.commit();

    server1.invoke(() -> verifyTransactionResult(numOfOperations));
    assertEquals(0, transactionID);
  }

  private void putData(int numberOfEntries) {
    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    for (int key = 0; key < numberOfEntries; key++) {
      String value = getValue(key);
      region.put(key, value);
    }
  }

  private void verifyTransactionResult(int numberOfEntries) {
    Region region = cacheRule.getCache().getRegion(regionName);
    for (int i = 0; i < numberOfEntries; i++) {
      LogService.getLogger().info("region get key {} value {} ", i, region.get(i));
    }
    for (int i = 0; i < numberOfEntries; i++) {
      assertEquals("value" + i, region.get(i));
    }
  }

  private int createServerRegion() throws Exception {
    PartitionAttributesFactory factory = new PartitionAttributesFactory();
    PartitionAttributes partitionAttributes = factory.create();
    cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(partitionAttributes).create(regionName);
    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void createClientRegion(boolean connectToFirstPort, int... ports) {
    clientCacheRule.createClientCache();

    PoolImpl pool = getPool(ports);
    ClientRegionFactory crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);
    crf.setPoolName(pool.getName());
    crf.create(regionName);

    if (ports.length > 1 && connectToFirstPort) {
      // first connection to the first port in the list
      pool.acquireConnection(new ServerLocation(hostName, ports[0]));
    }
  }

  private PoolImpl getPool(int... ports) {
    PoolFactory factory = PoolManager.createFactory();
    for (int port : ports) {
      factory.addServer(hostName, port);
    }
    return (PoolImpl) factory.create(uniqueName);
  }

  private String getValue(int key) {
    return "value" + key;
  }
}
