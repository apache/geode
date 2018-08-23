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

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.client.ClientRegionShortcut.LOCAL;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.ClientServerObserver;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Region destroy message from server to client results in client calling unregister to server (an
 * unnecessary callback). The unregister encounters an error because the region has been destroyed
 * on the server and hence falsely marks the server dead.
 *
 * <p>
 * TRAC #36457: Region Destroy results in unregister interest propagation to server resulting in
 * server being falsely marked dead
 */
@Category(ClientServerTest.class)
public class ClientDestroyRegionUnregisterInterestRegressionTest implements Serializable {

  private String uniqueName;
  private String regionName;
  private String hostName;

  private VM server1;
  private VM server2;
  private VM client1;
  private VM client2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server1 = getVM(0);
    server2 = getVM(1);
    client1 = getVM(2);
    client2 = getVM(3);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    hostName = getHostName();

    int port1 = server1.invoke(() -> createServerCache());
    int port2 = server2.invoke(() -> createServerCache());

    client1.invoke(() -> createClientCache(port1, port2));
    client2.invoke(() -> createClientCache(port1, port2));
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> {
      PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;
      ClientServerObserverHolder.clearInstance();
    });
  }

  @Test
  public void destroyRegionFromClientDoesNotUnregisterInterest() {
    client2.invoke(() -> {
      PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = true;
      ClientServerObserverHolder.setInstance(spy(ClientServerObserver.class));
    });

    client1.invoke(() -> clientCacheRule.getClientCache().getRegion(regionName).destroyRegion());

    client2.invoke(() -> verify(ClientServerObserverHolder.getInstance(), times(0))
        .afterPrimaryIdentificationFromBackup(any()));
  }

  private void createClientCache(int port1, int port2) {
    clientCacheRule.createClientCache();

    Pool pool = PoolManager.createFactory().addServer(hostName, port1).addServer(hostName, port2)
        .setSubscriptionEnabled(true).setMinConnections(4).create(uniqueName);

    ClientRegionFactory<Object, ?> clientRegionFactory =
        clientCacheRule.getClientCache().createClientRegionFactory(LOCAL);
    clientRegionFactory.setPoolName(pool.getName());

    Region<Object, ?> region = clientRegionFactory.create(regionName);

    List<String> listOfKeys = new ArrayList<>();
    listOfKeys.add("key-1");
    listOfKeys.add("key-2");
    listOfKeys.add("key-3");
    listOfKeys.add("key-4");
    listOfKeys.add("key-5");

    region.registerInterest(listOfKeys);
  }

  private int createServerCache() throws IOException {
    cacheRule.createCache();

    RegionFactory regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE);

    regionFactory.create(regionName);

    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }
}
