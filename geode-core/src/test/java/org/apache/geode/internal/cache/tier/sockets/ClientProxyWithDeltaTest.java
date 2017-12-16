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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePort;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class ClientProxyWithDeltaTest implements Serializable {

  private static final String PROXY_NAME = "PROXY_NAME";
  private static final String CACHING_PROXY_NAME = "CACHING_PROXY_NAME";

  private static InternalCache cache;
  private static InternalClientCache clientCache;

  private String hostName;

  private VM server;
  private VM client1;
  private VM client2;
  private int serverPort;

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Before
  public void setUp() throws Exception {
    server = getHost(0).getVM(0);
    client1 = getHost(0).getVM(1);
    client2 = getHost(0).getVM(3);

    hostName = getServerHostName(server.getHost());

    serverPort = server.invoke(() -> createServerCache());

    client1.invoke(() -> createClientCacheWithProxyRegion(hostName, serverPort));
    client2.invoke(() -> createClientCacheWithProxyRegion(hostName, serverPort));
  }

  @After
  public void tearDown() throws Exception {
    invokeInEveryVM(() -> DeltaEnabledObject.resetFromDeltaInvoked());
    invokeInEveryVM(() -> CacheClientUpdater.isUsedByTest = false);

    disconnectAllFromDS();

    cache = null;
    invokeInEveryVM(() -> cache = null);

    clientCache = null;
    invokeInEveryVM(() -> clientCache = null);
  }

  /**
   * Verifies that delta put arrives as delta object to client with CACHING_PROXY region
   */
  @Test
  public void cachingClientReceivesDeltaUpdates() throws Exception {
    client1.invoke(() -> {
      clientCache.close();
      clientCache = null;
      createClientCacheWithCachingRegion(hostName, serverPort);
    });
    client2.invoke(() -> {
      clientCache.close();
      clientCache = null;
      createClientCacheWithCachingRegion(hostName, serverPort);
    });

    client2.invoke(() -> {
      CacheClientUpdater.isUsedByTest = true;
    });

    client1.invoke(() -> {
      Region<Integer, DeltaEnabledObject> region = clientCache.getRegion(CACHING_PROXY_NAME);
      DeltaEnabledObject objectWithDelta = new DeltaEnabledObject();
      for (int i = 1; i <= 3; i++) {
        objectWithDelta.setValue(i);
        region.put(1, objectWithDelta);
      }
      region.put(0, new DeltaEnabledObject());
    });

    client2.invoke(() -> {
      await().atMost(30, SECONDS)
          .until(() -> clientCache.getRegion(CACHING_PROXY_NAME).containsKey(0));
      assertThat(CacheClientUpdater.fullValueRequested).isFalse();
      assertThat(DeltaEnabledObject.fromDeltaInvoked()).isTrue();
    });
  }

  /**
   * Verifies that delta put arrives as complete object to client with PROXY region
   */
  @Test
  public void emptyClientReceivesFullUpdatesInsteadOfDeltaUpdates() throws Exception {
    client2.invoke(() -> {
      CacheClientUpdater.isUsedByTest = true;
      clientCache.<Integer, DeltaEnabledObject>getRegion(PROXY_NAME).getAttributesMutator()
          .addCacheListener(new ClientListener());
    });

    client1.invoke(() -> {
      Region<Integer, DeltaEnabledObject> region = clientCache.getRegion(PROXY_NAME);
      DeltaEnabledObject objectWithDelta = new DeltaEnabledObject();
      for (int i = 1; i <= 3; i++) {
        objectWithDelta.setValue(i);
        region.put(1, objectWithDelta);
      }
      region.put(0, new DeltaEnabledObject());
    });

    client2.invoke(() -> {
      await().atMost(30, SECONDS).until(() -> ClientListener.keyZeroCreated.get());
      assertThat(CacheClientUpdater.fullValueRequested).isFalse();
      assertThat(DeltaEnabledObject.fromDeltaInvoked()).isFalse();
    });
  }

  /**
   * Verifies that reusing delta object as value does not use delta when putting with new key
   */
  @Test
  public void reusingValueForCreatesDoesNotUseDelta() throws Exception {
    client1.invoke(() -> {
      Region<Integer, DeltaEnabledObject> region = clientCache.getRegion(PROXY_NAME);
      DeltaEnabledObject objectWithDelta = new DeltaEnabledObject();
      for (int i = 1; i <= 3; i++) {
        objectWithDelta.setValue(i);
        region.create(i, objectWithDelta);
      }
    });

    server.invoke(() -> {
      CachePerfStats stats = ((InternalRegion) cache.getRegion(PROXY_NAME)).getCachePerfStats();
      assertThat(stats.getDeltaFailedUpdates()).isEqualTo(0);
      assertThat(DeltaEnabledObject.fromDeltaInvoked()).isFalse();
    });
  }

  private int createServerCache() throws IOException {
    cache = (InternalCache) new CacheFactory().create();

    RegionFactory<Integer, DeltaEnabledObject> regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.REPLICATE);
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);

    regionFactory.<Integer, DeltaEnabledObject>create(PROXY_NAME);
    regionFactory.<Integer, DeltaEnabledObject>create(CACHING_PROXY_NAME);

    CacheServer server = cache.addCacheServer();
    server.setPort(getRandomAvailablePort(SOCKET));
    server.start();
    return server.getPort();
  }

  private void createClientCacheWithProxyRegion(final String hostName, final int port) {
    clientCache = (InternalClientCache) new ClientCacheFactory().create();
    assertThat(clientCache.isClient()).isTrue();

    PoolFactory poolFactory = createPoolFactory();
    poolFactory.addServer(hostName, port);

    Pool pool = poolFactory.create(getClass().getSimpleName() + "-Pool");

    Region region = createRegionOnClient(PROXY_NAME, ClientRegionShortcut.PROXY, pool);

    region.registerInterest("ALL_KEYS");
    assertThat(region.getAttributes().getCloningEnabled()).isFalse();
  }

  private void createClientCacheWithCachingRegion(final String hostName, final int port) {
    clientCache = (InternalClientCache) new ClientCacheFactory().create();
    assertThat(clientCache.isClient()).isTrue();

    PoolFactory poolFactory = createPoolFactory();
    poolFactory.addServer(hostName, port);

    Pool pool = poolFactory.create(getClass().getSimpleName() + "-Pool");

    Region region =
        createRegionOnClient(CACHING_PROXY_NAME, ClientRegionShortcut.CACHING_PROXY, pool);

    region.registerInterest("ALL_KEYS");
    assertThat(region.getAttributes().getCloningEnabled()).isFalse();
  }

  private PoolFactory createPoolFactory() {
    return PoolManager.createFactory().setThreadLocalConnections(true).setMinConnections(3)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0).setReadTimeout(10000)
        .setSocketBufferSize(32768);
  }

  private Region<Integer, DeltaEnabledObject> createRegionOnClient(final String regionName,
      final ClientRegionShortcut shortcut, final Pool pool) {
    ClientRegionFactory<Integer, DeltaEnabledObject> regionFactory =
        clientCache.createClientRegionFactory(shortcut);
    regionFactory.setPoolName(pool.getName());
    Region<Integer, DeltaEnabledObject> region = regionFactory.create(regionName);
    assertThat(region.getAttributes().getCloningEnabled()).isFalse();
    return region;
  }

  private static class ClientListener extends CacheListenerAdapter<Integer, DeltaEnabledObject> {

    static AtomicBoolean keyZeroCreated = new AtomicBoolean(false);

    @Override
    public void afterCreate(EntryEvent<Integer, DeltaEnabledObject> event) {
      keyZeroCreated.set(true);
    }
  }

  /**
   * Object that implements {@code Delta} for use in {@code Cache}.
   */
  private static class DeltaEnabledObject implements Delta, DataSerializable {

    private static final AtomicBoolean fromDeltaInvoked = new AtomicBoolean();

    private int value = 0;

    public DeltaEnabledObject() {}

    public void setValue(int value) {
      this.value = value;
    }

    @Override
    public void fromDelta(DataInput in) throws IOException {
      fromDeltaInvoked.set(true);
      this.value = DataSerializer.readPrimitiveInt(in);
    }

    @Override
    public boolean hasDelta() {
      return true;
    }

    @Override
    public void toDelta(DataOutput out) throws IOException {
      DataSerializer.writePrimitiveInt(this.value, out);
    }

    static void resetFromDeltaInvoked() {
      fromDeltaInvoked.set(false);
    }

    static boolean fromDeltaInvoked() {
      return fromDeltaInvoked.get();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeInt(value);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      value = in.readInt();
    }
  }
}
