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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;


public class PersistentRegionTransactionDUnitTest extends JUnit4CacheTestCase {

  private VM server;
  private VM client;
  private static final int KEY = 5;
  private static final String VALUE = "value 5";
  private static final String REGIONNAME = "region";

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public DistributedDiskDirRule distributedDiskDir = new DistributedDiskDirRule();

  @Before
  public void allowTransactions() {
    server = VM.getVM(0);
    client = VM.getVM(1);
    server.invoke(() -> TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = true);
  }

  @After
  public void disallowTransactions() {
    server.invoke(() -> TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = false);

  }

  @Test
  public void clientTransactionCanGetNotRecoveredEntryOnPersistentOverflowRegion()
      throws Exception {
    createServer(server, true, false);
    putData(server);
    server.invoke(() -> getCache().close());
    int port = createServer(server, true, false);


    client.invoke(() -> {
      ClientCacheFactory factory = new ClientCacheFactory().addPoolServer("localhost", port);
      ClientCache cache = getClientCache(factory);
      cache.getCacheTransactionManager().begin();
      try {
        assertEquals(VALUE, cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create(REGIONNAME).get(KEY));
      } finally {
        cache.getCacheTransactionManager().rollback();
      }
    });
  }

  private void putData(final VM server) {
    server.invoke(() -> {
      IntStream.range(0, 20)
          .forEach(index -> getCache().getRegion(REGIONNAME).put(index, "value " + index));
    });
  }

  private int createServer(final VM server, boolean isOverflow, boolean isAsyncDiskWrite) {
    return server.invoke(() -> {
      if (!isOverflow) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
      }
      CacheFactory cacheFactory = new CacheFactory();
      Cache cache = getCache(cacheFactory);
      cache.createDiskStoreFactory().setQueueSize(3).setTimeInterval(10000).create("disk");
      if (isOverflow) {
        cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT)
            .setDiskSynchronous(!isAsyncDiskWrite).setDiskStoreName("disk")
            .setEvictionAttributes(
                EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK))
            .create(REGIONNAME);
      } else {
        cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).create(REGIONNAME);
      }
      CacheServer cacheServer = cache.addCacheServer();
      cacheServer.setPort(0);
      cacheServer.start();
      return cacheServer.getPort();
    });
  }

  @Test
  public void clientTransactionCanGetEvictedEntryOnPersistentOverflowRegion() throws Exception {
    int port = createServer(server, true, false);
    putData(server);
    client.invoke(() -> {
      ClientCacheFactory factory = new ClientCacheFactory().addPoolServer("localhost", port);
      ClientCache cache = getClientCache(factory);
      cache.getCacheTransactionManager().begin();
      try {
        assertEquals(VALUE, cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create(REGIONNAME).get(KEY));
      } finally {
        cache.getCacheTransactionManager().rollback();
      }
    });
  }

  @Test
  public void transactionCanGetEvictedEntryOnPersistentOverflowRegion() throws Exception {
    createServer(server, true, false);
    putData(server);
    server.invoke(() -> {
      LocalRegion region = (LocalRegion) getCache().getRegion(REGIONNAME);
      await()
          .untilAsserted(() -> assertThat(region.getValueInVM(KEY)).isNull());
      getCache().getCacheTransactionManager().begin();
      try {
        assertEquals(VALUE, region.get(KEY));
      } finally {
        cache.getCacheTransactionManager().rollback();
      }
    });
  }

  @Test
  public void transactionCanGetNotRecoveredEntryOnPersistentOverflowRegion() throws Exception {
    createServer(server, true, false);
    putData(server);
    server.invoke(() -> getCache().close());
    createServer(server, true, false);
    server.invoke(() -> {
      LocalRegion region = (LocalRegion) getCache().getRegion("region");
      getCache().getCacheTransactionManager().begin();
      try {
        assertEquals(VALUE, region.get(KEY));
      } finally {
        cache.getCacheTransactionManager().rollback();
      }
    });
  }

  @Test
  public void transactionCanGetNotRecoveredEntryOnPersistentRegion() throws Exception {
    createServer(server, false, false);
    putData(server);
    server.invoke(() -> getCache().close());
    createServer(server, false, false);
    server.invoke(() -> {
      LocalRegion region = (LocalRegion) getCache().getRegion("region");
      assertThat(region.getValueInVM(KEY)).isNull();
      getCache().getCacheTransactionManager().begin();
      try {
        assertEquals(VALUE, region.get(KEY));
      } finally {
        cache.getCacheTransactionManager().rollback();
      }
    });
  }

  @Test
  public void clientTransactionCanGetNotRecoveredEntryOnPersistentRegion() throws Exception {
    createServer(server, false, false);
    putData(server);
    server.invoke(() -> getCache().close());
    int port = createServer(server, false, false);


    client.invoke(() -> {
      ClientCacheFactory factory = new ClientCacheFactory().addPoolServer("localhost", port);
      ClientCache cache = getClientCache(factory);
      cache.getCacheTransactionManager().begin();
      try {
        assertEquals(VALUE, cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create(REGIONNAME).get(KEY));
      } finally {
        cache.getCacheTransactionManager().rollback();
      }
    });
  }

  @Test
  public void transactionCanUpdateEntryOnAsyncOverflowRegion() throws Exception {
    createServer(server, true, true);
    server.invoke(() -> {
      Cache cache = getCache();
      DiskStoreImpl diskStore = (DiskStoreImpl) cache.findDiskStore("disk");
      LocalRegion region = (LocalRegion) cache.getRegion("region");
      region.put(1, "value1");
      region.put(2, "value2"); // causes key 1 to be evicted and sits in the async queue
      TXManagerImpl txManager = getCache().getTxManager();
      txManager.begin();
      assertNotEquals(region.getValueInVM(1), Token.NOT_AVAILABLE);
      region.put(1, "new value");
      TransactionId txId = txManager.suspend();
      region.put(3, "value3");
      region.put(4, "value4");
      diskStore.flush();
      txManager.resume(txId);

      txManager.commit();

      assertEquals("new value", region.get(1));
    });
  }
}
