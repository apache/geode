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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class PersistentRegionTransactionDUnitTest extends JUnit4CacheTestCase {

  private VM server;
  private VM client;
  private final int KEY = 5;
  private final String VALUE = "value 5";
  private final String REGIONNAME = "region";

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();


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
  public void testClientTransactionWorksOnRecoveredPersistentOverflowRegion() throws Exception {
    createServer(server, true);

    putData(server);
    server.invoke(() -> getCache().close());
    int port = createServer(server, true);


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

  private int createServer(final VM server, boolean isOverflow) {
    return server.invoke(() -> {
      if (!isOverflow) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
      }
      CacheFactory cacheFactory = new CacheFactory();
      Cache cache = getCache(cacheFactory);
      if (isOverflow) {
        cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT)
            .setEvictionAttributes(
                EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK))
            .create(REGIONNAME);
      } else {
        cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).create(REGIONNAME);
      }
      CacheServer cacheServer = cache.addCacheServer();
      cacheServer.start();
      return cacheServer.getPort();
    });
  }

  @Test
  public void testClientTransactionWorksOnPersistentOverflowRegion() throws Exception {
    int port = createServer(server, true);
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
  public void testTransactionWorksOnPersistentOverflowRegion() throws Exception {
    createServer(server, true);
    putData(server);

    server.invoke(() -> {
      LocalRegion region = (LocalRegion) getCache().getRegion(REGIONNAME);
      Awaitility.await().atMost(10, SECONDS)
          .until(() -> assertThat(region.getValueInVM(KEY)).isNull());
      getCache().getCacheTransactionManager().begin();
      try {
        assertEquals(VALUE, region.get(KEY));
      } finally {
        cache.getCacheTransactionManager().rollback();
      }
    });
  }

  @Test
  public void testTransactionWorksOnRecoveredPersistentOverflowRegion() throws Exception {
    createServer(server, true);
    putData(server);
    server.invoke(() -> getCache().close());
    createServer(server, true);
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
  public void testTransactionWorksOnRecoveredPersistentRegion() throws Exception {
    createServer(server, false);
    putData(server);
    server.invoke(() -> getCache().close());
    createServer(server, false);
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
  public void testClientTransactionWorksOnRecoveredPersistentRegion() throws Exception {
    createServer(server, false);

    putData(server);
    server.invoke(() -> getCache().close());
    int port = createServer(server, false);


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
}
