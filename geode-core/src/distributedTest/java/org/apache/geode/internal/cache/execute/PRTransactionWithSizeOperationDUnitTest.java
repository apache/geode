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
package org.apache.geode.internal.cache.execute;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class PRTransactionWithSizeOperationDUnitTest extends JUnit4CacheTestCase {
  Host host = Host.getHost(0);
  VM dataStore1 = host.getVM(0);
  VM dataStore2 = host.getVM(1);
  VM client = host.getVM(2);

  @Test
  public void testSizeOpOnPRWithLocalRegionInTransaction() {
    String regionName = "region";
    String region2Name = "region2";
    int totalBuckets = 2;
    boolean isSecondRegionLocal = true;
    setupRegions(totalBuckets, regionName, isSecondRegionLocal, region2Name);

    dataStore1.invoke(() -> verifySizeOpTransaction(2, regionName, totalBuckets, region2Name,
        isSecondRegionLocal));
  }

  @Test
  public void testSizeOpOnPRWithReplicateRegionInTransaction() {
    String regionName = "region";
    String region2Name = "region2";
    int totalBuckets = 2;
    boolean isSecondRegionLocal = false;
    setupRegions(totalBuckets, regionName, isSecondRegionLocal, region2Name);

    dataStore1.invoke(() -> verifySizeOpTransaction(2, regionName, totalBuckets, region2Name,
        isSecondRegionLocal));
  }

  private void setupRegions(int totalBuckets, String regionName, boolean isLocal,
      String region2Name) {
    createPRAndInitABucketOnDataStore1(totalBuckets, regionName);

    createPRAndInitOtherBucketsOnDataStore2(totalBuckets, regionName);

    initSecondRegion(totalBuckets, region2Name, isLocal);
  }

  private void createPRAndInitABucketOnDataStore1(int totalBuckets, String regionName) {
    dataStore1.invoke(() -> {
      createPartitionedRegion(regionName, 0, totalBuckets);
      Region<Integer, String> region = getCache().getRegion(regionName);
      // should create first bucket on server1
      region.put(1, "VALUE-1");
    });
  }

  @SuppressWarnings("rawtypes")
  private void createPartitionedRegion(String regionName, int copies, int totalBuckets) {
    RegionFactory<Integer, String> factory = getCache().createRegionFactory();
    PartitionAttributes pa = new PartitionAttributesFactory().setTotalNumBuckets(totalBuckets)
        .setRedundantCopies(copies).create();
    factory.setPartitionAttributes(pa).create(regionName);
  }

  private void createPRAndInitOtherBucketsOnDataStore2(int totalBuckets, String regionName) {
    dataStore2.invoke(() -> {
      createPartitionedRegion(regionName, 0, totalBuckets);
      Region<Integer, String> region = getCache().getRegion(regionName);
      for (int i = totalBuckets; i > 1; i--) {
        region.put(i, "VALUE-" + i);
      }
    });
  }

  private void initSecondRegion(int totalBuckets, String region2Name, boolean isLocal) {
    dataStore2.invoke(() -> createSecondRegion(region2Name, isLocal));
    dataStore1.invoke(() -> {
      createSecondRegion(region2Name, isLocal);
      Region<Integer, String> region = getCache().getRegion(region2Name);
      for (int i = totalBuckets; i > 0; i--) {
        region.put(i, "" + i);
      }
    });
  }

  private void createSecondRegion(String regionName, boolean isLocal) {
    RegionFactory<Integer, String> rf =
        getCache().createRegionFactory(isLocal ? RegionShortcut.LOCAL : RegionShortcut.REPLICATE);
    rf.create(regionName);
  }

  private void verifySizeOpTransaction(int key, String regionName, int totalBuckets,
      String region2Name, boolean isLocal) {
    if (isLocal) {
      assertThatThrownBy(
          () -> doSizeOpTransaction(2, regionName, totalBuckets, region2Name, isLocal))
              .isInstanceOf(TransactionException.class);
    } else {
      doSizeOpTransaction(2, regionName, totalBuckets, region2Name, isLocal);
    }
  }

  private void doSizeOpTransaction(int key, String regionName, int totalBuckets, String region2Name,
      boolean isLocal) {
    TXManagerImpl txMgr = (TXManagerImpl) getCache().getCacheTransactionManager();
    Region<Integer, String> region = getCache().getRegion(regionName);
    Region<Integer, String> region2 = getCache().getRegion(region2Name);
    try {
      txMgr.begin();
      region.get(key);
      assertEquals(totalBuckets, region.size());
      int num = totalBuckets + 1;
      region2.put(num, "" + num);
      assertEquals(num, region2.size());
    } finally {
      txMgr.rollback();
    }
  }

  @Test
  public void testClientServerSizeOpOnPRWithReplicateRegionInTransaction() {
    final Properties properties = getDistributedSystemProperties();
    final int port = dataStore1.invoke("create cache", () -> {
      Cache cache = getCache(properties);
      CacheServer cacheServer = createCacheServer(cache);
      return cacheServer.getPort();
    });

    String regionName = "region";
    String region2Name = "region2";
    int totalBuckets = 2;
    boolean isSecondRegionLocal = false;
    setupRegions(totalBuckets, regionName, isSecondRegionLocal, region2Name);

    client.invoke(() -> createClientRegion(host, port, regionName, region2Name));

    client.invoke(() -> verifySizeOpTransaction(2, regionName, totalBuckets, region2Name,
        isSecondRegionLocal));

  }

  private CacheServer createCacheServer(Cache cache) throws Exception {
    CacheServer server = cache.addCacheServer();
    server.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    server.start();
    return server;
  }

  private void createClientRegion(final Host host, final int port0, String regionName,
      String region2Name) {
    ClientCacheFactory cf = new ClientCacheFactory();
    cf.addPoolServer(host.getHostName(), port0);
    ClientCache cache = getClientCache(cf);
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(region2Name);
  }


}
