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

import static org.apache.geode.internal.Assert.fail;
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getCache;
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getClientCache;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class PartitionedRegionClearDUnitTest implements Serializable {
  protected static final String REGION_NAME = "testPR";
  protected static final int TOTAL_BUCKET_NUM = 10;
  protected static final int NUM_ENTRIES = 1000;

  protected int locatorPort;
  protected MemberVM locator;
  protected MemberVM dataStore1, dataStore2, dataStore3, accessor;
  protected ClientVM client1, client2;

  private static final Logger logger = LogManager.getLogger();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(7);

  @Before
  public void setUp() throws Exception {
    locator = cluster.startLocatorVM(0);
    locatorPort = locator.getPort();
    dataStore1 = cluster.startServerVM(1, getProperties(), locatorPort);
    dataStore2 = cluster.startServerVM(2, getProperties(), locatorPort);
    dataStore3 = cluster.startServerVM(3, getProperties(), locatorPort);
    accessor = cluster.startServerVM(4, getProperties(), locatorPort);
    client1 = cluster.startClientVM(5,
        c -> c.withPoolSubscription(true).withLocatorConnection((locatorPort)));
    client2 = cluster.startClientVM(6,
        c -> c.withPoolSubscription(true).withLocatorConnection((locatorPort)));
  }

  protected RegionShortcut getRegionShortCut() {
    return RegionShortcut.PARTITION_REDUNDANT;
  }

  protected Properties getProperties() {
    Properties properties = new Properties();
    properties.setProperty("log-level", "info");
    return properties;
  }

  private Region getRegion(boolean isClient) {
    if (isClient) {
      return getClientCache().getRegion(REGION_NAME);
    } else {
      return getCache().getRegion(REGION_NAME);
    }
  }

  private void verifyRegionSize(boolean isClient, int expectedNum) {
    assertThat(getRegion(isClient).size()).isEqualTo(expectedNum);
  }

  private void initClientCache() {
    Region region = getClientCache().createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
        .create(REGION_NAME);
    region.registerInterestForAllKeys(InterestResultPolicy.KEYS);
  }

  private void initDataStore(boolean withWriter) {
    RegionFactory factory = getCache().createRegionFactory(getRegionShortCut())
        .setPartitionAttributes(
            new PartitionAttributesFactory().setTotalNumBuckets(TOTAL_BUCKET_NUM).create());
    if (withWriter) {
      factory.setCacheWriter(new CountingCacheWriter());
    }
    factory.create(REGION_NAME);
    clearsByRegion = new HashMap<>();
    destroysByRegion = new HashMap<>();
  }

  private void initAccessor(boolean withWriter) {
    RegionShortcut shortcut = getRegionShortCut();
    if (shortcut.isPersistent()) {
      if (shortcut == RegionShortcut.PARTITION_PERSISTENT) {
        shortcut = RegionShortcut.PARTITION;
      } else if (shortcut == RegionShortcut.PARTITION_PERSISTENT_OVERFLOW) {
        shortcut = RegionShortcut.PARTITION_OVERFLOW;
      } else if (shortcut == RegionShortcut.PARTITION_REDUNDANT_PERSISTENT) {
        shortcut = RegionShortcut.PARTITION_REDUNDANT;
      } else if (shortcut == RegionShortcut.PARTITION_REDUNDANT_PERSISTENT_OVERFLOW) {
        shortcut = RegionShortcut.PARTITION_REDUNDANT_OVERFLOW;
      } else {
        fail("Wrong region type:" + shortcut);
      }
    }
    RegionFactory factory = getCache().createRegionFactory(shortcut)
        .setPartitionAttributes(
            new PartitionAttributesFactory().setTotalNumBuckets(10).setLocalMaxMemory(0).create())
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(10).create());
    if (withWriter) {
      factory.setCacheWriter(new CountingCacheWriter());
    }
    factory.create(REGION_NAME);
    clearsByRegion = new HashMap<>();
    destroysByRegion = new HashMap<>();
  }

  private void feed(boolean isClient) {
    Region region = getRegion(isClient);
    IntStream.range(0, NUM_ENTRIES).forEach(i -> region.put(i, "value" + i));
  }

  private void verifyServerRegionSize(int expectedNum) {
    accessor.invoke(() -> verifyRegionSize(false, expectedNum));
    dataStore1.invoke(() -> verifyRegionSize(false, expectedNum));
    dataStore2.invoke(() -> verifyRegionSize(false, expectedNum));
    dataStore3.invoke(() -> verifyRegionSize(false, expectedNum));
  }

  private void verifyClientRegionSize(int expectedNum) {
    client1.invoke(() -> verifyRegionSize(true, expectedNum));
    // TODO: notify register clients
    // client2.invoke(()->verifyRegionSize(true, expectedNum));
  }

  SerializableCallableIF<Integer> getWriterClears = () -> {
    int clears =
        clearsByRegion.get(REGION_NAME) == null ? 0 : clearsByRegion.get(REGION_NAME).get();
    return clears;
  };

  SerializableCallableIF<Integer> getWriterDestroys = () -> {
    int destroys =
        destroysByRegion.get(REGION_NAME) == null ? 0 : destroysByRegion.get(REGION_NAME).get();
    return destroys;
  };

  SerializableCallableIF<Integer> getBucketRegionWriterClears = () -> {
    int clears = 0;
    for (int i = 0; i < TOTAL_BUCKET_NUM; i++) {
      String bucketRegionName = "_B__" + REGION_NAME + "_" + i;
      clears += clearsByRegion.get(bucketRegionName) == null ? 0
          : clearsByRegion.get(bucketRegionName).get();
    }
    return clears;
  };

  SerializableCallableIF<Integer> getBucketRegionWriterDestroys = () -> {
    int destroys = 0;
    for (int i = 0; i < TOTAL_BUCKET_NUM; i++) {
      String bucketRegionName = "_B__" + REGION_NAME + "_" + i;
      destroys += destroysByRegion.get(bucketRegionName) == null ? 0
          : destroysByRegion.get(bucketRegionName).get();
    }
    return destroys;
  };

  void configureServers(boolean dataStoreWithWriter, boolean accessorWithWriter) {
    dataStore1.invoke(() -> initDataStore(dataStoreWithWriter));
    dataStore2.invoke(() -> initDataStore(dataStoreWithWriter));
    dataStore3.invoke(() -> initDataStore(dataStoreWithWriter));
    accessor.invoke(() -> initAccessor(accessorWithWriter));
    // make sure only datastore3 has cacheWriter
    dataStore1.invoke(() -> {
      Region region = getRegion(false);
      region.getAttributesMutator().setCacheWriter(null);
    });
    dataStore2.invoke(() -> {
      Region region = getRegion(false);
      region.getAttributesMutator().setCacheWriter(null);
    });
  }

  @Test
  public void normalClearFromDataStoreWithWriterOnDataStore() {
    configureServers(true, true);
    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);

    accessor.invoke(() -> feed(false));
    verifyServerRegionSize(NUM_ENTRIES);
    dataStore3.invoke(() -> getRegion(false).clear());
    verifyServerRegionSize(0);

    // do the region destroy to compare that the same callbacks will be triggered
    dataStore3.invoke(() -> {
      Region region = getRegion(false);
      region.destroyRegion();
    });

    assertThat(dataStore1.invoke(getWriterDestroys)).isEqualTo(dataStore1.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(dataStore2.invoke(getWriterDestroys)).isEqualTo(dataStore2.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(dataStore3.invoke(getWriterDestroys)).isEqualTo(dataStore3.invoke(getWriterClears))
        .isEqualTo(1);
    assertThat(accessor.invoke(getWriterDestroys)).isEqualTo(accessor.invoke(getWriterClears))
        .isEqualTo(0);

    assertThat(dataStore3.invoke(getBucketRegionWriterDestroys))
        .isEqualTo(dataStore3.invoke(getBucketRegionWriterClears))
        .isEqualTo(0);
  }

  @Test
  public void normalClearFromDataStoreWithoutWriterOnDataStore() {
    configureServers(false, true);
    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);

    accessor.invoke(() -> feed(false));
    verifyServerRegionSize(NUM_ENTRIES);
    dataStore1.invoke(() -> getRegion(false).clear());
    verifyServerRegionSize(0);

    // do the region destroy to compare that the same callbacks will be triggered
    dataStore1.invoke(() -> {
      Region region = getRegion(false);
      region.destroyRegion();
    });

    assertThat(dataStore1.invoke(getWriterDestroys)).isEqualTo(dataStore1.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(dataStore2.invoke(getWriterDestroys)).isEqualTo(dataStore2.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(dataStore3.invoke(getWriterDestroys)).isEqualTo(dataStore3.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(accessor.invoke(getWriterDestroys)).isEqualTo(accessor.invoke(getWriterClears))
        .isEqualTo(1);

    assertThat(accessor.invoke(getBucketRegionWriterDestroys))
        .isEqualTo(accessor.invoke(getBucketRegionWriterClears))
        .isEqualTo(0);
  }

  @Test
  public void normalClearFromAccessorWithWriterOnDataStore() {
    configureServers(true, true);
    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);

    accessor.invoke(() -> feed(false));
    verifyServerRegionSize(NUM_ENTRIES);
    accessor.invoke(() -> getRegion(false).clear());
    verifyServerRegionSize(0);

    // do the region destroy to compare that the same callbacks will be triggered
    accessor.invoke(() -> {
      Region region = getRegion(false);
      region.destroyRegion();
    });

    assertThat(dataStore1.invoke(getWriterDestroys)).isEqualTo(dataStore1.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(dataStore2.invoke(getWriterDestroys)).isEqualTo(dataStore2.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(dataStore3.invoke(getWriterDestroys)).isEqualTo(dataStore3.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(accessor.invoke(getWriterDestroys)).isEqualTo(accessor.invoke(getWriterClears))
        .isEqualTo(1);

    assertThat(accessor.invoke(getBucketRegionWriterDestroys))
        .isEqualTo(accessor.invoke(getBucketRegionWriterClears))
        .isEqualTo(0);
  }

  @Test
  public void normalClearFromAccessorWithoutWriterButWithWriterOnDataStore() {
    configureServers(true, false);
    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);

    accessor.invoke(() -> feed(false));
    verifyServerRegionSize(NUM_ENTRIES);
    accessor.invoke(() -> getRegion(false).clear());
    verifyServerRegionSize(0);

    // do the region destroy to compare that the same callbacks will be triggered
    accessor.invoke(() -> {
      Region region = getRegion(false);
      region.destroyRegion();
    });

    assertThat(dataStore1.invoke(getWriterDestroys)).isEqualTo(dataStore1.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(dataStore2.invoke(getWriterDestroys)).isEqualTo(dataStore2.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(dataStore3.invoke(getWriterDestroys)).isEqualTo(dataStore3.invoke(getWriterClears))
        .isEqualTo(1);
    assertThat(accessor.invoke(getWriterDestroys)).isEqualTo(accessor.invoke(getWriterClears))
        .isEqualTo(0);

    assertThat(dataStore3.invoke(getBucketRegionWriterDestroys))
        .isEqualTo(dataStore3.invoke(getBucketRegionWriterClears))
        .isEqualTo(0);
  }

  @Test
  public void normalClearFromClient() {
    configureServers(true, false);
    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);

    client1.invoke(() -> feed(true));
    verifyClientRegionSize(NUM_ENTRIES);
    verifyServerRegionSize(NUM_ENTRIES);

    client1.invoke(() -> getRegion(true).clear());
    verifyServerRegionSize(0);
    verifyClientRegionSize(0);

    // do the region destroy to compare that the same callbacks will be triggered
    client1.invoke(() -> {
      Region region = getRegion(true);
      region.destroyRegion();
    });

    assertThat(dataStore1.invoke(getWriterDestroys)).isEqualTo(dataStore1.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(dataStore2.invoke(getWriterDestroys)).isEqualTo(dataStore2.invoke(getWriterClears))
        .isEqualTo(0);
    assertThat(dataStore3.invoke(getWriterDestroys)).isEqualTo(dataStore3.invoke(getWriterClears))
        .isEqualTo(1);
    assertThat(accessor.invoke(getWriterDestroys)).isEqualTo(accessor.invoke(getWriterClears))
        .isEqualTo(0);

    assertThat(dataStore3.invoke(getBucketRegionWriterDestroys))
        .isEqualTo(dataStore3.invoke(getBucketRegionWriterClears))
        .isEqualTo(0);
  }

  public static HashMap<String, AtomicInteger> clearsByRegion = new HashMap<>();
  public static HashMap<String, AtomicInteger> destroysByRegion = new HashMap<>();

  private static class CountingCacheWriter extends CacheWriterAdapter {
    @Override
    public void beforeRegionClear(RegionEvent event) throws CacheWriterException {
      Region region = event.getRegion();
      AtomicInteger clears = clearsByRegion.get(region.getName());
      if (clears == null) {
        clears = new AtomicInteger(1);
        clearsByRegion.put(region.getName(), clears);
      } else {
        clears.incrementAndGet();
      }
      logger
          .info("Region " + region.getName() + " will be cleared, clear count is:" + clears.get());
    }

    @Override
    public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {
      Region region = event.getRegion();
      AtomicInteger destroys = destroysByRegion.get(region.getName());
      if (destroys == null) {
        destroys = new AtomicInteger(1);
        destroysByRegion.put(region.getName(), destroys);
      } else {
        destroys.incrementAndGet();
      }
      logger.info(
          "Region " + region.getName() + " will be destroyed, destroy count is:" + destroys.get());
    }
  }
}
