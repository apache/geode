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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class PartitionedRegionClearDUnitTest implements Serializable {
  protected static final String REGION_NAME = "testPR";
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
    dataStore1.invoke(this::initDataStore);
    dataStore2.invoke(this::initDataStore);
    dataStore3.invoke(this::initDataStore);
    accessor.invoke(this::initAccessor);
    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);
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

  private void initDataStore() {
    getCache().createRegionFactory(getRegionShortCut())
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(10).create())
        .addCacheListener(new CountingCacheListener())
        .create(REGION_NAME);
  }

  private void initAccessor() {
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
    getCache().createRegionFactory(shortcut)
        .setPartitionAttributes(
            new PartitionAttributesFactory().setTotalNumBuckets(10).setLocalMaxMemory(0).create())
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(10).create())
        .addCacheListener(new CountingCacheListener())
        .create(REGION_NAME);
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

  private void verifyCacheListenerTriggerCount(MemberVM serverVM) {
    SerializableCallableIF<Integer> getListenerTriggerCount = () -> {
      CountingCacheListener countingCacheListener =
          (CountingCacheListener) getRegion(false).getAttributes()
              .getCacheListeners()[0];
      return countingCacheListener.getClears();
    };

    int count = accessor.invoke(getListenerTriggerCount)
        + dataStore1.invoke(getListenerTriggerCount)
        + dataStore2.invoke(getListenerTriggerCount)
        + dataStore3.invoke(getListenerTriggerCount);
    assertThat(count).isEqualTo(1);

    if (serverVM != null) {
      assertThat(serverVM.invoke(getListenerTriggerCount)).isEqualTo(1);
    }
  }

  @Test
  public void normalClearFromDataStore() {
    accessor.invoke(() -> feed(false));
    verifyServerRegionSize(NUM_ENTRIES);
    dataStore1.invoke(() -> getRegion(false).clear());
    verifyServerRegionSize(0);
    verifyCacheListenerTriggerCount(dataStore1);
  }

  @Test
  public void normalClearFromAccessor() {
    accessor.invoke(() -> feed(false));
    verifyServerRegionSize(NUM_ENTRIES);
    accessor.invoke(() -> getRegion(false).clear());
    verifyServerRegionSize(0);
    verifyCacheListenerTriggerCount(accessor);
  }

  @Test
  public void normalClearFromClient() {
    client1.invoke(() -> feed(true));
    verifyClientRegionSize(NUM_ENTRIES);
    verifyServerRegionSize(NUM_ENTRIES);

    client1.invoke(() -> getRegion(true).clear());
    verifyServerRegionSize(0);
    verifyClientRegionSize(0);
    verifyCacheListenerTriggerCount(null);
  }

  private static class CountingCacheListener extends CacheListenerAdapter {
    private final AtomicInteger clears = new AtomicInteger();

    @Override
    public void afterRegionClear(RegionEvent event) {
      Region region = event.getRegion();
      logger.info("Region " + region.getFullPath() + " is cleared.");
      clears.incrementAndGet();
    }

    int getClears() {
      return clears.get();
    }
  }
}
