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
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getCache;
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getClientCache;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class PartitionedRegionAfterClearNotificationDUnitTest implements Serializable {
  protected static final String REGION_NAME = "testPR";
  protected static final int NUM_ENTRIES = 100;

  protected int locatorPort;
  protected MemberVM locator;
  protected MemberVM dataStore1;
  protected MemberVM dataStore2;
  protected MemberVM dataStore3;
  protected MemberVM accessor;

  protected ClientVM client1;
  protected ClientVM client2;

  private static volatile DUnitBlackboard blackboard;

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

    getBlackboard().initBlackboard();
  }

  protected RegionShortcut getRegionShortCut() {
    return RegionShortcut.PARTITION_REDUNDANT;
  }

  protected Properties getProperties() {
    Properties properties = new Properties();
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
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(getRegion(isClient).size()).isEqualTo(expectedNum));
  }

  private void initClientCache() {
    Region region = getClientCache().createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
        .create(REGION_NAME);
    region.registerInterestForAllKeys(InterestResultPolicy.KEYS);
  }

  private void stopServers() {
    List<CacheServer> cacheServers = getCache().getCacheServers();
    for (CacheServer server : cacheServers) {
      server.stop();
    }
  }

  private void initDataStore() {
    getCache().createRegionFactory(getRegionShortCut())
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(10).create())
        .addCacheListener(new CountingCacheListener())
        .create(REGION_NAME);
  }

  private void initAccessor() {
    RegionShortcut shortcut = getRegionShortCut();
    getCache().createRegionFactory(shortcut)
        .setPartitionAttributes(
            new PartitionAttributesFactory().setTotalNumBuckets(10).setLocalMaxMemory(0).create())
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
    client2.invoke(() -> verifyRegionSize(true, expectedNum));
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
    assertThat(count).isEqualTo(4);

    if (serverVM != null) {
      assertThat(serverVM.invoke(getListenerTriggerCount)).isEqualTo(1);
    }
  }

  @Test
  public void invokeClearOnDataStoreAndVerifyListenerCount() {
    accessor.invoke(() -> feed(false));
    verifyServerRegionSize(NUM_ENTRIES);

    dataStore1.invoke(() -> getRegion(false).clear());

    verifyServerRegionSize(0);
    verifyCacheListenerTriggerCount(dataStore1);
  }

  @Test
  public void invokeClearOnAccessorAndVerifyListenerCount() {
    accessor.invoke(() -> feed(false));
    verifyServerRegionSize(NUM_ENTRIES);
    accessor.invoke(() -> getRegion(false).clear());
    verifyServerRegionSize(0);
    verifyCacheListenerTriggerCount(accessor);
  }

  @Test
  public void invokeClearFromClientAndVerifyListenerCount() {
    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);

    client1.invoke(() -> feed(true));
    verifyClientRegionSize(NUM_ENTRIES);
    verifyServerRegionSize(NUM_ENTRIES);

    client1.invoke(() -> getRegion(true).clear());

    verifyServerRegionSize(0);
    verifyClientRegionSize(0);
    verifyCacheListenerTriggerCount(null);
  }

  @Test
  public void invokeClearFromClientWithAccessorAsServer() {
    dataStore1.invoke(this::stopServers);
    dataStore2.invoke(this::stopServers);
    dataStore3.invoke(this::stopServers);

    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);

    client1.invoke(() -> feed(true));
    verifyClientRegionSize(NUM_ENTRIES);
    verifyServerRegionSize(NUM_ENTRIES);

    client1.invoke(() -> getRegion(true).clear());

    verifyServerRegionSize(0);
    verifyClientRegionSize(0);
    verifyCacheListenerTriggerCount(null);
  }

  @Test
  public void invokeClearFromDataStoreWithClientInterest() {
    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);

    accessor.invoke(() -> feed(false));
    verifyServerRegionSize(NUM_ENTRIES);

    dataStore1.invoke(() -> getRegion(false).clear());

    verifyServerRegionSize(0);
    verifyCacheListenerTriggerCount(dataStore1);
  }

  @Test(expected = AssertionError.class)
  public void verifyTheLocksAreClearedWhenMemberDepartsAfterTakingClearLockOnRemoteMembers()
      throws Exception {
    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);

    accessor.invoke(() -> feed(false));
    verifyServerRegionSize(NUM_ENTRIES);
    dataStore2.invoke(() -> DistributionMessageObserver.setInstance(
        testHookToKillMemberCallingClearBeforeMessageProcessed()));

    AsyncInvocation ds1ClearAsync = dataStore1.invokeAsync(() -> getRegion(false).clear());

    getBlackboard().waitForGate("CLOSE_CACHE", 30, SECONDS);

    dataStore1.invoke(() -> getCache().close());
    getBlackboard().signalGate("CACHE_CLOSED");

    // This should not be blocked.
    dataStore2.invoke(() -> feed(false));
    dataStore3.invoke(() -> feed(false));

    dataStore2.invoke(() -> verifyRegionSize(false, NUM_ENTRIES));
    dataStore3.invoke(() -> verifyRegionSize(false, NUM_ENTRIES));

    ds1ClearAsync.await();
  }

  @Test
  public void verifyTheLocksAreClearedWhenMemberDepartsAfterTakingClearLockOnRemoteMembersAfterMessageProcessed()
      throws Exception {
    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);

    accessor.invoke(() -> feed(false));
    verifyServerRegionSize(NUM_ENTRIES);

    dataStore2.invoke(() -> DistributionMessageObserver.setInstance(
        testHookToKillMemberCallingClearAfterMessageProcessed()));

    AsyncInvocation ds1ClearAsync = dataStore1.invokeAsync(() -> getRegion(false).clear());

    getBlackboard().waitForGate("CLOSE_CACHE", 30, SECONDS);

    dataStore1.invoke(() -> getCache().close());
    getBlackboard().signalGate("CACHE_CLOSED");

    // This should not be blocked.
    dataStore2.invoke(() -> feed(false));
    dataStore3.invoke(() -> feed(false));

    dataStore2.invoke(() -> verifyRegionSize(false, NUM_ENTRIES));
    dataStore3.invoke(() -> verifyRegionSize(false, NUM_ENTRIES));

    ds1ClearAsync.await();
  }


  private static class CountingCacheListener extends CacheListenerAdapter {
    private final AtomicInteger clears = new AtomicInteger();

    @Override
    public void afterRegionClear(RegionEvent event) {
      clears.incrementAndGet();
    }

    int getClears() {
      return clears.get();

    }
  }

  private DistributionMessageObserver testHookToKillMemberCallingClearBeforeMessageProcessed() {
    return new DistributionMessageObserver() {

      @Override
      public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
        if (message instanceof PartitionedRegionClearMessage) {
          if (((PartitionedRegionClearMessage) message)
              .getOperationType() == PartitionedRegionClearMessage.OperationType.OP_LOCK_FOR_PR_CLEAR) {
            DistributionMessageObserver.setInstance(null);
            getBlackboard().signalGate("CLOSE_CACHE");
            try {
              getBlackboard().waitForGate("CACHE_CLOSED", 30, SECONDS);
              GeodeAwaitility.await().untilAsserted(
                  () -> assertThat(dm.isCurrentMember(message.getSender())).isFalse());
            } catch (TimeoutException | InterruptedException e) {
              throw new RuntimeException("Failed waiting for signal.");
            }
          }
        }
      }
    };
  }

  private DistributionMessageObserver testHookToKillMemberCallingClearAfterMessageProcessed() {
    return new DistributionMessageObserver() {
      @Override
      public void afterProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
        if (message instanceof PartitionedRegionClearMessage) {
          if (((PartitionedRegionClearMessage) message)
              .getOperationType() == PartitionedRegionClearMessage.OperationType.OP_LOCK_FOR_PR_CLEAR) {
            DistributionMessageObserver.setInstance(null);
            getBlackboard().signalGate("CLOSE_CACHE");
            try {
              getBlackboard().waitForGate("CACHE_CLOSED", 30, SECONDS);
            } catch (TimeoutException | InterruptedException e) {
              throw new RuntimeException("Failed waiting for signal.");
            }
          }
        }
      }
    };
  }

  private static DUnitBlackboard getBlackboard() {
    if (blackboard == null) {
      blackboard = new DUnitBlackboard();
    }
    return blackboard;
  }

}
