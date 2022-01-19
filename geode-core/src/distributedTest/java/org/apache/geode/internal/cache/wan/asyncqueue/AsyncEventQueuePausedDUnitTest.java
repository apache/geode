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
package org.apache.geode.internal.cache.wan.asyncqueue;


import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.core.ConditionTimeoutException;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.AEQTest;

@Category({AEQTest.class})
@RunWith(Parameterized.class)
public class AsyncEventQueuePausedDUnitTest implements Serializable {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        // RegionType , isParallel AEQ
        {RegionShortcut.PARTITION, true},
        {RegionShortcut.PARTITION, false},
        {RegionShortcut.REPLICATE, false},
        {RegionShortcut.PARTITION_PERSISTENT, true},
        {RegionShortcut.PARTITION_PERSISTENT, false},
        {RegionShortcut.PARTITION_REDUNDANT, true},
        {RegionShortcut.PARTITION_REDUNDANT, false},
        {RegionShortcut.REPLICATE_PERSISTENT, false},
        {RegionShortcut.PARTITION_REDUNDANT_PERSISTENT, true},
        {RegionShortcut.PARTITION_REDUNDANT_PERSISTENT, false}
    });
  }

  @Parameterized.Parameter
  public static RegionShortcut regionShortcut;

  @Parameterized.Parameter(1)
  public static boolean isParallel;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule(4);

  private static MemberVM locator, server1, server2;
  private static ClientVM client;

  @Before
  public void beforeClass() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, "group1", locator.getPort());
    server2 = lsRule.startServerVM(2, "group1", locator.getPort());
    int serverPort = server1.getPort();
    client =
        lsRule.startClientVM(3, new Properties(),
            clientCacheFactory -> configureClientCacheFactory(clientCacheFactory, serverPort));
  }

  private static void configureClientCacheFactory(ClientCacheFactory ccf, int... serverPorts) {
    for (int serverPort : serverPorts) {
      ccf.addPoolServer("localhost", serverPort);
    }
    ccf.setPoolReadTimeout(10 * 60 * 1000); // 10 min
    ccf.setPoolSubscriptionEnabled(true);
  }

  @Test
  public void whenAEQCreatedInPausedStateThenListenersMustNotBeInvoked() {
    final AEQandRegionProperties props = new AEQandRegionProperties(regionShortcut, isParallel);
    server1.invoke(() -> {
      createRegionAndDispatchingPausedAEQ(props);
    });
    server2.invoke(() -> {
      createRegionAndDispatchingPausedAEQ(props);
    });
    client.invoke(AsyncEventQueuePausedDUnitTest::createClientRegion);

    server1.invoke(AsyncEventQueuePausedDUnitTest::validateAEQDispatchingIsPaused);

    server2.invoke(AsyncEventQueuePausedDUnitTest::validateAEQDispatchingIsPaused);

    // Resume dispatching.
    server1.invoke(() -> {
      ClusterStartupRule.getCache().getAsyncEventQueue("aeqID").resumeEventDispatching();
    });

    server2.invoke(() -> {
      ClusterStartupRule.getCache().getAsyncEventQueue("aeqID").resumeEventDispatching();
    });

    // Validate dispatching resumed.
    await().atMost(1, TimeUnit.MINUTES).until(() -> {

      final int count1 = server1.invoke(AsyncEventQueuePausedDUnitTest::getEventDispatchedSize);
      final int count2 = server2.invoke(AsyncEventQueuePausedDUnitTest::getEventDispatchedSize);
      return (count1 + count2) == 1000;
    });

  }

  @NotNull
  private static Integer getEventDispatchedSize() {
    Cache cache = ClusterStartupRule.getCache();
    AsyncEventQueueImpl aeq = (AsyncEventQueueImpl) cache.getAsyncEventQueue("aeqID");
    MyAsyncEventListener listener = (MyAsyncEventListener) aeq.getAsyncEventListener();
    return listener.getEventsMap().size();
  }

  private static void createClientRegion() {
    ClientCache cache = ClusterStartupRule.getClientCache();
    Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create("region");
    for (int i = 0; i < 1000; i++) {
      region.put(i, i);
    }
  }

  private static void createRegionAndDispatchingPausedAEQ(AEQandRegionProperties props) {
    Cache cache = ClusterStartupRule.getCache();
    cache.createAsyncEventQueueFactory()
        .pauseEventDispatching()
        .setParallel(props.isParallel())
        .setPersistent(isPersistent(props))
        .create("aeqID", new MyAsyncEventListener());
    cache.createRegionFactory(props.getRegionShortcut())
        .addAsyncEventQueueId("aeqID")
        .create("region");
  }

  private static boolean isPersistent(AEQandRegionProperties props) {
    switch (props.getRegionShortcut()) {
      case PARTITION:
      case REPLICATE:
      case PARTITION_REDUNDANT:
        return false;
      default:
        return true;
    }
  }

  private static void validateAEQDispatchingIsPaused() {
    Cache cache = ClusterStartupRule.getCache();
    AsyncEventQueueImpl aeq = (AsyncEventQueueImpl) cache.getAsyncEventQueue("aeqID");
    assertTrue(aeq.isDispatchingPaused());
    MyAsyncEventListener listener = (MyAsyncEventListener) aeq.getAsyncEventListener();
    try {
      await().atMost(10, TimeUnit.SECONDS).until(() -> listener.getEventsMap().size() > 0);
    } catch (ConditionTimeoutException ex) {
      // Expected Exception
    }
    // Ensure that the the queues are filling up
    assertTrue(aeq.getSender().getQueues().stream().mapToInt(RegionQueue::size).sum() == 1000);
  }

  class AEQandRegionProperties implements DataSerializable, Serializable {
    RegionShortcut regionShortcut;
    boolean isParallel;

    public AEQandRegionProperties(RegionShortcut regionShortcut, boolean isParallel) {
      this.regionShortcut = regionShortcut;
      this.isParallel = isParallel;
    }

    public RegionShortcut getRegionShortcut() {
      return regionShortcut;
    }

    public void setRegionShortcut(RegionShortcut regionShortcut) {
      this.regionShortcut = regionShortcut;
    }

    public boolean isParallel() {
      return isParallel;
    }

    public void setParallel(boolean parallel) {
      isParallel = parallel;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeObject(regionShortcut, out);
      out.writeBoolean(isParallel);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      regionShortcut = DataSerializer.readObject(in);
      isParallel = in.readBoolean();
    }
  }

}
