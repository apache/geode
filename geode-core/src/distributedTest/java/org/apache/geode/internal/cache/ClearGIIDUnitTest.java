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

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.internal.cache.InitialImageOperation.getGIITestHookForCheckingPurpose;
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getCache;
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getClientCache;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionedRegionPartialClearException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;


@RunWith(Parameterized.class)
public class ClearGIIDUnitTest implements Serializable {


  protected static final String REGION_NAME = "testPR";
  protected static final String INDEX_NAME = "testIndex";
  protected static final int TOTAL_BUCKET_NUM = 10;
  protected static final int REDUNDANT_COPIES = 1;
  protected static final int DATA_SIZE = 100;
  protected static final int NUM_SERVERS = 2;

  @Parameterized.Parameter(0)
  public RegionShortcut regionShortcut;

  protected int locatorPort;
  protected MemberVM locator;
  protected MemberVM[] serverVMs;
  protected ClientVM client;

  private static final Logger logger = LogManager.getLogger();

  @Parameterized.Parameters
  public static Collection<Object[]> getRegionShortcuts() {
    List<Object[]> params = new ArrayList<>();
    params.add(new Object[] {PARTITION});
    params.add(new Object[] {REPLICATE});
    return params;
  }


  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(4);

  @Before
  public void setUp() throws Exception {
    locator = cluster.startLocatorVM(0);
    locatorPort = locator.getPort();
  }

  private void initDataStore(RegionShortcut regionShortcut, boolean isPartitioned) {
    RegionFactory factory = getCache().createRegionFactory(regionShortcut);
    if (isPartitioned) {
      factory.setPartitionAttributes(
          new PartitionAttributesFactory().setTotalNumBuckets(TOTAL_BUCKET_NUM)
              .setRedundantCopies(REDUNDANT_COPIES)
              .create());
    }

    factory.create(REGION_NAME);
  }

  private void startServers(int numberOfMembers) {
    serverVMs = new MemberVM[numberOfMembers];
    for (int i = 0; i < numberOfMembers; i++) {
      serverVMs[i] = cluster.startServerVM(i + 1, locatorPort);
    }

    createAndPopulateRegion();
  }

  private void invokeClear(MemberVM datastore) {
    datastore.invoke(() -> getCache().getRegion(REGION_NAME).clear());
  }

  private AsyncInvocation invokeClearAsync(MemberVM datastore) {
    return datastore.invokeAsync(() -> getCache().getRegion(REGION_NAME).clear());
  }

  private void createAndPopulateRegion() {
    for (MemberVM datastore : serverVMs) {
      datastore.invoke(() -> initDataStore(regionShortcut, isPartitioned()));
    }

    serverVMs[0].invoke(() -> {
      Map<String, String> dataMap = new HashMap<String, String>();

      for (int i = 0; i < DATA_SIZE; i++) {
        dataMap.put("key" + i, String.valueOf(i));
      }

      getCache().getRegion(REGION_NAME).putAll(dataMap);

    });
  }

  private void createDelta(int deltaSize) {
    serverVMs[0].invoke(() -> {
      Map<String, String> dataMap = new HashMap<String, String>();

      for (int i = 0; i < deltaSize; i++) {
        dataMap.put("key" + (i + DATA_SIZE), String.valueOf(i + DATA_SIZE));
      }

      getCache().getRegion(REGION_NAME).putAll(dataMap);

    });
  }

  private boolean isPartitioned() {
    if (regionShortcut == RegionShortcut.PARTITION
        || regionShortcut == RegionShortcut.PARTITION_PERSISTENT) {
      return true;
    } else {
      return false;
    }
  }

  void restartServerOnVM(int index) {
    cluster.startServerVM(index, locatorPort);
  }

  private void verifyRegionSize(int expectedNum) {
    Region region = getCache().getRegion(REGION_NAME);
    assertThat(region.size()).isEqualTo(expectedNum);
  }

  @Test
  public void GIICompletesSuccessfullyWhenRunningDuringClear() {
    startServers(NUM_SERVERS);
    verifyRegionSizes(DATA_SIZE);

    // set tesk hook
    serverVMs[1].invoke(() -> {
      PauseDuringGIICallback myAfterReceivedImageReply =
          // using bucket name for region name to ensure callback is triggered
          new PauseDuringGIICallback(
              InitialImageOperation.GIITestHookType.AfterReceivedRequestImage, "_B__testPR_9");
      InitialImageOperation.setGIITestHook(myAfterReceivedImageReply);
    });

    cluster.stop(2);

    createDelta(50);

    restartServerOnVM(2);

    AsyncInvocation async = createRegionAsync(serverVMs[1]);
    invokeClear(serverVMs[0]);

    serverVMs[1].invoke(() -> InitialImageOperation.resetGIITestHook(
        InitialImageOperation.GIITestHookType.AfterReceivedRequestImage,
        true));
    try {
      async.getResult(30000);
    } catch (InterruptedException ex) {
      Assert.fail("Async create interupted" + ex.getMessage());
    }

    verifyRegionSizes(0);
  }

  @Test
  public void GIICompletesSuccessfullyWhenRunningDuringClearWithThirdDatastore() {
    startServers(NUM_SERVERS + 1);
    verifyRegionSizes(DATA_SIZE);

    // set tesk hook
    serverVMs[1].invoke(() -> {
      PauseDuringGIICallback myAfterReceivedImageReply =
          // using bucket name for region name to ensure callback is triggered
          new PauseDuringGIICallback(
              InitialImageOperation.GIITestHookType.AfterReceivedRequestImage, "_B__testPR_9");
      InitialImageOperation.setGIITestHook(myAfterReceivedImageReply);
    });

    cluster.stop(2);

    createDelta(50);

    restartServerOnVM(2);

    AsyncInvocation async = createRegionAsync(serverVMs[1]);
    invokeClear(serverVMs[0]);

    serverVMs[1].invoke(() -> InitialImageOperation.resetGIITestHook(
        InitialImageOperation.GIITestHookType.AfterReceivedRequestImage,
        true));
    try {
      async.getResult(30000);
    } catch (InterruptedException ex) {
      Assert.fail("Async create interupted" + ex.getMessage());
    }

    verifyRegionSizes(0);
  }

  @Test
  public void GIICompletesSuccessfullyWhenRunningDuringClearWithPersistentRegion() {
    if (regionShortcut == RegionShortcut.PARTITION) {
      regionShortcut = RegionShortcut.PARTITION_PERSISTENT;
    } else if (regionShortcut == RegionShortcut.REPLICATE) {
      regionShortcut = RegionShortcut.REPLICATE_PERSISTENT;
    }
    startServers(NUM_SERVERS);

    verifyRegionSizes(DATA_SIZE);

    // set tesk hook
    serverVMs[1].invoke(() -> {
      PauseDuringGIICallback myAfterReceivedImageReply =
          // using bucket name for region name to ensure callback is triggered
          new PauseDuringGIICallback(
              InitialImageOperation.GIITestHookType.AfterReceivedRequestImage, "_B__testPR_9");
      InitialImageOperation.setGIITestHook(myAfterReceivedImageReply);
    });

    cluster.stop(2);

    createDelta(50);

    restartServerOnVM(2);

    AsyncInvocation async = createRegionAsync(serverVMs[1]);
    invokeClear(serverVMs[0]);

    serverVMs[1].invoke(() -> InitialImageOperation.resetGIITestHook(
        InitialImageOperation.GIITestHookType.AfterReceivedRequestImage,
        true));
    try {
      async.getResult(30000);
    } catch (InterruptedException ex) {
      Assert.fail("Async create interupted" + ex.getMessage());
    }

    verifyRegionSizes(0);
  }

  @Test
  public void GIICompletesSuccessfullyWhenRunningDuringClearWithIndexing() {
    startServers(NUM_SERVERS);

    verifyRegionSizes(DATA_SIZE);
    createIndex(serverVMs[0]);

    // set tesk hook
    serverVMs[1].invoke(new SerializableRunnable() {
      @Override
      public void run() {
        PauseDuringGIICallback myAfterReceivedImageReply =
            // using bucket name for region name to ensure callback is triggered
            new PauseDuringGIICallback(
                InitialImageOperation.GIITestHookType.AfterReceivedRequestImage, "_B__testPR_9");
        InitialImageOperation.setGIITestHook(myAfterReceivedImageReply);
      }
    });

    cluster.stop(2);

    createDelta(50);

    restartServerOnVM(2);

    AsyncInvocation async = createRegionAsync(serverVMs[1]);
    invokeClear(serverVMs[0]);

    serverVMs[1].invoke(() -> InitialImageOperation.resetGIITestHook(
        InitialImageOperation.GIITestHookType.AfterReceivedRequestImage,
        true));
    try {
      async.getResult(30000);
      if (!isPartitioned()) {
        createIndex(serverVMs[1]);
      }
    } catch (InterruptedException ex) {
      Assert.fail("Async create interupted" + ex.getMessage());
    }

    verifyRegionSizes(0);
    verifyIndexSize(0);
  }

  @Test
  public void ClearThrowsPartialClearExceptionWhenRunningDuringGIIP2P() {
    startServers(NUM_SERVERS);
    int deltaSize = 50;

    serverVMs[0].invoke(() -> {
      DistributionMessageObserver.setInstance(new PauseDuringClearDistributionMessageObserver());
    });


    verifyRegionSizes(DATA_SIZE);

    AsyncInvocation async = invokeClearAsync(serverVMs[0]);

    cluster.stop(2);

    createDelta(deltaSize);

    restartServerOnVM(2);

    createRegion(serverVMs[1]);

    serverVMs[0].invoke(() -> {
      PauseDuringClearDistributionMessageObserver observer =
          (PauseDuringClearDistributionMessageObserver) DistributionMessageObserver.getInstance();
      DistributionMessageObserver.setInstance(null);
      observer.latch.countDown();
    });

    boolean caughtException = false;
    try {
      async.getResult(30000);
    } catch (Throwable ex) {
      caughtException = true;
      assertThat(ex.getCause().getClass() == PartitionedRegionPartialClearException.class);
    }
    if (isPartitioned()) {
      assertThat(caughtException).isTrue();
    } else {
      verifyRegionSizes(DATA_SIZE / NUM_SERVERS);
    }
  }

  public void verifyRegionSizes(int expectedSize) {
    for (MemberVM datastore : serverVMs) {
      datastore.invoke(() -> verifyRegionSize(expectedSize));
    }
  }

  public void verifyIndexSize(int expectedSize) {
    for (MemberVM vm : serverVMs) {
      vm.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        QueryService queryService = cache.getQueryService();
        Region region = cache.getRegion(REGION_NAME);
        assertThat(region.size()).isEqualTo(expectedSize);

        Index index = queryService.getIndex(region, INDEX_NAME);
        IndexStatistics stats = index.getStatistics();
        assertThat(stats.getNumberOfKeys()).isEqualTo(expectedSize);
      });
    }
  }

  public void waitForCallbackStarted(final MemberVM vm,
      final InitialImageOperation.GIITestHookType callbacktype) {
    SerializableRunnable waitForCallbackStarted = new SerializableRunnable() {
      @Override
      public void run() {

        final InitialImageOperation.GIITestHook callback =
            getGIITestHookForCheckingPurpose(callbacktype);
        WaitCriterion ev = new WaitCriterion() {

          @Override
          public boolean done() {
            return (callback != null && callback.isRunning);
          }

          @Override
          public String description() {
            return null;
          }
        };

        GeodeAwaitility.await().untilAsserted(ev);
        if (callback == null || !callback.isRunning) {
          fail("GII tesk hook is not started yet");
        }
      }
    };
    vm.invoke(waitForCallbackStarted);
  }

  protected AsyncInvocation createRegionAsync(MemberVM vm) {
    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
      @Override
      public void run() {
        try {
          initDataStore(regionShortcut, isPartitioned());
        } catch (CacheException ex) {
          Assert.fail("While creating region", ex);
        }
      }
    };
    return vm.invokeAsync(createRegion);
  }

  protected void createIndex(MemberVM vm) {
    vm.invoke("create index", () -> {
      Cache cache = ClusterStartupRule.getCache();
      QueryService queryService = cache.getQueryService();
      queryService.createKeyIndex(INDEX_NAME, "c", "/" + REGION_NAME + " c");
      assertThat(queryService.getIndexes(cache.getRegion(REGION_NAME))).extracting(Index::getName)
          .contains(INDEX_NAME);
    });
  }

  protected void createRegion(MemberVM vm) {
    vm.invoke(() -> {
      initDataStore(regionShortcut, isPartitioned());
    });
  }

  protected void createClientRegion(ClientVM vm) {
    vm.invoke(() -> {
      getClientCache().createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .create(REGION_NAME);
    });
  }

  private class PauseDuringGIICallback extends InitialImageOperation.GIITestHook {
    private Object lockObject = new Object();

    public PauseDuringGIICallback(InitialImageOperation.GIITestHookType type, String region_name) {
      super(type, region_name);
    }

    @Override
    public void reset() {
      synchronized (this.lockObject) {
        this.lockObject.notify();
      }
    }

    @Override
    public void run() {
      synchronized (this.lockObject) {
        try {
          isRunning = true;
          this.lockObject.wait();
        } catch (InterruptedException e) {
        }
      }
    }
  } // Mycallback

  private static class PauseDuringClearDistributionMessageObserver
      extends DistributionMessageObserver {
    private CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void beforeSendMessage(ClusterDistributionManager dm,
        DistributionMessage message) {
      if (message instanceof PartitionedRegionClearMessage) {
        try {
          latch.await();
        } catch (InterruptedException ex) {

        }

      }
    }
  }
}
