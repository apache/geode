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

import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_PERSISTENT;
import static org.apache.geode.internal.cache.InitialImageOperation.getGIITestHookForCheckingPurpose;
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getCache;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionedRegionPartialClearException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@RunWith(Parameterized.class)
public class ClearPRDuringGIIProviderDUnitTest implements Serializable {
  protected static final String REGION_NAME = "testPR";
  protected static final int TOTAL_BUCKET_NUM = 10;
  protected static final int DATA_SIZE = 100;
  protected static final int NUM_SERVERS = 3;

  @Parameterized.Parameter(0)
  public RegionShortcut regionShortcut;

  @Parameterized.Parameter(1)
  public InitialImageOperation.GIITestHookType giiTestHookType;

  protected int locatorPort;
  protected MemberVM[] memberVMS;

  private static final Logger logger = LogManager.getLogger();

  static RegionShortcut[] regionTypes() {
    return new RegionShortcut[] {
        PARTITION_REDUNDANT, PARTITION_REDUNDANT_OVERFLOW, PARTITION_REDUNDANT_PERSISTENT
    };
  }

  @Parameterized.Parameters(name = "{index}: regionShortcut={0} {1}")
  public static Collection<Object[]> getCombinations() {
    List<Object[]> params = new ArrayList<>();
    RegionShortcut[] regionShortcuts = regionTypes();
    Arrays.stream(regionShortcuts).forEach(regionShortcut -> {
      params.add(
          new Object[] {regionShortcut, InitialImageOperation.GIITestHookType.AfterSentImageReply});
      params.add(
          new Object[] {regionShortcut, InitialImageOperation.GIITestHookType.DuringPackingImage});
      params.add(new Object[] {regionShortcut,
          InitialImageOperation.GIITestHookType.AfterReceivedRequestImage});
    });
    return params;
  }

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(NUM_SERVERS + 1);

  @Before
  public void setUp() throws Exception {
    // serverVMs[1] is the accessor, which is feeder and invokes clear
    // serverVMs[2] will add GII TestHook of requester
    // serverVMs[3] will be GII provider for the specified bucket region
    memberVMS = new MemberVM[NUM_SERVERS + 1];
    memberVMS[0] = cluster.startLocatorVM(0);
    locatorPort = memberVMS[0].getPort();
    memberVMS[1] = cluster.startServerVM(1, locatorPort);
    memberVMS[1].invoke(() -> initAccessor());
    for (int i = 2; i <= NUM_SERVERS; i++) {
      memberVMS[i] = cluster.startServerVM(i, locatorPort);
      memberVMS[i].invoke(() -> initDataStore(regionShortcut));
    }
    feed("valueOne");
    verifyRegionSizes(DATA_SIZE);
  }

  @After
  public final void preTearDown() throws Exception {
    for (int i = 1; i <= NUM_SERVERS; i++) {
      memberVMS[i].invoke(() -> InitialImageOperation.resetAllGIITestHooks());
    }
  }

  private void initDataStore(RegionShortcut regionShortcut) {
    RegionFactory factory = getCache().createRegionFactory(regionShortcut);
    factory.setPartitionAttributes(
        new PartitionAttributesFactory().setTotalNumBuckets(TOTAL_BUCKET_NUM).create());
    factory.create(REGION_NAME);
  }

  private void initAccessor() {
    RegionFactory factory = getCache().createRegionFactory(PARTITION_REDUNDANT);
    factory.setPartitionAttributes(new PartitionAttributesFactory()
        .setTotalNumBuckets(TOTAL_BUCKET_NUM).setLocalMaxMemory(0).create());
    factory.create(REGION_NAME);
  }

  private void feed(String valueStub) {
    memberVMS[1].invoke(() -> {
      Region region = getCache().getRegion(REGION_NAME);
      IntStream.range(0, DATA_SIZE).forEach(i -> region.put(i, valueStub + i));
    });
  }

  private void verifyRegionSize(int expectedNum) {
    Region region = getCache().getRegion(REGION_NAME);
    assertThat(region.size()).isEqualTo(expectedNum);
  }

  protected void giiTestHookSyncWithClear(boolean clearBeforeGII) {
    // set test hook at server3, the provider
    memberVMS[3].invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      List<Integer> localBucketList = pr.getLocalBucketsListTestOnly();
      final String bucketName = "_B__testPR_" + localBucketList.get(0);

      PauseDuringGIICallback myGIITestHook =
          // using bucket name for region name to ensure callback is triggered
          new PauseDuringGIICallback(giiTestHookType, bucketName);
      InitialImageOperation.setGIITestHook(myGIITestHook);
    });

    memberVMS[2].invoke(() -> getCache().getRegion(REGION_NAME).close());
    feed("valueTwo");

    for (int i = 1; i < memberVMS.length; i++) {
      memberVMS[i].invoke(() -> {
        DistributionMessageObserver.setInstance(new PauseDuringClearDistributionMessageObserver());
      });
    }

    AsyncInvocation asyncGII = memberVMS[2].invokeAsync(() -> initDataStore(regionShortcut));
    AsyncInvocation asyncClear =
        memberVMS[1].invokeAsync(() -> getCache().getRegion(REGION_NAME).clear());

    waitForGIITeskHookStarted(memberVMS[3], giiTestHookType);

    if (clearBeforeGII) {
      for (int i = 1; i < memberVMS.length; i++) {
        memberVMS[i].invoke(() -> {
          PauseDuringClearDistributionMessageObserver observer =
              (PauseDuringClearDistributionMessageObserver) DistributionMessageObserver
                  .getInstance();
          DistributionMessageObserver.setInstance(null);
          observer.latch.countDown();
        });
      }

      memberVMS[3].invoke(() -> {
        InitialImageOperation.resetGIITestHook(giiTestHookType, true);
      });
    } else {
      memberVMS[3].invoke(() -> {
        InitialImageOperation.resetGIITestHook(giiTestHookType, true);
      });

      for (int i = 1; i < memberVMS.length; i++) {
        memberVMS[i].invoke(() -> {
          PauseDuringClearDistributionMessageObserver observer =
              (PauseDuringClearDistributionMessageObserver) DistributionMessageObserver
                  .getInstance();
          DistributionMessageObserver.setInstance(null);
          observer.latch.countDown();
        });
      }
    }

    try {
      asyncGII.join(10000);
    } catch (InterruptedException ex) {
      Assert.fail("Async recreate region interupted" + ex.getMessage());
    }
    try {
      asyncClear.join(10000);
    } catch (InterruptedException ex) {
      Assert.fail("Async clear interupted" + ex.getMessage());
    }

    if (asyncClear.exceptionOccurred()) {
      assertThat(asyncClear.getException() instanceof PartitionedRegionPartialClearException);
    } else {
      verifyRegionSizes(0);
    }
  }

  @Test
  public void clearBeforeGIIShouldClearTheRegion() {
    giiTestHookSyncWithClear(true);
  }

  @Test
  public void clearAfterGIIShouldClearTheRegion() {
    giiTestHookSyncWithClear(false);
  }

  private void verifyRegionSizes(int expectedSize) {
    for (int i = 2; i < memberVMS.length; i++) {
      memberVMS[i].invoke(() -> {
        PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
        for (BucketRegion br : pr.getDataStore().getAllLocalBucketRegions()) {
          logger.info("verifyRegionSizes:" + br.getFullPath() + ":"
              + br.getBucketAdvisor().isPrimary() + ":" + br.size());
        }
      });
    }
    for (int i = 1; i < memberVMS.length; i++) {
      memberVMS[i].invoke(() -> verifyRegionSize(expectedSize));
    }
  }

  public void waitForGIITeskHookStarted(final MemberVM vm,
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

  private static class PauseDuringGIICallback extends InitialImageOperation.GIITestHook {
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

  private class PauseDuringClearDistributionMessageObserver
      extends DistributionMessageObserver {
    private CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof PartitionedRegionClearMessage) {
        PartitionedRegionClearMessage prcm = (PartitionedRegionClearMessage) message;
        try {
          logger.info("before wait for clear message");
          latch.await();
          logger.info("after wait for clear message");
        } catch (InterruptedException ex) {
        }
      }
    }
  }
}
