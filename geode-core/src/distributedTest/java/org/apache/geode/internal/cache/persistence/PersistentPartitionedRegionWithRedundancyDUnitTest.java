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
package org.apache.geode.internal.cache.persistence;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserver;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

/**
 * Tests the basic use cases for PR persistence.
 */
@RunWith(GeodeParamsRunner.class)
@SuppressWarnings("serial,unused")
public class PersistentPartitionedRegionWithRedundancyDUnitTest implements Serializable {

  private static final int NUM_BUCKETS = 113;

  private String partitionedRegionName;
  private String parentRegion1Name;
  private String parentRegion2Name;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule =
      CacheRule.builder().addConfig(getDistributedSystemProperties()).build();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedDiskDirRule diskDirRule = new DistributedDiskDirRule();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    String uniqueName = getClass().getSimpleName() + "-" + testName.getMethodName();
    partitionedRegionName = uniqueName + "-partitionedRegion";
    parentRegion1Name = "parent1";
    parentRegion2Name = "parent2";
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> {
      InternalResourceManager.setResourceObserver(null);
      DistributionMessageObserver.setInstance(null);
    });
  }

  private Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.setProperty(SERIALIZABLE_OBJECT_FILTER, TestFunction.class.getName());
    return config;
  }

  /**
   * A simple test case that we are actually persisting with a PR.
   */
  @Test
  public void recoversFromDisk() throws Exception {
    createPartitionedRegion(0, -1, 113, true);

    createData(0, 1);

    Set<Integer> vm0Buckets = getBucketList();

    getCache().close();

    createPartitionedRegion(0, -1, 113, true);

    assertThat(getBucketList()).isEqualTo(vm0Buckets);

    checkData(0, 1);

  }


  /**
   * Test to make sure that we can recover from a complete system shutdown
   */
  @Test
  public void testGetDataDelayDueToRecoveryAfterServerShutdown() throws Exception {
    int numEntries = 10000;

    vm0.invoke(() -> createPartitionedRegion(1, -1, 113, true));
    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));
    vm2.invoke(() -> createPartitionedRegion(1, -1, 113, true));
    vm3.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    vm0.invoke(() -> createData(0, numEntries));

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM2 = vm2.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM3 = vm3.invoke(() -> getBucketList());

    vm1.invoke(() -> getCache().close());

    AsyncInvocation<Void> createPartitionedRegionOnVM1 =
        vm1.invokeAsync(() -> createPartitionedRegion(1, -1, 113, true));

    vm0.invoke(() -> {
      long timeElapsed;
      int key;
      Region<?, ?> region = getCache().getRegion(partitionedRegionName);
      for (int i = 0; i < numEntries; i++) {
        key = getRandomNumberInRange(0, numEntries - 1);
        assertThat(region.get(key)).isEqualTo(key);
      }

    });


    createPartitionedRegionOnVM1.await(2, MINUTES);

    assertThat(vm1.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM1);
    assertThat(vm0.invoke(() -> getRegionStats().getGetRetries())).isEqualTo(0);
  }

  private void createPartitionedRegion(final int redundancy, final int recoveryDelay,
      final int numBuckets, final boolean synchronous) throws InterruptedException {
    CountDownLatch recoveryDone = new CountDownLatch(1);

    if (redundancy > 0) {
      ResourceObserver observer = new ResourceObserverAdapter() {
        @Override
        public void recoveryFinished(Region region) {
          recoveryDone.countDown();
        }
      };

      InternalResourceManager.setResourceObserver(observer);
    } else {
      recoveryDone.countDown();
    }

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(redundancy);
    partitionAttributesFactory.setRecoveryDelay(recoveryDelay);
    partitionAttributesFactory.setTotalNumBuckets(numBuckets);
    partitionAttributesFactory.setLocalMaxMemory(500);

    RegionFactory<?, ?> regionFactory =
        getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskSynchronous(synchronous);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(partitionedRegionName);

    recoveryDone.await(2, MINUTES);
  }

  private void createData(final int startKey, final int endKey) {
    createDataFor(startKey, endKey, partitionedRegionName);
  }

  private void createDataFor(final int startKey, final int endKey,
      final String regionName) {
    Region<Integer, Integer> region = getCache().getRegion(regionName);
    for (int i = startKey; i < endKey; i++) {
      region.put(i, i);
    }
  }

  private Set<Integer> getBucketList() {
    return getBucketListFor(partitionedRegionName);
  }

  private Set<Integer> getBucketListFor(final String regionName) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);
    return new TreeSet<>(region.getDataStore().getAllLocalBucketIds());
  }

  private void checkData(final int startKey, final int endKey) {
    checkDataFor(startKey, endKey, partitionedRegionName);
  }

  private void checkDataFor(final int startKey, final int endKey,
      final String regionName) {
    Region<?, ?> region = getCache().getRegion(regionName);
    for (int i = startKey; i < endKey; i++) {
      assertThat(region.get(i)).isEqualTo(i);
    }
  }


  private InternalCache getCache() {
    return cacheRule.getOrCreateCache();
  }

  private PartitionedRegionStats getRegionStats() {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(partitionedRegionName);
    return region.getPrStats();
  }

  int getRandomNumberInRange(int min, int max) {
    Random r = new Random();
    return r.nextInt((max - min) + 1) + min;
  }


  private static class RecoveryObserver extends ResourceObserverAdapter {

    private final String partitionedRegionName;
    private final CountDownLatch recoveryDone = new CountDownLatch(1);

    RecoveryObserver(final String partitionedRegionName) {
      this.partitionedRegionName = partitionedRegionName;
    }

    @Override
    public void rebalancingOrRecoveryFinished(final Region region) {
      if (region.getName().equals(partitionedRegionName)) {
        recoveryDone.countDown();
      }
    }

    void await(final long timeout, final TimeUnit unit) throws InterruptedException {
      recoveryDone.await(timeout, unit);
    }
  }

  private static class TestFunction implements Function, Serializable {

    @Override
    public void execute(final FunctionContext context) {
      context.getResultSender().lastResult(null);
    }

    @Override
    public String getId() {
      return TestFunction.class.getSimpleName();
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean optimizeForWrite() {
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

}
