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
package org.apache.geode.internal.cache.partitioned;


import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.util.internal.GeodeGlossary;

public class PartitionedRegionCacheCloseNoRetryDistributedTest implements Serializable {

  private String partitionedRegionName;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  private static final long TIMEOUT_MILLIS = GeodeAwaitility.getTimeout().toMillis();

  @Rule
  public CacheRule cacheRule =
      CacheRule.builder().addConfig(getDistributedSystemProperties()).build();

  @Rule
  public SerializableTestName testName = new SerializableTestName();


  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    invokeInEveryVM(() -> {
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "PartitionMessage.DISABLE_REATTEMPT_ON_CACHE_CLOSE",
          "true");
    });
    String uniqueName = getClass().getSimpleName() + "-" + testName.getMethodName();
    partitionedRegionName = uniqueName + "-partitionedRegion";
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> {
      System.clearProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "PartitionMessage.DISABLE_REATTEMPT_ON_CACHE_CLOSE");
      InternalResourceManager.setResourceObserver(null);
      DistributionMessageObserver.setInstance(null);
    });
  }

  private Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.setProperty(SERIALIZABLE_OBJECT_FILTER, TestFunction.class.getName());
    return config;
  }


  @Test
  public void testCacheCloseDuringWrite()
      throws InterruptedException {
    int redundantCopies = 1;
    int recoveryDelay = -1;
    int numBuckets = 100;
    boolean diskSynchronous = true;

    vm0.invoke(() -> {
      createPartitionedRegion(redundantCopies, recoveryDelay, numBuckets, diskSynchronous);
      createData(0, numBuckets, "a");
    });

    vm1.invoke(() -> {
      createPartitionedRegion(redundantCopies, recoveryDelay, numBuckets, diskSynchronous);
    });

    // Need to invoke this async because vm1 will wait for vm0 to come back online
    // unless we explicitly revoke it.

    int endData = 10000;

    AsyncInvocation createRegionDataAsync = vm0.invokeAsync(
        () -> {
          Exception exc = null;
          try {
            createData(numBuckets, endData, "b");
          } catch (Exception e) {
            exc = e;
          }

          assertThat(exc).isNotNull();
          assertThat(exc).isInstanceOf(InternalGemFireException.class);

        });

    AsyncInvocation closeCacheAsync = vm1.invokeAsync(
        () -> {
          getCache().close();
        });

    closeCacheAsync.get();
    createRegionDataAsync.get();

  }


  @Test
  public void testCacheCloseDuringInvalidate()
      throws InterruptedException {
    int redundantCopies = 1;
    int recoveryDelay = -1;
    int numBuckets = 100;
    boolean diskSynchronous = true;
    int endData = 10000;

    vm0.invoke(() -> {
      createPartitionedRegion(redundantCopies, recoveryDelay, numBuckets, diskSynchronous);
      createData(0, endData, "a");
    });

    vm1.invoke(() -> {
      createPartitionedRegion(redundantCopies, recoveryDelay, numBuckets, diskSynchronous);
    });

    // Need to invoke this async because vm1 will wait for vm0 to come back online
    // unless we explicitly revoke it.

    AsyncInvocation invalidateRegionDataAsync = vm0.invokeAsync(
        () -> {
          Exception exc = null;
          try {
            invalidateData(0, endData);
          } catch (Exception e) {
            exc = e;
          }

          assertThat(exc).isNotNull();
          assertThat(exc).isInstanceOf(InternalGemFireException.class);

        });

    AsyncInvocation closeCacheAsync = vm1.invokeAsync(
        () -> {
          getCache().close();
        });

    closeCacheAsync.get();
    invalidateRegionDataAsync.get();

  }


  private void createPartitionedRegion(final int redundancy, final int recoveryDelay,
      final int numBuckets, final boolean synchronous) throws InterruptedException {
    CountDownLatch recoveryDone = new CountDownLatch(1);

    if (redundancy > 0) {
      InternalResourceManager.ResourceObserver observer =
          new InternalResourceManager.ResourceObserverAdapter() {
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

    recoveryDone.await(TIMEOUT_MILLIS, MILLISECONDS);
  }


  private InternalCache getCache() {
    return cacheRule.getOrCreateCache();
  }

  private void createData(final int startKey, final int endKey, final String value) {
    createDataFor(startKey, endKey, value, partitionedRegionName);
  }

  private void createDataFor(final int startKey, final int endKey, final String value,
      final String regionName) {
    Region<Integer, String> region = getCache().getRegion(regionName);
    for (int i = startKey; i < endKey; i++) {
      region.put(i, value);
    }
  }

  private void invalidateData(final int startKey, final int endKey) {
    invalidateDataFor(startKey, endKey, partitionedRegionName);
  }

  private void invalidateDataFor(final int startKey, final int endKey,
      final String regionName) {
    Region<?, ?> region = getCache().getRegion(regionName);
    for (int i = startKey; i < endKey; i++) {
      region.invalidate(i);
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
