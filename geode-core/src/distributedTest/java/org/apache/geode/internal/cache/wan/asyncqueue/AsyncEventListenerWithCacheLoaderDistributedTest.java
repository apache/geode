/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.wan.asyncqueue;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * Extracted from {@link AsyncEventListenerDistributedTest}.
 */
@Category(AEQTest.class)
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@SuppressWarnings("serial")
public class AsyncEventListenerWithCacheLoaderDistributedTest implements Serializable {

  @Parameter
  public int dispatcherThreadCount;

  @Parameters(name = "dispatcherThreadCount={0}")
  public static Iterable<Integer> dispatcherThreadCounts() {
    return Arrays.asList(1, 3);
  }

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  private String partitionedRegionName;
  private String replicateRegionName;
  private String asyncEventQueueId;

  private VM vm0;
  private VM vm1;
  private VM vm2;

  @Before
  public void setUp() throws Exception {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);

    String className = getClass().getSimpleName();
    partitionedRegionName = className + "_PR";
    replicateRegionName = className + "_RR";

    asyncEventQueueId = className;
  }

  /**
   * Override as needed to add to the configuration, such as off-heap-memory-size.
   */
  protected Properties getDistributedSystemProperties() {
    return new Properties();
  }

  /**
   * Override as needed to add to the configuration, such as regionFactory.setOffHeap(boolean).
   */
  protected RegionFactory<?, ?> configureRegion(RegionFactory<?, ?> regionFactory) {
    return regionFactory;
  }

  /**
   * Verify that the events loaded by CacheLoader reach the AsyncEventListener with correct
   * operation detail.
   *
   * <p>
   * Regression test for TRAC #50237: AsyncEventListeners does not report correct operation detail
   */
  @Test // serial, ReplicateRegion, CacheLoader, RegressionTest
  public void testReplicatedSerialAsyncEventQueueWithCacheLoader() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), 100,
        dispatcherThreadCount, 100, false));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), 100,
        dispatcherThreadCount, 100, false));
    vm2.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), 100,
        dispatcherThreadCount, 100, false));

    vm0.invoke(() -> createReplicateRegionWithCacheLoader(replicateRegionName, asyncEventQueueId,
        new StringCacheLoader()));
    vm1.invoke(() -> createReplicateRegionWithCacheLoader(replicateRegionName, asyncEventQueueId,
        new StringCacheLoader()));
    vm2.invoke(() -> createReplicateRegionWithCacheLoader(replicateRegionName, asyncEventQueueId,
        new StringCacheLoader()));

    vm0.invoke(() -> doGets(replicateRegionName, 10));

    // primary sender
    vm0.invoke(() -> validateAsyncEventForOperationDetail(10, OperationType.LOAD));

    // secondaries
    vm1.invoke(() -> validateAsyncEventForOperationDetail(0, OperationType.LOAD));
    vm2.invoke(() -> validateAsyncEventForOperationDetail(0, OperationType.LOAD));
  }

  /**
   * Verify that the events reaching the AsyncEventListener have correct operation detail.
   *
   * <p>
   * Regression test for TRAC #50237: AsyncEventListeners does not report correct operation detail
   */
  @Test // parallel, PartitionedRegion, CacheLoader, RegressionTest
  public void testParallelAsyncEventQueueWithCacheLoader() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), 100,
        dispatcherThreadCount, 100, true));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), 100,
        dispatcherThreadCount, 100, true));

    vm0.invoke(() -> createPartitionedRegionWithCacheLoader(partitionedRegionName,
        asyncEventQueueId, new StringCacheLoader(), 0, 16));
    vm1.invoke(() -> createPartitionedRegionWithCacheLoader(partitionedRegionName,
        asyncEventQueueId, new StringCacheLoader(), 0, 16));

    vm0.invoke(() -> doPutAll(partitionedRegionName, 100, 10));

    vm0.invoke(() -> validateAsyncEventForOperationDetail(500, OperationType.PUT_ALL));
    vm1.invoke(() -> validateAsyncEventForOperationDetail(500, OperationType.PUT_ALL));
  }

  private InternalCache getCache() {
    return cacheRule.getOrCreateCache(getDistributedSystemProperties());
  }

  private void createCache() {
    cacheRule.createCache(getDistributedSystemProperties());
  }

  private void createPartitionedRegionWithCacheLoader(String regionName,
      String asyncEventQueueId,
      CacheLoader cacheLoader,
      int redundantCopies,
      int totalNumBuckets) {
    assertThat(regionName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();
    assertThat(cacheLoader).isNotNull();

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.addAsyncEventQueueId(asyncEventQueueId);
    regionFactory.setCacheLoader(cacheLoader);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    configureRegion(regionFactory).create(regionName);
  }

  private void createReplicateRegionWithCacheLoader(String regionName,
      String asyncEventQueueId,
      CacheLoader cacheLoader) {
    assertThat(regionName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();
    assertThat(cacheLoader).isNotNull();

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(REPLICATE);
    regionFactory.addAsyncEventQueueId(asyncEventQueueId);
    regionFactory.setCacheLoader(cacheLoader);

    configureRegion(regionFactory).create(regionName);
  }

  private void createAsyncEventQueue(String asyncEventQueueId,
      AsyncEventListener asyncEventListener,
      int batchSize,
      int dispatcherThreads,
      int maximumQueueMemory,
      boolean isParallel) {
    assertThat(asyncEventQueueId).isNotEmpty();
    assertThat(asyncEventListener).isNotNull();

    AsyncEventQueueFactory asyncEventQueueFactory = getCache().createAsyncEventQueueFactory();
    asyncEventQueueFactory.setBatchConflationEnabled(false);
    asyncEventQueueFactory.setBatchSize(batchSize);
    asyncEventQueueFactory.setDispatcherThreads(dispatcherThreads);
    asyncEventQueueFactory.setMaximumQueueMemory(maximumQueueMemory);
    asyncEventQueueFactory.setParallel(isParallel);
    asyncEventQueueFactory.setPersistent(false);

    asyncEventQueueFactory.create(asyncEventQueueId, asyncEventListener);
  }

  private void doGets(String regionName, int numGets) {
    Region<Integer, Integer> region = getCache().getRegion(regionName);
    for (int i = 0; i < numGets; i++) {
      region.get(i);
    }
  }

  private void doPutAll(String regionName, int numPuts, int size) {
    Region<Integer, Integer> region = getCache().getRegion(regionName);
    for (int i = 0; i < numPuts; i++) {
      Map<Integer, Integer> putAllMap = new HashMap<>();
      for (int j = 0; j < size; j++) {
        putAllMap.put(size * i + j, i);
      }
      region.putAll(putAllMap, "putAllCallback");
      putAllMap.clear();
    }
  }

  private void validateAsyncEventForOperationDetail(int expectedSize, OperationType operationType) {
    Map<?, AsyncEvent> eventsMap = (Map<?, AsyncEvent>) getSpyAsyncEventListener().getEventsMap();

    await()
        .untilAsserted(() -> assertThat(eventsMap.size()).isEqualTo(expectedSize));

    for (AsyncEvent<?, ?> asyncEvent : eventsMap.values()) {
      switch (operationType) {
        case LOAD:
          assertThat(asyncEvent.getOperation().isLoad()).isTrue();
          break;
        case PUT_ALL:
          assertThat(asyncEvent.getOperation().isPutAll()).isTrue();
          break;
        default:
          fail("Invalid OperationType: " + operationType);
      }
    }
  }

  private SpyAsyncEventListener getSpyAsyncEventListener() {
    return (SpyAsyncEventListener) getAsyncEventListener();
  }

  private AsyncEventListener getAsyncEventListener() {
    AsyncEventListener asyncEventListener = getAsyncEventQueue().getAsyncEventListener();
    assertThat(asyncEventListener).isNotNull();
    return asyncEventListener;
  }

  private AsyncEventQueue getAsyncEventQueue() {
    AsyncEventQueue value = null;

    Set<AsyncEventQueue> asyncEventQueues = getCache().getAsyncEventQueues();
    for (AsyncEventQueue asyncEventQueue : asyncEventQueues) {
      if (asyncEventQueueId.equals(asyncEventQueue.getId())) {
        value = asyncEventQueue;
      }
    }

    assertThat(value).isNotNull();
    return value;
  }

  private enum OperationType {
    LOAD, PUT_ALL
  }

  private static class SpyAsyncEventListener<K, V> implements AsyncEventListener, Declarable {

    private final Map<K, AsyncEvent> eventsMap = new ConcurrentHashMap<>();

    Map<K, AsyncEvent> getEventsMap() {
      assertThat(eventsMap).isNotNull();
      return eventsMap;
    }

    @Override
    public boolean processEvents(List<AsyncEvent> events) {
      for (AsyncEvent<K, V> event : events) {
        eventsMap.put(event.getKey(), event);
      }
      return true;
    }
  }

  private static class StringCacheLoader implements CacheLoader<Integer, String>, Declarable {

    @Override
    public String load(final LoaderHelper<Integer, String> helper) {
      return "LoadedValue" + "_" + helper.getKey();
    }
  }
}
