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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getCurrentVMNum;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.asyncqueue.internal.InternalAsyncEventQueue;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.internal.size.Sizeable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * Extracted from {@link AsyncEventListenerDistributedTest}.
 */
@Category(AEQTest.class)
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@SuppressWarnings("serial")
public class AsyncEventListenerWithFilterDistributedTest implements Serializable {

  private static final String SUBSTITUTION_PREFIX = "substituted_";

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

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  private String partitionedRegionName;
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

  @Test // parallel, PartitionedRegion, GatewayEventSubstitutionFilter
  public void testParallelAsyncEventQueueWithSubstitutionFilter() {
    vm0.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueueWithGatewayEventSubstitutionFilter(asyncEventQueueId,
        new SpyAsyncEventListener(), new StringGatewayEventSubstitutionFilter(),
        100, dispatcherThreadCount, 100, true));

    vm0.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, -1, 0, 16));

    int numPuts = 10;
    vm0.invoke(() -> doPuts(partitionedRegionName, numPuts));

    vm0.invoke(() -> waitForAsyncQueueToEmpty());

    vm0.invoke(() -> validateSubstitutionFilterInvocations(numPuts));
  }

  @Test // parallel, PartitionedRegion, GatewayEventSubstitutionFilter
  public void testParallelAsyncEventQueueWithSubstitutionFilterNoSubstituteValueToDataInvocations() {
    vm0.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueueWithGatewayEventSubstitutionFilter(asyncEventQueueId,
        new SpyAsyncEventListener(), new SizeableGatewayEventSubstitutionFilter(),
        100, dispatcherThreadCount, 100, true));

    vm0.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, -1, 0, 16));

    int numPuts = 10;
    vm0.invoke(() -> doPuts(partitionedRegionName, numPuts));

    vm0.invoke(() -> waitForAsyncQueueToEmpty());

    vm0.invoke(() -> validateSubstitutionFilterToDataInvocations(0));
  }

  @Test // parallel, PartitionedRegion, persistent, GatewayEventFilter
  public void testCacheClosedBeforeAEQWrite() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueueWithGatewayEventFilter(asyncEventQueueId,
        new SpyAsyncEventListener(), new CloseCacheGatewayFilter(getCache(), 1000),
        createDiskStoreName(asyncEventQueueId), true, dispatcherThreadCount, true));
    vm1.invoke(() -> createAsyncEventQueueWithGatewayEventFilter(asyncEventQueueId,
        new SpyAsyncEventListener(), new CloseCacheGatewayFilter(getCache(), 1000),
        createDiskStoreName(asyncEventQueueId), true, dispatcherThreadCount, true));

    vm0.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, -1, 0, 16));
    vm1.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, -1, 0, 16));
    vm2.invoke(() -> createPartitionedRegionAccessor(partitionedRegionName, asyncEventQueueId, 16));

    vm2.invoke(() -> {
      Region<Integer, Integer> region = getCache().getRegion(partitionedRegionName);
      region.put(1, 1);
      region.put(2, 2);

      // This will trigger the gateway event filter to close the cache
      assertThatThrownBy(() -> region.removeAll(Collections.singleton(1)))
          .isInstanceOf(PartitionOfflineException.class);
    });
  }

  private InternalCache getCache() {
    return cacheRule.getOrCreateCache(getDistributedSystemProperties());
  }

  private void createCache() {
    cacheRule.createCache(getDistributedSystemProperties());
  }

  private void createPartitionedRegion(String regionName,
      String asyncEventQueueId,
      long recoveryDelay,
      int redundantCopies,
      int totalNumBuckets) {
    assertThat(regionName).isNotNull();
    assertThat(asyncEventQueueId).isNotNull();

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRecoveryDelay(recoveryDelay);
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.addAsyncEventQueueId(asyncEventQueueId);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    configureRegion(regionFactory).create(regionName);
  }

  private void createPartitionedRegionAccessor(String regionName, String asyncEventQueueId,
      int totalNumBuckets) {
    assertThat(regionName).isNotNull();
    assertThat(asyncEventQueueId).isNotNull();

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setLocalMaxMemory(0);
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.addAsyncEventQueueId(asyncEventQueueId);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    configureRegion(regionFactory).create(regionName);
  }

  private void createDiskStore(String diskStoreName, String asyncEventQueueId) {
    assertThat(diskStoreName).isNotNull();
    assertThat(asyncEventQueueId).isNotNull();

    File directory = createDirectory(createDiskStoreName(asyncEventQueueId));

    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] {directory});

    diskStoreFactory.create(diskStoreName);
  }

  private File createDirectory(String name) {
    assertThat(name).isNotNull();

    File directory = new File(temporaryFolder.getRoot(), name);
    if (!directory.exists()) {
      try {
        return temporaryFolder.newFolder(name);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return directory;
  }

  private String createDiskStoreName(String asyncEventQueueId) {
    assertThat(asyncEventQueueId).isNotNull();

    return asyncEventQueueId + "_disk_" + getCurrentVMNum();
  }

  private void createAsyncEventQueueWithGatewayEventFilter(String asyncEventQueueId,
      AsyncEventListener asyncEventListener,
      GatewayEventFilter gatewayEventFilter,
      String diskStoreName,
      boolean isDiskSynchronous,
      int dispatcherThreads,
      boolean isParallel) {
    assertThat(asyncEventQueueId).isNotNull();
    assertThat(asyncEventListener).isNotNull();
    assertThat(gatewayEventFilter).isNotNull();
    assertThat(diskStoreName).isNotNull();

    createDiskStore(diskStoreName, asyncEventQueueId);

    AsyncEventQueueFactory asyncEventQueueFactory = getCache().createAsyncEventQueueFactory();
    asyncEventQueueFactory.setDiskStoreName(diskStoreName);
    asyncEventQueueFactory.setDiskSynchronous(isDiskSynchronous);
    asyncEventQueueFactory.setDispatcherThreads(dispatcherThreads);
    asyncEventQueueFactory.addGatewayEventFilter(gatewayEventFilter);
    asyncEventQueueFactory.setParallel(isParallel);
    asyncEventQueueFactory.setPersistent(true);

    asyncEventQueueFactory.create(asyncEventQueueId, asyncEventListener);
  }

  private void createAsyncEventQueueWithGatewayEventSubstitutionFilter(String asyncEventQueueId,
      AsyncEventListener asyncEventListener,
      GatewayEventSubstitutionFilter gatewayEventSubstitutionFilter,
      int batchSize,
      int dispatcherThreads,
      int maximumQueueMemory,
      boolean isParallel) {
    assertThat(asyncEventQueueId).isNotNull();
    assertThat(asyncEventListener).isNotNull();

    AsyncEventQueueFactory asyncEventQueueFactory = getCache().createAsyncEventQueueFactory();
    asyncEventQueueFactory.setBatchConflationEnabled(false);
    asyncEventQueueFactory.setBatchSize(batchSize);
    asyncEventQueueFactory.setDispatcherThreads(dispatcherThreads);
    asyncEventQueueFactory.setGatewayEventSubstitutionListener(gatewayEventSubstitutionFilter);
    asyncEventQueueFactory.setMaximumQueueMemory(maximumQueueMemory);
    asyncEventQueueFactory.setParallel(isParallel);
    asyncEventQueueFactory.setPersistent(false);

    asyncEventQueueFactory.create(asyncEventQueueId, asyncEventListener);
  }

  private void doPuts(String regionName, int numPuts) {
    Region<Integer, Integer> region = getCache().getRegion(regionName);
    for (int i = 0; i < numPuts; i++) {
      region.put(i, i);
    }
  }

  private void assertRegionQueuesAreEmpty(InternalGatewaySender gatewaySender) {
    Set<RegionQueue> regionQueues = gatewaySender.getQueues();
    for (RegionQueue queue : regionQueues) {
      assertThat(queue.size()).isZero();
    }
  }

  private void validateSubstitutionFilterInvocations(int expectedNumInvocations) {
    // Verify the GatewayEventSubstitutionFilter has been invoked the appropriate number of times
    StringGatewayEventSubstitutionFilter gatewayEventSubstitutionFilter =
        getMyGatewayEventSubstitutionFilter();
    assertThat(gatewayEventSubstitutionFilter.getNumInvocations())
        .isEqualTo(expectedNumInvocations);

    // Verify the AsyncEventListener has received the substituted values
    SpyAsyncEventListener asyncEventListener = getSpyAsyncEventListener();
    Map<Integer, String> eventsMap = asyncEventListener.getEventsMap();
    assertThat(eventsMap).isNotNull().hasSize(expectedNumInvocations);

    for (Map.Entry<Integer, String> entry : eventsMap.entrySet()) {
      assertThat(entry.getValue()).isEqualTo(SUBSTITUTION_PREFIX + entry.getKey());
    }
  }

  private void validateSubstitutionFilterToDataInvocations(int expectedToDataInvocations) {
    // Verify the GatewayEventSubstitutionFilter has been invoked the appropriate number of times
    SizeableGatewayEventSubstitutionFilter filter = getSizeableGatewayEventSubstitutionFilter();
    assertThat(filter.getNumToDataInvocations()).isEqualTo(expectedToDataInvocations);
  }

  private void waitForAsyncQueueToEmpty() {
    InternalGatewaySender gatewaySender = getInternalGatewaySender();
    await().untilAsserted(() -> assertRegionQueuesAreEmpty(gatewaySender));
  }

  private StringGatewayEventSubstitutionFilter getMyGatewayEventSubstitutionFilter() {
    return (StringGatewayEventSubstitutionFilter) getGatewayEventSubstitutionFilter();
  }

  private SizeableGatewayEventSubstitutionFilter getSizeableGatewayEventSubstitutionFilter() {
    return (SizeableGatewayEventSubstitutionFilter) getGatewayEventSubstitutionFilter();
  }

  private GatewayEventSubstitutionFilter getGatewayEventSubstitutionFilter() {
    GatewayEventSubstitutionFilter gatewayEventSubstitutionFilter =
        getAsyncEventQueue().getGatewayEventSubstitutionFilter();
    assertThat(gatewayEventSubstitutionFilter).isNotNull();
    return gatewayEventSubstitutionFilter;
  }

  private InternalGatewaySender getInternalGatewaySender() {
    InternalGatewaySender gatewaySender = getInternalAsyncEventQueue().getSender();
    assertThat(gatewaySender).isNotNull();
    return gatewaySender;
  }

  private SpyAsyncEventListener getSpyAsyncEventListener() {
    return (SpyAsyncEventListener) getAsyncEventListener();
  }

  private AsyncEventListener getAsyncEventListener() {
    AsyncEventListener asyncEventListener = getAsyncEventQueue().getAsyncEventListener();
    assertThat(asyncEventListener).isNotNull();
    return asyncEventListener;
  }

  private InternalAsyncEventQueue getInternalAsyncEventQueue() {
    return (InternalAsyncEventQueue) getAsyncEventQueue();
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

  private static class CloseCacheGatewayFilter implements GatewayEventFilter {

    private final Cache cache;
    private final long sleepMillis;

    CloseCacheGatewayFilter(final Cache cache, final long sleepMillis) {
      this.cache = cache;
      this.sleepMillis = sleepMillis;
    }

    @Override
    public boolean beforeEnqueue(final GatewayQueueEvent event) {
      if (event.getOperation().isRemoveAll()) {
        new Thread(() -> cache.close()).start();
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
          throw new Error(e);
        }
        throw new CacheClosedException("Thrown by CloseCacheGatewayFilter");
      }
      return true;
    }

    @Override
    public boolean beforeTransmit(final GatewayQueueEvent event) {
      return false;
    }

    @Override
    public void afterAcknowledgement(final GatewayQueueEvent event) {
      // nothing
    }
  }

  private static class SizeableGatewayEventSubstitutionFilter<K, V>
      implements GatewayEventSubstitutionFilter<K, V>, Declarable {

    private final AtomicInteger numToDataInvocations = new AtomicInteger();

    void incNumToDataInvocations() {
      numToDataInvocations.incrementAndGet();
    }

    int getNumToDataInvocations() {
      return numToDataInvocations.get();
    }

    @Override
    public Object getSubstituteValue(final EntryEvent<K, V> event) {
      return new GatewayEventSubstituteObject(this, SUBSTITUTION_PREFIX + event.getKey());
    }
  }

  private static class GatewayEventSubstituteObject implements DataSerializable, Sizeable {

    private final SizeableGatewayEventSubstitutionFilter filter;

    private String id;

    GatewayEventSubstituteObject(final SizeableGatewayEventSubstitutionFilter filter,
        final String id) {
      this.filter = filter;
      this.id = id;
    }

    String getId() {
      assertThat(id).isNotEmpty();
      return id;
    }

    @Override
    public void toData(final DataOutput out) throws IOException {
      filter.incNumToDataInvocations();
      DataSerializer.writeString(id, out);
    }

    @Override
    public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
      id = DataSerializer.readString(in);
    }

    @Override
    public int getSizeInBytes() {
      return 0;
    }

    @Override
    public String toString() {
      return new StringBuilder().append(getClass().getSimpleName()).append("[").append("id=")
          .append(id).append("]").toString();
    }
  }

  private static class StringGatewayEventSubstitutionFilter
      implements GatewayEventSubstitutionFilter<Integer, String>, Declarable {

    private final AtomicInteger numInvocations = new AtomicInteger();

    int getNumInvocations() {
      return numInvocations.get();
    }

    @Override
    public Object getSubstituteValue(final EntryEvent<Integer, String> event) {
      numInvocations.incrementAndGet();
      return SUBSTITUTION_PREFIX + event.getKey();
    }
  }

  private static class SpyAsyncEventListener<K, V> implements AsyncEventListener, Declarable {

    private final Map<K, V> eventsMap = new ConcurrentHashMap<>();

    Map<K, V> getEventsMap() {
      assertThat(eventsMap).isNotNull();
      return eventsMap;
    }

    @Override
    public boolean processEvents(List<AsyncEvent> events) {
      for (AsyncEvent<K, V> event : events) {
        eventsMap.put(event.getKey(), event.getDeserializedValue());
      }
      return true;
    }
  }
}
