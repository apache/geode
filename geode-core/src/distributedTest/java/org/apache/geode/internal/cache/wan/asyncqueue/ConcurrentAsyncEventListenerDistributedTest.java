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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
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

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.asyncqueue.internal.InternalAsyncEventQueue;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.internal.size.Sizeable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.AEQTest;

/**
 * Extracted from {@link AsyncEventListenerDistributedTest}.
 */
@Category(AEQTest.class)
@SuppressWarnings("serial")
public class ConcurrentAsyncEventListenerDistributedTest implements Serializable {

  private static final String SUBSTITUTION_PREFIX = "substituted_";

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
   * Regression test for TRAC #50366: NullPointerException with AsyncEventQueue#size() when number
   * of dispatchers is more than 1
   */
  @Test // serial, ReplicateRegion, RegressionTest, MOVE
  public void testConcurrentSerialAsyncEventQueueSize() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());

    vm0.invoke(() -> createConcurrentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 150, 2, 100, OrderPolicy.KEY, false));
    vm1.invoke(() -> createConcurrentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 150, 2, 100, OrderPolicy.KEY, false));

    vm0.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm1.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));

    vm0.invoke(() -> getInternalGatewaySender().pause());
    vm1.invoke(() -> getInternalGatewaySender().pause());

    vm0.invoke(() -> waitForDispatcherToPause());
    vm1.invoke(() -> waitForDispatcherToPause());

    vm0.invoke(() -> doPuts(replicateRegionName, 1000));

    assertThat(vm0.invoke(() -> getAsyncEventQueue().size())).isEqualTo(1000);
    assertThat(vm1.invoke(() -> getAsyncEventQueue().size())).isEqualTo(1000);
  }

  @Test // serial, OrderPolicy.KEY, ReplicateRegion
  public void testConcurrentSerialAsyncEventQueueWithReplicateRegionAndOrderPolicyKey() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createConcurrentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 100, 3, 100, OrderPolicy.KEY, false));
    vm1.invoke(() -> createConcurrentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 100, 3, 100, OrderPolicy.KEY, false));
    vm2.invoke(() -> createConcurrentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 100, 3, 100, OrderPolicy.KEY, false));

    vm0.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm1.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm2.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));

    vm0.invoke(() -> doPuts(replicateRegionName, 1000));

    // primary sender
    vm0.invoke(() -> validateAsyncEventListener(1000));

    // secondaries
    vm1.invoke(() -> validateAsyncEventListener(0));
    vm2.invoke(() -> validateAsyncEventListener(0));
  }

  @Test // serial, OrderPolicy.THREAD, ReplicateRegion
  public void testConcurrentSerialAsyncEventQueueWithReplicateRegionAndOrderPolicyThread() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createConcurrentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 100, 3, 100, OrderPolicy.THREAD, false));
    vm1.invoke(() -> createConcurrentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 100, 3, 100, OrderPolicy.THREAD, false));
    vm2.invoke(() -> createConcurrentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 100, 3, 100, OrderPolicy.THREAD, false));

    vm0.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm1.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm2.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));

    vm0.invokeAsync(() -> doPuts(replicateRegionName, 500));
    vm0.invokeAsync(() -> doNextPuts(replicateRegionName, 500, 1000));

    // primary sender
    vm0.invoke(() -> validateAsyncEventListener(1000));

    // secondaries
    vm1.invoke(() -> validateAsyncEventListener(0));
    vm2.invoke(() -> validateAsyncEventListener(0));
  }

  /**
   * Dispatcher threads set to more than 1 but no order policy set.
   *
   * <p>
   * Regression test for TRAC #50514: NullPointerException in AsyncEventListener if adding
   * AsyncEventQueue with using API and setting number of dispatchers more than 1
   */
  @Test // serial, ReplicateRegion, RegressionTest
  public void testConcurrentSerialAsyncEventQueueWithoutOrderPolicy() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createConcurrentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 100, 3, 100, null, false));
    vm1.invoke(() -> createConcurrentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 100, 3, 100, null, false));
    vm2.invoke(() -> createConcurrentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 100, 3, 100, null, false));

    vm0.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm1.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm2.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));

    vm0.invoke(() -> doPuts(replicateRegionName, 1000));

    // primary sender
    vm0.invoke(() -> validateAsyncEventListener(1000));

    // secondaries
    vm1.invoke(() -> validateAsyncEventListener(0));
    vm2.invoke(() -> validateAsyncEventListener(0));
  }

  /**
   * Regression test for TRAC #50366: NullPointerException with AsyncEventQueue#size() when number
   * of dispatchers is more than 1
   */
  @Test // parallel, OrderPolicy.KEY, PartitionedRegion, RegressionTest
  public void testConcurrentParallelAsyncEventQueueSize() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());

    vm0.invoke(() -> createConcurrentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        false, 100, 2, 100, OrderPolicy.KEY, true));
    vm1.invoke(() -> createConcurrentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        false, 100, 2, 100, OrderPolicy.KEY, true));

    vm0.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, -1, 0, 16));
    vm1.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, -1, 0, 16));

    vm0.invoke(() -> getInternalGatewaySender().pause());
    vm1.invoke(() -> getInternalGatewaySender().pause());

    vm0.invoke(() -> waitForDispatcherToPause());
    vm1.invoke(() -> waitForDispatcherToPause());

    vm0.invoke(() -> doPuts(partitionedRegionName, 1000));

    vm0.invoke(() -> await()
        .untilAsserted(() -> assertThat(getAsyncEventQueue().size()).isEqualTo(1000)));
    vm1.invoke(() -> await()
        .untilAsserted(() -> assertThat(getAsyncEventQueue().size()).isEqualTo(1000)));
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
    assertThat(regionName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRecoveryDelay(recoveryDelay);
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION);
    if (asyncEventQueueId != null) {
      regionFactory.addAsyncEventQueueId(asyncEventQueueId);
    }
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    configureRegion(regionFactory).create(regionName);
  }

  private void createReplicateRegion(String regionName, String asyncEventQueueId) {
    assertThat(regionName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(REPLICATE);
    regionFactory.addAsyncEventQueueId(asyncEventQueueId);

    configureRegion(regionFactory).create(regionName);
  }

  private void createConcurrentAsyncEventQueue(String asyncEventQueueId,
      AsyncEventListener asyncEventListener,
      boolean isBatchConflationEnabled,
      int batchSize,
      int dispatcherThreads,
      int maximumQueueMemory,
      OrderPolicy orderPolicy,
      boolean isParallel) {
    assertThat(asyncEventQueueId).isNotEmpty();
    assertThat(asyncEventListener).isNotNull();

    AsyncEventQueueFactory asyncEventQueueFactory = getCache().createAsyncEventQueueFactory();
    asyncEventQueueFactory.setBatchConflationEnabled(isBatchConflationEnabled);
    asyncEventQueueFactory.setBatchSize(batchSize);
    asyncEventQueueFactory.setDispatcherThreads(dispatcherThreads);
    asyncEventQueueFactory.setOrderPolicy(orderPolicy);
    asyncEventQueueFactory.setMaximumQueueMemory(maximumQueueMemory);
    asyncEventQueueFactory.setParallel(isParallel);

    asyncEventQueueFactory.create(asyncEventQueueId, asyncEventListener);
  }

  private void doPuts(String regionName, int numPuts) {
    Region<Integer, Integer> region = getCache().getRegion(regionName);
    for (int i = 0; i < numPuts; i++) {
      region.put(i, i);
    }
  }

  private void doNextPuts(String regionName, int start, int numPuts) {
    Region<Integer, Integer> region = getCache().getRegion(regionName);
    for (int i = start; i < numPuts; i++) {
      region.put(i, i);
    }
  }

  private void validateAsyncEventListener(int expectedSize) {
    Map<?, ?> eventsMap = getSpyAsyncEventListener().getEventsMap();
    await().until(() -> eventsMap.size() == expectedSize);
  }

  private void waitForDispatcherToPause() {
    getInternalGatewaySender().getEventProcessor().waitForDispatcherToPause();
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
