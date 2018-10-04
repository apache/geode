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

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getCurrentVMNum;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.asyncqueue.internal.InternalAsyncEventQueue;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

/**
 * Extracted from {@link AsyncEventListenerDistributedTest}.
 */
@Category(AEQTest.class)
@SuppressWarnings("serial")
public class SerialEventListenerDistributedTest implements Serializable {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public DistributedDiskDirRule diskDirRule = new DistributedDiskDirRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

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

  @Test // serial, persistence, no region, IntegrationTest
  public void testSerialAsyncEventQueueAttributes() {
    vm0.invoke(() -> {
      createCache();
      createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true, 150,
          "testDS", true, 100);

      validateAsyncEventQueueAttributes(100, 150, DEFAULT_BATCH_TIME_INTERVAL, true, "testDS", true,
          true);
    });
  }

  /**
   * Error is thrown from AsyncEventListener implementation while processing the batch.
   *
   * <p>
   * Regression test for TRAC #45152: AsyncEventListener misses an event callback
   */
  @Test // serial, ReplicateRegion, RegressionTest
  public void testReplicatedSerialAsyncEventQueue_ExceptionScenario() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new ThrowingAsyncEventListener(),
        false, 100, 100));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new ThrowingAsyncEventListener(),
        false, 100, 100));
    vm2.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new ThrowingAsyncEventListener(),
        false, 100, 100));

    vm0.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm1.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm2.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));

    vm0.invoke(() -> getInternalGatewaySender().pause());
    vm1.invoke(() -> getInternalGatewaySender().pause());
    vm2.invoke(() -> getInternalGatewaySender().pause());

    vm0.invoke(() -> waitForDispatcherToPause());
    vm1.invoke(() -> waitForDispatcherToPause());
    vm2.invoke(() -> waitForDispatcherToPause());

    vm0.invoke(() -> doPuts(replicateRegionName, 100));

    vm0.invoke(() -> getInternalGatewaySender().resume());
    vm1.invoke(() -> getInternalGatewaySender().resume());
    vm2.invoke(() -> getInternalGatewaySender().resume());

    // primary sender
    vm0.invoke(() -> {
      validateThrowingAsyncEventListenerEventsMap(100);
      validateThrowingAsyncEventListenerExceptionThrown();
    });

    // secondaries
    vm1.invoke(() -> validateThrowingAsyncEventListenerEventsMap(0));
    vm2.invoke(() -> validateThrowingAsyncEventListenerEventsMap(0));
  }

  private InternalCache getCache() {
    return cacheRule.getOrCreateCache(getDistributedSystemProperties());
  }

  private void createCache() {
    cacheRule.createCache(getDistributedSystemProperties());
  }

  private void createReplicateRegion(String regionName, String asyncEventQueueId) {
    assertThat(regionName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(REPLICATE);
    regionFactory.addAsyncEventQueueId(asyncEventQueueId);

    configureRegion(regionFactory).create(regionName);
  }

  private void createDiskStore(String diskStoreName, String asyncEventQueueId) {
    assertThat(diskStoreName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();

    File directory = createDirectory(createDiskStoreName(asyncEventQueueId));

    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] {directory});

    diskStoreFactory.create(diskStoreName);
  }

  private File createDirectory(String name) {
    assertThat(name).isNotEmpty();

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
    assertThat(asyncEventQueueId).isNotEmpty();

    return asyncEventQueueId + "_disk_" + getCurrentVMNum();
  }

  private void createAsyncEventQueue(String asyncEventQueueId,
      AsyncEventListener asyncEventListener,
      boolean isBatchConflationEnabled,
      int batchSize,
      int maximumQueueMemory) {
    assertThat(asyncEventQueueId).isNotEmpty();
    assertThat(asyncEventListener).isNotNull();

    AsyncEventQueueFactory asyncEventQueueFactory = getCache().createAsyncEventQueueFactory();
    asyncEventQueueFactory.setBatchConflationEnabled(isBatchConflationEnabled);
    asyncEventQueueFactory.setBatchSize(batchSize);
    asyncEventQueueFactory.setDispatcherThreads(1);
    asyncEventQueueFactory.setMaximumQueueMemory(maximumQueueMemory);
    asyncEventQueueFactory.setParallel(false);
    asyncEventQueueFactory.setPersistent(false);

    asyncEventQueueFactory.create(asyncEventQueueId, asyncEventListener);
  }


  private void createPersistentAsyncEventQueue(String asyncEventQueueId,
      AsyncEventListener asyncEventListener,
      boolean isBatchConflationEnabled,
      int batchSize,
      String diskStoreName,
      boolean isDiskSynchronous,
      int maximumQueueMemory) {
    assertThat(asyncEventQueueId).isNotEmpty();
    assertThat(asyncEventListener).isNotNull();
    assertThat(diskStoreName).isNotEmpty();

    createDiskStore(diskStoreName, asyncEventQueueId);

    AsyncEventQueueFactory asyncEventQueueFactory = getCache().createAsyncEventQueueFactory();
    asyncEventQueueFactory.setBatchConflationEnabled(isBatchConflationEnabled);
    asyncEventQueueFactory.setBatchSize(batchSize);
    asyncEventQueueFactory.setDiskStoreName(diskStoreName);
    asyncEventQueueFactory.setDiskSynchronous(isDiskSynchronous);
    asyncEventQueueFactory.setDispatcherThreads(1);
    asyncEventQueueFactory.setMaximumQueueMemory(maximumQueueMemory);
    asyncEventQueueFactory.setParallel(false);
    asyncEventQueueFactory.setPersistent(true);

    asyncEventQueueFactory.create(asyncEventQueueId, asyncEventListener);
  }

  private void doPuts(String regionName, int numPuts) {
    Region<Integer, Integer> region = getCache().getRegion(regionName);
    for (int i = 0; i < numPuts; i++) {
      region.put(i, i);
    }
  }

  /**
   * Validate whether all the attributes set on AsyncEventQueueFactory are set on the sender
   * underneath the AsyncEventQueue.
   */
  private void validateAsyncEventQueueAttributes(int maxQueueMemory,
      int batchSize,
      int batchTimeInterval,
      boolean isPersistent,
      String diskStoreName,
      boolean isDiskSynchronous,
      boolean batchConflationEnabled) {
    GatewaySender gatewaySender = getInternalGatewaySender();

    assertThat(gatewaySender.getMaximumQueueMemory()).isEqualTo(maxQueueMemory);
    assertThat(gatewaySender.getBatchSize()).isEqualTo(batchSize);
    assertThat(gatewaySender.getBatchTimeInterval()).isEqualTo(batchTimeInterval);
    assertThat(gatewaySender.isPersistenceEnabled()).isEqualTo(isPersistent);
    assertThat(gatewaySender.getDiskStoreName()).isEqualTo(diskStoreName);
    assertThat(gatewaySender.isDiskSynchronous()).isEqualTo(isDiskSynchronous);
    assertThat(gatewaySender.isBatchConflationEnabled()).isEqualTo(batchConflationEnabled);
  }

  private void validateThrowingAsyncEventListenerEventsMap(int expectedSize) {
    Map<?, AsyncEvent<?, ?>> eventsMap = getThrowingAsyncEventListener().getEventsMap();

    await().untilAsserted(() -> assertThat(eventsMap).hasSize(expectedSize));

    for (AsyncEvent<?, ?> event : eventsMap.values()) {
      assertThat(event.getPossibleDuplicate()).isTrue();
    }
  }

  private void validateThrowingAsyncEventListenerExceptionThrown() {
    await()
        .untilAsserted(() -> assertThat(isThrowingAsyncEventListenerExceptionThrown()).isTrue());
  }

  private void waitForDispatcherToPause() {
    getInternalGatewaySender().getEventProcessor().waitForDispatcherToPause();
  }

  private InternalGatewaySender getInternalGatewaySender() {
    InternalGatewaySender gatewaySender = getInternalAsyncEventQueue().getSender();
    assertThat(gatewaySender).isNotNull();
    return gatewaySender;
  }

  private boolean isThrowingAsyncEventListenerExceptionThrown() {
    return getThrowingAsyncEventListener().isExceptionThrown();
  }

  private ThrowingAsyncEventListener getThrowingAsyncEventListener() {
    return (ThrowingAsyncEventListener) getAsyncEventListener();
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

  private static class ThrowingAsyncEventListener<K, V> implements AsyncEventListener {

    private final Map<K, AsyncEvent<K, V>> eventsMap = new ConcurrentHashMap<>();
    private volatile boolean exceptionThrown;

    Map<K, AsyncEvent<K, V>> getEventsMap() {
      assertThat(eventsMap).isNotNull();
      return eventsMap;
    }

    boolean isExceptionThrown() {
      return exceptionThrown;
    }

    @Override
    public synchronized boolean processEvents(List<AsyncEvent> events) {
      int i = 0;
      for (AsyncEvent<K, V> event : events) {
        i++;
        if (!exceptionThrown && i == 40) {
          i = 0;
          exceptionThrown = true;
          throw new Error("Thrown by ThrowingAsyncEventListener");
        }
        if (exceptionThrown) {
          eventsMap.put(event.getKey(), event);
        }
      }
      return true;
    }
  }
}
