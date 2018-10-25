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

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getCurrentVMNum;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

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
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category(AEQTest.class)
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@SuppressWarnings("serial")
public class AsyncEventListenerDistributedTest implements Serializable {

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

  @Test // serial, ReplicateRegion
  public void testSerialAsyncEventQueueSize() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100));
    vm2.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100));

    vm0.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm1.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm2.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));

    vm0.invoke(() -> getInternalGatewaySender().pause());
    vm1.invoke(() -> getInternalGatewaySender().pause());
    vm2.invoke(() -> getInternalGatewaySender().pause());

    vm0.invoke(() -> waitForDispatcherToPause());
    vm1.invoke(() -> waitForDispatcherToPause());
    vm2.invoke(() -> waitForDispatcherToPause());

    vm0.invoke(() -> doPuts(replicateRegionName, 1000));

    assertThat(vm0.invoke(() -> getAsyncEventQueue().size())).isEqualTo(1000);
    assertThat(vm1.invoke(() -> getAsyncEventQueue().size())).isEqualTo(1000);
    assertThat(vm2.invoke(() -> getAsyncEventQueue().size())).isEqualTo(1000);
  }

  @Test // serial, ReplicateRegion
  public void testReplicatedSerialAsyncEventQueue() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100));
    vm2.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100));

    vm0.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm1.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm2.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));

    vm0.invoke(() -> doPuts(replicateRegionName, 1000));

    // primary sender
    vm0.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(1000));

    // secondaries
    vm1.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(0));
    vm2.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(0));
  }

  @Test // serial, conflation, ReplicateRegion
  public void testReplicatedSerialAsyncEventQueueWithConflationEnabled() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true,
        100, dispatcherThreadCount, 100));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true,
        100, dispatcherThreadCount, 100));
    vm2.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true,
        100, dispatcherThreadCount, 100));

    vm0.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm1.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm2.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));

    vm0.invoke(() -> getInternalGatewaySender().pause());
    vm1.invoke(() -> getInternalGatewaySender().pause());
    vm2.invoke(() -> getInternalGatewaySender().pause());

    vm0.invoke(() -> waitForDispatcherToPause());
    vm1.invoke(() -> waitForDispatcherToPause());
    vm2.invoke(() -> waitForDispatcherToPause());

    Map<Integer, Integer> keyValues = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm0.invoke(() -> {
      putGivenKeyValue(replicateRegionName, keyValues);
      waitForAsyncEventQueueSize(keyValues.size());
    });

    Map<Integer, String> updateKeyValues = new HashMap<>();
    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    // Put the update events and check the queue size.
    // There should be no conflation with the previous create events.
    vm0.invoke(() -> {
      putGivenKeyValue(replicateRegionName, updateKeyValues);
      waitForAsyncEventQueueSize(keyValues.size() + updateKeyValues.size());
    });

    // Put the update events again and check the queue size.
    // There should be conflation with the previous update events.
    vm0.invoke(() -> {
      putGivenKeyValue(replicateRegionName, updateKeyValues);
      waitForAsyncEventQueueSize(keyValues.size() + updateKeyValues.size());
    });

    vm0.invoke(() -> getInternalGatewaySender().resume());
    vm1.invoke(() -> getInternalGatewaySender().resume());
    vm2.invoke(() -> getInternalGatewaySender().resume());

    // primary sender
    vm0.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(1000));

    // secondaries
    vm1.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(0));
    vm2.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(0));
  }

  @Test // serial, persistent, conflation, ReplicateRegion
  public void testReplicatedSerialAsyncEventQueueWithPersistenceEnabled() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 100, createDiskStoreName(asyncEventQueueId), false, dispatcherThreadCount, 100));
    vm1.invoke(() -> createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 100, createDiskStoreName(asyncEventQueueId), false, dispatcherThreadCount, 100));
    vm2.invoke(() -> createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        true, 100, createDiskStoreName(asyncEventQueueId), false, dispatcherThreadCount, 100));

    vm0.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm1.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm2.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));

    vm0.invoke(() -> doPuts(replicateRegionName, 1000));

    // primary sender
    vm0.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(1000));

    // secondaries
    vm1.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(0));
    vm2.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(0));
  }

  @Test // serial, persistent, ReplicateRegion, IntegrationTest
  public void testReplicatedSerialAsyncEventQueueWithPersistenceEnabled_Restart() {
    vm0.invoke(() -> {
      createCache();

      createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false, 100,
          createDiskStoreName(asyncEventQueueId), true, dispatcherThreadCount, 100);

      createReplicateRegion(replicateRegionName, asyncEventQueueId);

      // pause async channel and then do the puts
      getInternalGatewaySender().pause();
      waitForDispatcherToPause();

      doPuts(replicateRegionName, 1000);

      // kill vm0 and rebuild
      getCache().close();

      createCache();
      createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false, 100,
          createDiskStoreName(asyncEventQueueId), true, dispatcherThreadCount, 100);
      createReplicateRegion(replicateRegionName, asyncEventQueueId);

      // primary sender
      waitForAsyncEventListenerWithEventsMapSize(1000);
    });
  }

  /**
   * There are 3 VMs in the site and the VM with primary sender is shut down.
   *
   * <p>
   * TODO: fix this test
   */
  @Ignore("TODO: Disabled for 52351")
  @Test // serial, persistent, ReplicateRegion
  public void testReplicatedSerialAsyncEventQueueWithPersistenceEnabled_Restart2() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        false, 100, createDiskStoreName(asyncEventQueueId), true, dispatcherThreadCount, 100));
    vm1.invoke(() -> createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        false, 100, createDiskStoreName(asyncEventQueueId), true, dispatcherThreadCount, 100));
    vm2.invoke(() -> createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        false, 100, createDiskStoreName(asyncEventQueueId), true, dispatcherThreadCount, 100));

    vm0.invoke(() -> {
      createReplicateRegion(replicateRegionName, asyncEventQueueId);
      addClosingCacheListener(replicateRegionName, 900);
    });

    vm1.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));
    vm2.invoke(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId));

    vm1.invoke(() -> waitForSenderToBecomePrimary());
    vm1.invoke(() -> doPuts(replicateRegionName, 2000));

    vm1.invoke(() -> waitForRegionQueuesToEmpty());
    vm2.invoke(() -> waitForRegionQueuesToEmpty());

    int vm1size = vm1.invoke(() -> ((Map<?, ?>) getSpyAsyncEventListener().getEventsMap()).size());
    int vm2size = vm2.invoke(() -> ((Map<?, ?>) getSpyAsyncEventListener().getEventsMap()).size());

    // verify that there is no event loss (fails)
    assertThat(vm1size + vm2size).isGreaterThanOrEqualTo(2000);
  }

  @Test // serial, PartitionedRegion
  public void testPartitionedSerialAsyncEventQueue() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100));
    vm2.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100));

    vm0.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm1.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm2.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));

    vm0.invoke(() -> doPuts(partitionedRegionName, 500));
    vm1.invoke(() -> doPutsFrom(partitionedRegionName, 500, 1000));

    // primary sender
    vm0.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(1000));

    // secondaries
    vm1.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(0));
    vm2.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(0));
  }

  @Test // serial, conflation, PartitionedRegion
  public void testPartitionedSerialAsyncEventQueueWithConflationEnabled() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true,
        100, dispatcherThreadCount, 100));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true,
        100, dispatcherThreadCount, 100));
    vm2.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true,
        100, dispatcherThreadCount, 100));

    vm0.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm1.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm2.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));

    vm0.invoke(() -> getInternalGatewaySender().pause());
    vm1.invoke(() -> getInternalGatewaySender().pause());
    vm2.invoke(() -> getInternalGatewaySender().pause());

    vm0.invoke(() -> waitForDispatcherToPause());
    vm1.invoke(() -> waitForDispatcherToPause());
    vm2.invoke(() -> waitForDispatcherToPause());

    Map<Integer, Integer> keyValues = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm0.invoke(() -> {
      putGivenKeyValue(partitionedRegionName, keyValues);
      waitForAsyncEventQueueSize(keyValues.size());
    });

    Map<Integer, String> updateKeyValues = new HashMap<>();
    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    // Put the update events and check the queue size.
    // There should be no conflation with the previous create events.
    vm1.invoke(() -> {
      putGivenKeyValue(partitionedRegionName, updateKeyValues);
      waitForAsyncEventQueueSize(keyValues.size() + updateKeyValues.size());
    });

    // Put the update events again and check the queue size.
    // There should be conflation with the previous update events.
    vm1.invoke(() -> {
      putGivenKeyValue(partitionedRegionName, updateKeyValues);
      waitForAsyncEventQueueSize(keyValues.size() + updateKeyValues.size());
    });

    vm0.invoke(() -> getInternalGatewaySender().resume());
    vm1.invoke(() -> getInternalGatewaySender().resume());
    vm2.invoke(() -> getInternalGatewaySender().resume());

    // primary sender
    vm0.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(1000));

    // secondaries
    vm1.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(0));
    vm2.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(0));
  }

  /**
   * Test configuration::
   *
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async channel persistence
   * enabled: true
   *
   * No VM is restarted.
   */
  @Test // persistent, PartitionedRegion
  public void testPartitionedSerialAsyncEventQueueWithPersistenceEnabled() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());

    vm0.invoke(() -> createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        false, 100, createDiskStoreName(asyncEventQueueId), false, dispatcherThreadCount, 100));
    vm1.invoke(() -> createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        false, 100, createDiskStoreName(asyncEventQueueId), false, dispatcherThreadCount, 100));
    vm2.invoke(() -> createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        false, 100, createDiskStoreName(asyncEventQueueId), false, dispatcherThreadCount, 100));

    vm0.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm1.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm2.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));

    vm0.invoke(() -> doPuts(partitionedRegionName, 500));
    vm1.invoke(() -> doPutsFrom(partitionedRegionName, 500, 1000)); // hangs?

    // primary sender
    vm0.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(1000));

    // secondaries
    vm1.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(0));
    vm2.invoke(() -> waitForAsyncEventListenerWithEventsMapSize(0));
  }

  @Test // persistent, PartitionedRegion, IntegrationTest
  public void testPartitionedSerialAsyncEventQueueWithPersistenceEnabled_Restart() {
    vm0.invoke(() -> {
      createCache();

      createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false, 100,
          createDiskStoreName(asyncEventQueueId), true, dispatcherThreadCount, 100);

      createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16);

      // pause async channel and then do the puts
      getInternalGatewaySender().pause();
      waitForDispatcherToPause();

      doPuts(partitionedRegionName, 1000);

      getCache().close();

      createCache();
      createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false, 100,
          createDiskStoreName(asyncEventQueueId), true, dispatcherThreadCount, 100);
      createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16);

      // primary sender
      waitForAsyncEventListenerWithEventsMapSize(1000);
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
      int redundantCopies,
      int totalNumBuckets) {
    assertThat(regionName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.addAsyncEventQueueId(asyncEventQueueId);
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
      int dispatcherThreads,
      int maximumQueueMemory) {
    assertThat(asyncEventQueueId).isNotEmpty();
    assertThat(asyncEventListener).isNotNull();

    AsyncEventQueueFactory asyncEventQueueFactory = getCache().createAsyncEventQueueFactory();
    asyncEventQueueFactory.setBatchConflationEnabled(isBatchConflationEnabled);
    asyncEventQueueFactory.setBatchSize(batchSize);
    asyncEventQueueFactory.setDispatcherThreads(dispatcherThreads);
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
      int dispatcherThreads,
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
    asyncEventQueueFactory.setDispatcherThreads(dispatcherThreads);
    asyncEventQueueFactory.setMaximumQueueMemory(maximumQueueMemory);
    asyncEventQueueFactory.setParallel(false);
    asyncEventQueueFactory.setPersistent(true);

    asyncEventQueueFactory.create(asyncEventQueueId, asyncEventListener);
  }

  private void addClosingCacheListener(String regionName, int closeAfterCreateKey) {
    assertThat(regionName).isNotEmpty();

    Region region = getCache().getRegion(regionName);
    assertNotNull(region);

    CacheListenerAdapter cacheListener = new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        if ((Integer) event.getKey() == closeAfterCreateKey) {
          getCache().close();
        }
      }
    };

    region.getAttributesMutator().addCacheListener(cacheListener);
  }

  private void doPuts(String regionName, int numPuts) {
    Region<Integer, Integer> region = getCache().getRegion(regionName);
    for (int i = 0; i < numPuts; i++) {
      region.put(i, i);
    }
  }

  private void doPutsFrom(String regionName, int from, int numPuts) {
    Region<Integer, Integer> region = getCache().getRegion(regionName);
    for (int i = from; i < numPuts; i++) {
      region.put(i, i);
    }
  }

  private void putGivenKeyValue(String regionName, Map<?, ?> keyValues) {
    Region<Object, Object> region = getCache().getRegion(regionName);
    for (Object key : keyValues.keySet()) {
      region.put(key, keyValues.get(key));
    }
  }

  private void assertRegionQueuesAreEmpty(InternalGatewaySender gatewaySender) {
    Set<RegionQueue> regionQueues = gatewaySender.getQueues();
    for (RegionQueue queue : regionQueues) {
      assertThat(queue.size()).isZero();
    }
  }

  private int getTotalRegionQueueSize() {
    InternalGatewaySender gatewaySender = getInternalGatewaySender();

    int totalSize = 0;
    for (RegionQueue regionQueue : gatewaySender.getQueues()) {
      totalSize += regionQueue.size();
    }
    return totalSize;
  }

  private void waitForAsyncEventListenerWithEventsMapSize(int expectedSize) {
    await().untilAsserted(
        () -> assertThat(getSpyAsyncEventListener().getEventsMap()).hasSize(expectedSize));
  }

  private void waitForAsyncEventQueueSize(int expectedRegionQueueSize) {
    await().untilAsserted(
        () -> assertThat(getTotalRegionQueueSize()).isEqualTo(expectedRegionQueueSize));
  }

  private void waitForDispatcherToPause() {
    getInternalGatewaySender().getEventProcessor().waitForDispatcherToPause();
  }

  private void waitForRegionQueuesToEmpty() {
    await()
        .untilAsserted(() -> assertRegionQueuesAreEmpty(getInternalGatewaySender()));
  }

  private void waitForSenderToBecomePrimary() {
    InternalGatewaySender gatewaySender = getInternalGatewaySender();
    await().untilAsserted(
        () -> assertThat(gatewaySender.isPrimary()).as("gatewaySender: " + gatewaySender).isTrue());
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
