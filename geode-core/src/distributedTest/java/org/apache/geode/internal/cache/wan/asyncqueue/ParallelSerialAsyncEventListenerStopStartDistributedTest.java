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

import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getCurrentVMNum;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.asyncqueue.internal.InternalAsyncEventQueue;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

/**
 * Extracted from {@link AsyncEventListenerDistributedTest}.
 */
@Category(AEQTest.class)
@SuppressWarnings("serial")
public class ParallelSerialAsyncEventListenerStopStartDistributedTest implements Serializable {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  private String basePartitionedRegionName;
  private String baseReplicateRegionName;
  private String baseSerialAsyncEventQueueId;
  private String baseParallelAsyncEventQueueId;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Before
  public void setUp() throws Exception {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    String className = getClass().getSimpleName();
    basePartitionedRegionName = className + "_PR_";
    baseReplicateRegionName = className + "_RR_";
    baseSerialAsyncEventQueueId = className + "_serialAEQ_";
    baseParallelAsyncEventQueueId = className + "_parallelAEQ_";
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

  @Test
  public void testStopStartPersistentParallelAndSerialAsyncEventQueues() {
    // Create cache
    vm0.invoke(this::createCache);
    vm1.invoke(this::createCache);
    vm2.invoke(this::createCache);
    vm3.invoke(this::createCache);

    // Create several serial and parallel AEQs
    int numAEQs = 3;
    vm0.invoke(() -> createAsyncEventQueues(numAEQs));
    vm1.invoke(() -> createAsyncEventQueues(numAEQs));
    vm2.invoke(() -> createAsyncEventQueues(numAEQs));
    vm3.invoke(() -> createAsyncEventQueues(numAEQs));

    // Create replicated and partitioned regions attached to those AEQs
    int numRegions = 3;
    vm0.invoke(() -> createRegions(numRegions));
    vm1.invoke(() -> createRegions(numRegions));
    vm2.invoke(() -> createRegions(numRegions));
    vm3.invoke(() -> createRegions(numRegions));

    // Do puts into all replicated and partitioned regions
    vm0.invoke(() -> doPuts(numRegions, 1000));

    // Wait for all AEQs to be empty
    vm0.invoke(this::waitForAsyncQueuesToEmpty);
    vm1.invoke(this::waitForAsyncQueuesToEmpty);
    vm2.invoke(this::waitForAsyncQueuesToEmpty);
    vm3.invoke(this::waitForAsyncQueuesToEmpty);

    // Stop all AEQs
    vm0.invoke(this::stopAsyncQueues);
    vm1.invoke(this::stopAsyncQueues);
    vm2.invoke(this::stopAsyncQueues);
    vm3.invoke(this::stopAsyncQueues);

    // Start all AEQs
    AsyncInvocation startAeqsVm0 = vm0.invokeAsync(this::startAsyncQueues);
    AsyncInvocation startAeqsVm1 = vm1.invokeAsync(this::startAsyncQueues);
    AsyncInvocation startAeqsVm2 = vm2.invokeAsync(this::startAsyncQueues);
    AsyncInvocation startAeqsVm3 = vm3.invokeAsync(this::startAsyncQueues);

    // Wait for async tasks to complete
    ThreadUtils.join(startAeqsVm0, 120 * 1000);
    ThreadUtils.join(startAeqsVm1, 120 * 1000);
    ThreadUtils.join(startAeqsVm2, 120 * 1000);
    ThreadUtils.join(startAeqsVm3, 120 * 1000);
  }

  private void createAsyncEventQueues(int numAEQs) {
    for (int i = 0; i < numAEQs; i++) {
      createPersistentAsyncEventQueue(baseSerialAsyncEventQueueId + i,
          createDiskStoreName(baseSerialAsyncEventQueueId + i), false);
      createPersistentAsyncEventQueue(baseParallelAsyncEventQueueId + i,
          createDiskStoreName(baseParallelAsyncEventQueueId + i), true);
    }
  }

  private void createRegions(int numRegions) {
    for (int i = 0; i < numRegions; i++) {
      createReplicateRegion(baseReplicateRegionName + i, baseSerialAsyncEventQueueId + i);
      createPartitionedRegion(basePartitionedRegionName + i, baseParallelAsyncEventQueueId + i);
    }
  }

  private void waitForAsyncQueuesToEmpty() {
    for (final AsyncEventQueue aeq : getAsyncEventQueues()) {
      await().until(() -> aeq.size() == 0);
    }
  }

  private void stopAsyncQueues() {
    for (final AsyncEventQueue aeq : getAsyncEventQueues()) {
      InternalAsyncEventQueue iaeq = (InternalAsyncEventQueue) aeq;
      assertThat(iaeq.getSender().isRunning()).isTrue();
      iaeq.stop();
      assertThat(iaeq.getSender().isRunning()).isFalse();
    }
  }

  private void startAsyncQueues() {
    for (final AsyncEventQueue aeq : getAsyncEventQueues()) {
      GatewaySender sender = ((InternalAsyncEventQueue) aeq).getSender();
      assertThat(sender.isRunning()).isFalse();
      sender.start();
      assertThat(sender.isRunning()).isTrue();
    }
  }

  private Collection<AsyncEventQueue> getAsyncEventQueues() {
    return getCache().getAsyncEventQueues().stream()
        .sorted(Comparator.comparing(AsyncEventQueue::getId)).collect(Collectors.toList());
  }

  private InternalCache getCache() {
    return cacheRule.getOrCreateCache(getDistributedSystemProperties());
  }

  private void createCache() {
    cacheRule.createCache(getDistributedSystemProperties());
  }

  private void createPartitionedRegion(String regionName, String asyncEventQueueId) {
    assertThat(regionName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION_REDUNDANT);
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

  private void createPersistentAsyncEventQueue(String asyncEventQueueId, String diskStoreName,
      boolean isParallel) {
    createDiskStore(diskStoreName, asyncEventQueueId);

    AsyncEventQueueFactory asyncEventQueueFactory = getCache().createAsyncEventQueueFactory();
    asyncEventQueueFactory.setDiskStoreName(diskStoreName);
    asyncEventQueueFactory.setParallel(isParallel);
    asyncEventQueueFactory.setPersistent(true);

    asyncEventQueueFactory.create(asyncEventQueueId, new TestAsyncEventListener());
  }

  private void doPuts(int numRegions, int numPuts) {
    for (int i = 0; i < numRegions; i++) {
      Region<Integer, Integer> rr = getCache().getRegion(baseReplicateRegionName + i);
      Region<Integer, Integer> pr = getCache().getRegion(basePartitionedRegionName + i);
      for (int j = 0; j < numPuts; j++) {
        rr.put(j, j);
        pr.put(j, j);
      }
    }
  }

  private static class TestAsyncEventListener implements AsyncEventListener {

    @Override
    public synchronized boolean processEvents(List<AsyncEvent> events) {
      return true;
    }
  }
}
