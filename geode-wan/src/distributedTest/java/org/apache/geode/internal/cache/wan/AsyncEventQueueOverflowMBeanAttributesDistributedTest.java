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
package org.apache.geode.internal.cache.wan;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.DiskRegionStats;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.management.AsyncEventQueueMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
@RunWith(JUnitParamsRunner.class)
public class AsyncEventQueueOverflowMBeanAttributesDistributedTest extends AsyncEventQueueTestBase {

  @Test
  @Parameters({"true", "false"})
  public void testParallelAsyncEventQueueOverflowMBeanAttributes(boolean createSenderFirst)
      throws Exception {
    // Start locator
    Integer locatorPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));

    // Create the cache
    vm1.invoke(() -> createCache(locatorPort));

    String aeqId = "db";
    if (createSenderFirst) {
      // Create an async event queue then a region (normal xml order)

      // Create a async event queue
      vm1.invoke(() -> createAsyncEventQueue(aeqId, true, 1, 10, false, false, null, true,
          new WaitingAsyncEventListener()));

      // Create a partitioned region attached to the async event queue
      vm1.invoke(() -> createPartitionedRegionWithAsyncEventQueue(getTestMethodName(), aeqId,
          isOffHeap()));
    } else {
      // Create a region then an async event queue

      // Create a partitioned region attached to the async event queue
      vm1.invoke(() -> createPartitionedRegionWithAsyncEventQueue(getTestMethodName(), aeqId,
          isOffHeap()));

      // Create a async event queue
      vm1.invoke(() -> createAsyncEventQueue(aeqId, true, 1, 10, false, false, null, true,
          new WaitingAsyncEventListener()));
    }

    // Do some puts to cause overflow
    int numPuts = 10;
    vm1.invoke(() -> doHeavyPuts(getTestMethodName(), numPuts));

    // Compare overflow stats to mbean attributes
    vm1.invoke(() -> compareParallelOverflowStatsToMBeanAttributes(aeqId));

    // Resume the listener
    vm1.invoke(() -> startProcessingAsyncEvents(aeqId));

    // Wait for queue to drain
    vm1.invoke(() -> waitForAsyncQueueToGetEmpty(aeqId));

    // Compare overflow stats to mbean attributes
    vm1.invoke(() -> compareParallelOverflowStatsToMBeanAttributes(aeqId));
  }

  @Test
  @Parameters({"true", "false"})
  public void testSerialAsyncEventQueueOverflowMBeanAttributes(boolean createSenderFirst)
      throws Exception {
    // Start locator
    Integer locatorPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));

    // Create the cache
    vm1.invoke(() -> createCache(locatorPort));

    String aeqId = "db";
    if (createSenderFirst) {
      // Create an async event queue then a region (normal xml order)

      // Create a async event queue
      vm1.invoke(() -> createAsyncEventQueue(aeqId, false, 10, 10, false, false, null, true, 5,
          new WaitingAsyncEventListener()));

      // Create a partitioned region attached to the async event queue
      vm1.invoke(() -> createPartitionedRegionWithAsyncEventQueue(getTestMethodName(), aeqId,
          isOffHeap()));
    } else {
      // Create a region then an async event queue

      // Create a partitioned region attached to the async event queue
      vm1.invoke(() -> createPartitionedRegionWithAsyncEventQueue(getTestMethodName(), aeqId,
          isOffHeap()));

      // Create a async event queue
      vm1.invoke(() -> createAsyncEventQueue(aeqId, false, 1, 10, false, false, null, true,
          new WaitingAsyncEventListener()));
    }

    // Do some puts to cause overflow
    int numPuts = 15;
    vm1.invoke(() -> doHeavyPuts(getTestMethodName(), numPuts));

    // Compare overflow stats to mbean attributes
    vm1.invoke(() -> compareSerialOverflowStatsToMBeanAttributes(aeqId));

    // Resume the listener
    vm1.invoke(() -> startProcessingAsyncEvents(aeqId));

    // Wait for queue to drain
    vm1.invoke(() -> waitForAsyncQueueToGetEmpty(aeqId));

    // Compare overflow stats to mbean attributes
    vm1.invoke(() -> compareSerialOverflowStatsToMBeanAttributes(aeqId));
  }

  private void compareParallelOverflowStatsToMBeanAttributes(String aeqId) throws Exception {
    // Get disk region stats associated with the queue region
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(
        AsyncEventQueueImpl.ASYNC_EVENT_QUEUE_PREFIX + aeqId + ParallelGatewaySenderQueue.QSTRING);
    DiskRegionStats drs = region.getDiskRegionStats();
    assertThat(drs).isNotNull();

    // Get async event queue mbean
    ManagementService service = ManagementService.getManagementService(cache);
    AsyncEventQueueMXBean bean = service.getLocalAsyncEventQueueMXBean(aeqId);
    assertThat(bean).isNotNull();

    // Wait for the sampler to take a few samples
    waitForSamplerToSample(5);

    // Verify the bean attributes match the stat values
    await().untilAsserted(() -> {
      assertThat(bean.getEntriesOverflowedToDisk()).isEqualTo(drs.getNumOverflowOnDisk());
      assertThat(bean.getBytesOverflowedToDisk()).isEqualTo(drs.getNumOverflowBytesOnDisk());
    });
  }

  private void compareSerialOverflowStatsToMBeanAttributes(String aeqId) throws Exception {
    // Get the async event queue
    AsyncEventQueueImpl aeq = (AsyncEventQueueImpl) cache.getAsyncEventQueue(aeqId);

    // Get the sender
    AbstractGatewaySender sender = (AbstractGatewaySender) aeq.getSender();

    // Get the sender's queue regions
    Set<RegionQueue> queues = sender.getQueues();

    // Get async event queue mbean
    ManagementService service = ManagementService.getManagementService(cache);
    AsyncEventQueueMXBean bean = service.getLocalAsyncEventQueueMXBean(aeqId);
    assertThat(bean).isNotNull();

    // Wait for the sampler to take a few samples
    waitForSamplerToSample(5);

    // Verify the bean attributes match the stat values
    await().untilAsserted(() -> {
      // Calculate the total entries and bytes overflowed to disk
      int entriesOverflowedToDisk = 0;
      long bytesOverflowedToDisk = 0l;
      for (RegionQueue queue : queues) {
        LocalRegion lr = (LocalRegion) queue.getRegion();
        DiskRegionStats drs = lr.getDiskRegion().getStats();
        entriesOverflowedToDisk += drs.getNumOverflowOnDisk();
        bytesOverflowedToDisk += drs.getNumOverflowBytesOnDisk();
      }

      // Verify the bean attributes match the stat values
      assertThat(bean.getEntriesOverflowedToDisk()).isEqualTo(entriesOverflowedToDisk);
      assertThat(bean.getBytesOverflowedToDisk()).isEqualTo(bytesOverflowedToDisk);
    });
  }

  private void waitForSamplerToSample(int numTimesToSample) throws Exception {
    InternalDistributedSystem ids = (InternalDistributedSystem) cache.getDistributedSystem();
    assertThat(ids.getStatSampler().waitForSampleCollector(60000)).isNotNull();
    for (int i = 0; i < numTimesToSample; i++) {
      assertThat(ids.getStatSampler().waitForSample((60000))).isTrue();
    }
  }

  private void startProcessingAsyncEvents(String aeqId) {
    // Get the async event listener
    WaitingAsyncEventListener listener = getWaitingAsyncEventListener(aeqId);

    // Start processing waiting events
    listener.startProcessingEvents();
  }

  private WaitingAsyncEventListener getWaitingAsyncEventListener(String aeqId) {
    // Get the async event queue
    AsyncEventQueue aeq = cache.getAsyncEventQueue(aeqId);
    assertThat(aeq).isNotNull();

    // Get and return the async event listener
    AsyncEventListener aeqListener = aeq.getAsyncEventListener();
    assertThat(aeqListener).isInstanceOf(WaitingAsyncEventListener.class);
    return (WaitingAsyncEventListener) aeqListener;
  }
}
