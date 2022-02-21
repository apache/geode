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
package org.apache.geode.internal.cache.wan.parallel;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.internal.cache.BucketRegionQueue;
import org.apache.geode.internal.cache.BucketRegionQueueHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.statistics.DummyStatisticsFactory;
import org.apache.geode.test.fake.Fakes;

public class ParallelQueueSetPossibleDuplicateMessageJUnitTest {

  private static final String GATEWAY_SENDER_ID = "ny";
  private static final int BUCKET_ID = 85;
  private static final long KEY = 198;

  private GemFireCacheImpl cache;
  private PartitionedRegion queueRegion;
  private AbstractGatewaySender sender;
  private PartitionedRegion rootRegion;
  private BucketRegionQueue bucketRegionQueue;
  private BucketRegionQueueHelper bucketRegionQueueHelper;
  private GatewaySenderStats stats;

  @Before
  public void setUpGemFire() {
    createCache();
    createQueueRegion();
    createGatewaySender();
    createRootRegion();
    createBucketRegionQueue();
  }

  private void createCache() {
    // Mock cache
    cache = Fakes.cache();
  }

  private void createQueueRegion() {
    // Mock queue region
    queueRegion =
        ParallelGatewaySenderHelper.createMockQueueRegion(cache,
            ParallelGatewaySenderHelper.getRegionQueueName(GATEWAY_SENDER_ID));
  }

  private void createGatewaySender() {
    // Mock gateway sender
    sender = ParallelGatewaySenderHelper.createGatewaySender(cache);
    when(queueRegion.getParallelGatewaySender()).thenReturn(sender);
    when(sender.getQueues()).thenReturn(null);
    when(sender.getDispatcherThreads()).thenReturn(1);
    stats = new GatewaySenderStats(new DummyStatisticsFactory(), "gatewaySenderStats-", "ln",
        disabledClock());
    when(sender.getStatistics()).thenReturn(stats);
  }

  private void createRootRegion() {
    // Mock root region
    rootRegion = mock(PartitionedRegion.class);
    when(rootRegion.getFullPath())
        .thenReturn(SEPARATOR + PartitionedRegionHelper.PR_ROOT_REGION_NAME);
    when(cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME, true))
        .thenReturn(rootRegion);
    when(cache.getRegion(ParallelGatewaySenderHelper.getRegionQueueName(GATEWAY_SENDER_ID)))
        .thenReturn(queueRegion);
  }

  private void createBucketRegionQueue() {
    // Create BucketRegionQueue
    BucketRegionQueue realBucketRegionQueue = ParallelGatewaySenderHelper
        .createBucketRegionQueue(cache, rootRegion, queueRegion, BUCKET_ID);
    bucketRegionQueue = spy(realBucketRegionQueue);

    bucketRegionQueueHelper =
        new BucketRegionQueueHelper(cache, queueRegion, bucketRegionQueue);
  }

  @Test
  public void validateSetPossibleDuplicateKeyInUninitializedBucketRegionQueue() throws Exception {

    assertFalse(bucketRegionQueue.isInitialized());

    // Create a real ConcurrentParallelGatewaySenderQueue
    ParallelGatewaySenderEventProcessor processor =
        ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(sender);
    GatewaySenderEventImpl gsEvent = mock(GatewaySenderEventImpl.class);

    // Add a mock GatewaySenderEventImpl to the temp queue
    BlockingQueue<GatewaySenderEventImpl> tempQueue =
        createTempQueueAndAddEvent(processor, gsEvent);
    assertEquals(1, tempQueue.size());

    createAndProcessParallelQueueSetPossibleDuplicateMessage();

    verify(sender, times(1)).markAsDuplicateInTempQueueEvents(KEY);
    verify(bucketRegionQueue, times(0)).setAsPossibleDuplicate(KEY);

  }

  @Test
  public void validateSetPossibleDuplicateKeyInInitializedBucketRegionQueue() throws Exception {

    assertFalse(bucketRegionQueue.isInitialized());

    // Create a real ConcurrentParallelGatewaySenderQueue
    ParallelGatewaySenderEventProcessor processor =
        ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(sender);
    GatewaySenderEventImpl gsEvent = mock(GatewaySenderEventImpl.class);

    // Add a mock GatewaySenderEventImpl to the temp queue
    BlockingQueue<GatewaySenderEventImpl> tempQueue =
        createTempQueueAndAddEvent(processor, gsEvent);
    assertEquals(1, tempQueue.size());

    // Clean up destroyed tokens
    bucketRegionQueueHelper.cleanUpDestroyedTokensAndMarkGIIComplete();

    assertTrue(bucketRegionQueue.isInitialized());

    bucketRegionQueue.pushKeyIntoQueue(KEY);

    createAndProcessParallelQueueSetPossibleDuplicateMessage();

    verify(sender, times(1)).markAsDuplicateInTempQueueEvents(KEY);
    verify(bucketRegionQueue, times(1)).setAsPossibleDuplicate(KEY);

  }

  private void createAndProcessParallelQueueSetPossibleDuplicateMessage() {
    ParallelQueueSetPossibleDuplicateMessage message =
        new ParallelQueueSetPossibleDuplicateMessage(createRegionToDispatchedKeysMap());
    message.process((ClusterDistributionManager) cache.getDistributionManager());
  }

  private HashMap<String, Map<Integer, List<Object>>> createRegionToDispatchedKeysMap() {
    HashMap<String, Map<Integer, List<Object>>> regionToDispatchedKeys = new HashMap<>();
    Map<Integer, List<Object>> bucketIdToDispatchedKeys = new HashMap<>();
    List<Object> dispatchedKeys = new ArrayList<>();
    dispatchedKeys.add(KEY);
    bucketIdToDispatchedKeys.put(BUCKET_ID, dispatchedKeys);
    regionToDispatchedKeys.put(ParallelGatewaySenderHelper.getRegionQueueName(GATEWAY_SENDER_ID),
        bucketIdToDispatchedKeys);
    return regionToDispatchedKeys;
  }

  private BlockingQueue<GatewaySenderEventImpl> createTempQueueAndAddEvent(
      ParallelGatewaySenderEventProcessor processor, GatewaySenderEventImpl event) {
    ParallelGatewaySenderQueue queue = (ParallelGatewaySenderQueue) processor.getQueue();
    Map<Integer, BlockingQueue<GatewaySenderEventImpl>> tempQueueMap =
        queue.getBucketToTempQueueMap();
    BlockingQueue<GatewaySenderEventImpl> tempQueue = new LinkedBlockingQueue<>();
    when(event.getShadowKey()).thenReturn(KEY);
    tempQueue.add(event);
    tempQueueMap.put(BUCKET_ID, tempQueue);
    return tempQueue;
  }
}
