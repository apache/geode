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
import static org.apache.geode.internal.cache.wan.parallel.ParallelQueueSetPossibleDuplicateMessage.UNSUCCESSFULLY_DISPATCHED;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
import java.util.function.ToDoubleFunction;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DSClock;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegionQueue;
import org.apache.geode.internal.cache.BucketRegionQueueHelper;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.statistics.DummyStatisticsFactory;
import org.apache.geode.internal.statistics.StatisticsManager;

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
    cache = mock(GemFireCacheImpl.class);
    DistributedSystem ds = mock(DistributedSystem.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);

    MeterRegistry mr = mock(MeterRegistry.class);
    StatisticsManager sm = mock(StatisticsManager.class);
    ClusterDistributionManager dm = mock(ClusterDistributionManager.class);

    when(cache.getDistributedSystem()).thenReturn(ds);
    when(cache.getInternalDistributedSystem()).thenReturn(ids);
    when(cache.getDistributionManager()).thenReturn(dm);

    when(cache.getCachePerfStats()).thenReturn(mock(CachePerfStats.class));
    when(cache.getMeterRegistry()).thenReturn(mr);
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));


    cache.getCancelCriterion().checkCancelInProgress(null);

    when(ds.createAtomicStatistics(any(), anyString())).thenReturn(mock(Statistics.class));
    when(ids.getStatisticsManager()).thenReturn(sm);
    when(ids.getClock()).thenReturn(mock(DSClock.class));
    when(ids.getDistributionManager()).thenReturn(dm);
    when(ids.getDistributedMember()).thenReturn(mock(InternalDistributedMember.class));

    when(mr.timer(any(), any(), any())).thenReturn(mock(Timer.class));
    when(mr.gauge(anyString(), any(), any(ToDoubleFunction.class))).thenReturn(mock(Gauge.class));
    when(mr.config()).thenReturn(mock(MeterRegistry.Config.class));

    when(sm.createAtomicStatistics(any(), anyString())).thenReturn(mock(Statistics.class));

    when(dm.getConfig()).thenReturn(mock(DistributionConfig.class));
    when(dm.getCache()).thenReturn(cache);

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

    assertThat(bucketRegionQueue.isInitialized()).isFalse();

    // Create a real ConcurrentParallelGatewaySenderQueue
    ParallelGatewaySenderEventProcessor processor =
        ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(sender);
    GatewaySenderEventImpl gsEvent = mock(GatewaySenderEventImpl.class);

    // Add a mock GatewaySenderEventImpl to the temp queue
    BlockingQueue<GatewaySenderEventImpl> tempQueue =
        createTempQueueAndAddEvent(processor, gsEvent);
    assertThat(tempQueue.size()).isEqualTo(1);

    createAndProcessParallelQueueSetPossibleDuplicateMessage();

    verify(sender, times(1)).markAsDuplicateInTempQueueEvents(KEY);
    verify(bucketRegionQueue, times(0)).setAsPossibleDuplicate(KEY);

  }

  @Test
  public void validateSetPossibleDuplicateKeyInInitializedBucketRegionQueue() throws Exception {

    assertThat(bucketRegionQueue.isInitialized()).isFalse();

    // Create a real ConcurrentParallelGatewaySenderQueue
    ParallelGatewaySenderEventProcessor processor =
        ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(sender);
    GatewaySenderEventImpl gsEvent = mock(GatewaySenderEventImpl.class);

    // Add a mock GatewaySenderEventImpl to the temp queue
    BlockingQueue<GatewaySenderEventImpl> tempQueue =
        createTempQueueAndAddEvent(processor, gsEvent);
    assertThat(tempQueue.size()).isEqualTo(1);

    // Clean up destroyed tokens
    bucketRegionQueueHelper.cleanUpDestroyedTokensAndMarkGIIComplete();

    assertThat(bucketRegionQueue.isInitialized()).isTrue();

    bucketRegionQueue.pushKeyIntoQueue(KEY);

    createAndProcessParallelQueueSetPossibleDuplicateMessage();

    verify(sender, times(1)).markAsDuplicateInTempQueueEvents(KEY);
    verify(bucketRegionQueue, times(1)).setAsPossibleDuplicate(KEY);

  }

  private void createAndProcessParallelQueueSetPossibleDuplicateMessage() {
    ParallelQueueSetPossibleDuplicateMessage message =
        new ParallelQueueSetPossibleDuplicateMessage(UNSUCCESSFULLY_DISPATCHED,
            createRegionToDispatchedKeysMap());
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
