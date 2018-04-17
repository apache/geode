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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketRegionQueue;
import org.apache.geode.internal.cache.BucketRegionQueueHelper;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EvictionAttributesImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.internal.cache.ProxyBucketRegion;
import org.apache.geode.internal.cache.eviction.AbstractEvictionController;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.statistics.DummyStatisticsFactory;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ParallelQueueRemovalMessageJUnitTest {

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
    this.cache = Fakes.cache();
  }

  private void createQueueRegion() {
    // Mock queue region
    this.queueRegion = mock(PartitionedRegion.class);
    when(this.queueRegion.getFullPath()).thenReturn(getRegionQueueName());
    when(this.queueRegion.getPrStats()).thenReturn(mock(PartitionedRegionStats.class));
    when(this.queueRegion.getDataStore()).thenReturn(mock(PartitionedRegionDataStore.class));
    when(this.queueRegion.getCache()).thenReturn(this.cache);
    EvictionAttributesImpl ea = (EvictionAttributesImpl) EvictionAttributes
        .createLRUMemoryAttributes(100, null, EvictionAction.OVERFLOW_TO_DISK);
    EvictionController eviction = AbstractEvictionController.create(ea, false,
        this.cache.getDistributedSystem(), "queueRegion");
    when(this.queueRegion.getEvictionController()).thenReturn(eviction);
  }

  private void createGatewaySender() {
    // Mock gateway sender
    this.sender = ParallelGatewaySenderHelper.createGatewaySender(this.cache);
    when(this.queueRegion.getParallelGatewaySender()).thenReturn(this.sender);
    when(this.sender.getQueues()).thenReturn(null);
    when(this.sender.getDispatcherThreads()).thenReturn(1);
    stats = new GatewaySenderStats(new DummyStatisticsFactory(), "ln");
    when(this.sender.getStatistics()).thenReturn(stats);
  }

  private void createRootRegion() {
    // Mock root region
    this.rootRegion = mock(PartitionedRegion.class);
    when(this.rootRegion.getFullPath())
        .thenReturn(Region.SEPARATOR + PartitionedRegionHelper.PR_ROOT_REGION_NAME);
    when(this.cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME, true))
        .thenReturn(this.rootRegion);
    when(this.cache.getRegion(getRegionQueueName())).thenReturn(this.queueRegion);
  }

  private void createBucketRegionQueue() {
    // Create InternalRegionArguments
    InternalRegionArguments ira = new InternalRegionArguments();
    ira.setPartitionedRegion(this.queueRegion);
    ira.setPartitionedRegionBucketRedundancy(1);
    BucketAdvisor ba = mock(BucketAdvisor.class);
    ira.setBucketAdvisor(ba);
    InternalRegionArguments pbrIra = new InternalRegionArguments();
    RegionAdvisor ra = mock(RegionAdvisor.class);
    when(ra.getPartitionedRegion()).thenReturn(this.queueRegion);
    pbrIra.setPartitionedRegionAdvisor(ra);
    PartitionAttributes pa = mock(PartitionAttributes.class);
    when(this.queueRegion.getPartitionAttributes()).thenReturn(pa);

    when(this.queueRegion.getBucketName(eq(BUCKET_ID))).thenAnswer(new Answer<String>() {
      @Override
      public String answer(final InvocationOnMock invocation) throws Throwable {
        return PartitionedRegionHelper.getBucketName(queueRegion.getFullPath(), BUCKET_ID);
      }
    });

    when(this.queueRegion.getDataPolicy()).thenReturn(DataPolicy.PARTITION);

    when(pa.getColocatedWith()).thenReturn(null);

    when(ba.getProxyBucketRegion()).thenReturn(mock(ProxyBucketRegion.class));

    // Create RegionAttributes
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setEvictionAttributes(
        EvictionAttributes.createLRUMemoryAttributes(100, null, EvictionAction.OVERFLOW_TO_DISK));
    RegionAttributes attributes = factory.create();

    // Create BucketRegionQueue
    BucketRegionQueue realBucketRegionQueue = new BucketRegionQueue(
        this.queueRegion.getBucketName(BUCKET_ID), attributes, this.rootRegion, this.cache, ira);
    this.bucketRegionQueue = spy(realBucketRegionQueue);
    // (this.queueRegion.getBucketName(BUCKET_ID), attributes, this.rootRegion, this.cache, ira);
    EntryEventImpl entryEvent = EntryEventImpl.create(this.bucketRegionQueue, Operation.DESTROY,
        KEY, "value", null, false, mock(DistributedMember.class));
    doReturn(entryEvent).when(this.bucketRegionQueue).newDestroyEntryEvent(any(), any());
    // when(this.bucketRegionQueue.newDestroyEntryEvent(any(), any())).thenReturn();

    this.bucketRegionQueueHelper =
        new BucketRegionQueueHelper(this.cache, this.queueRegion, this.bucketRegionQueue);
  }

  @Test
  public void validateFailedBatchRemovalMessageKeysInUninitializedBucketRegionQueue()
      throws Exception {
    // Validate initial BucketRegionQueue state
    assertFalse(this.bucketRegionQueue.isInitialized());
    assertEquals(0, this.bucketRegionQueue.getFailedBatchRemovalMessageKeys().size());
    stats.setSecondaryQueueSize(1);

    // Create and process a ParallelQueueRemovalMessage (causes the failedBatchRemovalMessageKeys to
    // add a key)
    createAndProcessParallelQueueRemovalMessage();

    // Validate BucketRegionQueue after processing ParallelQueueRemovalMessage
    assertEquals(1, this.bucketRegionQueue.getFailedBatchRemovalMessageKeys().size());
    // failed BatchRemovalMessage will not modify stats
    assertEquals(1, stats.getEventSecondaryQueueSize());
  }

  @Test
  public void validateDestroyKeyFromBucketQueueInUninitializedBucketRegionQueue() throws Exception {
    // Validate initial BucketRegionQueue state
    assertEquals(0, this.bucketRegionQueue.size());
    assertFalse(this.bucketRegionQueue.isInitialized());

    // Add an event to the BucketRegionQueue and verify BucketRegionQueue state
    this.bucketRegionQueueHelper.addEvent(KEY);
    assertEquals(1, this.bucketRegionQueue.size());
    assertEquals(1, stats.getEventSecondaryQueueSize());

    // Create and process a ParallelQueueRemovalMessage (causes the value of the entry to be set to
    // DESTROYED)
    when(this.queueRegion.getKeyInfo(KEY, null, null)).thenReturn(new KeyInfo(KEY, null, null));
    createAndProcessParallelQueueRemovalMessage();

    // Clean up destroyed tokens and validate BucketRegionQueue
    this.bucketRegionQueueHelper.cleanUpDestroyedTokensAndMarkGIIComplete();
    assertEquals(0, this.bucketRegionQueue.size());
    assertEquals(0, stats.getEventSecondaryQueueSize());
  }

  @Test
  public void validateDestroyFromTempQueueInUninitializedBucketRegionQueue() throws Exception {
    // Validate initial BucketRegionQueue state
    assertFalse(this.bucketRegionQueue.isInitialized());

    // Create a real ConcurrentParallelGatewaySenderQueue
    ParallelGatewaySenderEventProcessor processor =
        ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(this.sender);

    // Add a mock GatewaySenderEventImpl to the temp queue
    BlockingQueue<GatewaySenderEventImpl> tempQueue =
        createTempQueueAndAddEvent(processor, mock(GatewaySenderEventImpl.class));
    assertEquals(1, tempQueue.size());

    // Create and process a ParallelQueueRemovalMessage (causes the failedBatchRemovalMessageKeys to
    // add a key)
    createAndProcessParallelQueueRemovalMessage();

    // Validate temp queue is empty after processing ParallelQueueRemovalMessage
    assertEquals(0, tempQueue.size());
  }

  @Test
  public void validateDestroyFromBucketQueueAndTempQueueInUninitializedBucketRegionQueue() {
    // Validate initial BucketRegionQueue state
    assertFalse(this.bucketRegionQueue.isInitialized());
    assertEquals(0, this.bucketRegionQueue.size());

    // Create a real ConcurrentParallelGatewaySenderQueue
    ParallelGatewaySenderEventProcessor processor =
        ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(this.sender);

    // Add an event to the BucketRegionQueue and verify BucketRegionQueue state
    GatewaySenderEventImpl event = this.bucketRegionQueueHelper.addEvent(KEY);
    assertEquals(1, this.bucketRegionQueue.size());
    assertEquals(1, stats.getEventSecondaryQueueSize());

    // Add a mock GatewaySenderEventImpl to the temp queue
    BlockingQueue<GatewaySenderEventImpl> tempQueue = createTempQueueAndAddEvent(processor, event);
    assertEquals(1, tempQueue.size());

    // Create and process a ParallelQueueRemovalMessage (causes the value of the entry to be set to
    // DESTROYED)
    when(this.queueRegion.getKeyInfo(KEY, null, null)).thenReturn(new KeyInfo(KEY, null, null));
    createAndProcessParallelQueueRemovalMessage();

    // Validate temp queue is empty after processing ParallelQueueRemovalMessage
    assertEquals(0, tempQueue.size());
    assertEquals(0, stats.getEventSecondaryQueueSize());

    // Clean up destroyed tokens
    this.bucketRegionQueueHelper.cleanUpDestroyedTokensAndMarkGIIComplete();

    // Validate BucketRegionQueue is empty after processing ParallelQueueRemovalMessage
    assertEquals(0, this.bucketRegionQueue.size());
  }

  private void createAndProcessParallelQueueRemovalMessage() {
    ParallelQueueRemovalMessage message =
        new ParallelQueueRemovalMessage(createRegionToDispatchedKeysMap());
    message.process((ClusterDistributionManager) this.cache.getDistributionManager());
  }

  private HashMap<String, Map<Integer, List<Long>>> createRegionToDispatchedKeysMap() {
    HashMap<String, Map<Integer, List<Long>>> regionToDispatchedKeys = new HashMap<>();
    Map<Integer, List<Long>> bucketIdToDispatchedKeys = new HashMap<>();
    List<Long> dispatchedKeys = new ArrayList<>();
    dispatchedKeys.add(KEY);
    bucketIdToDispatchedKeys.put(BUCKET_ID, dispatchedKeys);
    regionToDispatchedKeys.put(getRegionQueueName(), bucketIdToDispatchedKeys);
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

  private String getRegionQueueName() {
    return Region.SEPARATOR + GATEWAY_SENDER_ID + ParallelGatewaySenderQueue.QSTRING;
  }
}
