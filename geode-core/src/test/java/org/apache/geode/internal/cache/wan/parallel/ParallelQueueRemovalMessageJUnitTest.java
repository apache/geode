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

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.*;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.lru.LRUAlgorithm;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class ParallelQueueRemovalMessageJUnitTest {

  private GemFireCacheImpl cache;
  private PartitionedRegion queueRegion;
  private AbstractGatewaySender sender;
  private PartitionedRegion rootRegion;
  private BucketRegionQueue bucketRegionQueue;
  private BucketRegionQueueHelper bucketRegionQueueHelper;

  private static String GATEWAY_SENDER_ID = "ny";
  private static int BUCKET_ID = 85;
  private static long KEY = 198l;

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
    GemFireCacheImpl.setInstanceForTests(this.cache);
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
    LRUAlgorithm algorithm = ea.createEvictionController(this.queueRegion, false);
    algorithm.getLRUHelper().initStats(this.queueRegion, this.cache.getDistributedSystem());
    when(this.queueRegion.getEvictionController()).thenReturn(algorithm);
  }

  private void createGatewaySender() {
    // Mock gateway sender
    this.sender = mock(AbstractGatewaySender.class);
    when(this.queueRegion.getParallelGatewaySender()).thenReturn(this.sender);
    when(this.sender.getQueues()).thenReturn(null);
    when(this.sender.getDispatcherThreads()).thenReturn(1);
    when(this.sender.getCache()).thenReturn(this.cache);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(sender.getCancelCriterion()).thenReturn(cancelCriterion);
  }

  private void createRootRegion() {
    // Mock root region
    this.rootRegion = mock(PartitionedRegion.class);
    when(this.rootRegion.getFullPath())
        .thenReturn(Region.SEPARATOR + PartitionedRegionHelper.PR_ROOT_REGION_NAME);
    when(this.cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME, true))
        .thenReturn(this.rootRegion);
    when(this.cache.getRegion(getRegionQueueName(), false)).thenReturn(this.queueRegion);
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
    when(this.queueRegion.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    when(pa.getColocatedWith()).thenReturn(null);
    ProxyBucketRegion pbr = new ProxyBucketRegion(BUCKET_ID, this.queueRegion, pbrIra); // final
                                                                                        // classes
                                                                                        // cannot be
                                                                                        // mocked
    when(ba.getProxyBucketRegion()).thenReturn(pbr);

    // Create RegionAttributes
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setEvictionAttributes(
        EvictionAttributes.createLRUMemoryAttributes(100, null, EvictionAction.OVERFLOW_TO_DISK));
    RegionAttributes attributes = factory.create();

    // Create BucketRegionQueue
    this.bucketRegionQueue = new BucketRegionQueue(this.queueRegion.getBucketName(BUCKET_ID),
        attributes, this.rootRegion, this.cache, ira);
    this.bucketRegionQueueHelper =
        new BucketRegionQueueHelper(this.cache, this.queueRegion, this.bucketRegionQueue);
  }

  @After
  public void tearDownGemFire() {
    GemFireCacheImpl.setInstanceForTests(null);
  }

  @Test
  public void validateFailedBatchRemovalMessageKeysInUninitializedBucketRegionQueue()
      throws Exception {
    // Validate initial BucketRegionQueue state
    assertFalse(this.bucketRegionQueue.isInitialized());
    assertEquals(0, this.bucketRegionQueue.getFailedBatchRemovalMessageKeys().size());

    // Create and process a ParallelQueueRemovalMessage (causes the failedBatchRemovalMessageKeys to
    // add a key)
    createAndProcessParallelQueueRemovalMessage();

    // Validate BucketRegionQueue after processing ParallelQueueRemovalMessage
    assertEquals(1, this.bucketRegionQueue.getFailedBatchRemovalMessageKeys().size());
  }

  @Test
  public void validateDestroyKeyFromBucketQueueInUninitializedBucketRegionQueue() throws Exception {
    // Validate initial BucketRegionQueue state
    assertEquals(0, this.bucketRegionQueue.size());
    assertFalse(this.bucketRegionQueue.isInitialized());

    // Add an event to the BucketRegionQueue and verify BucketRegionQueue state
    this.bucketRegionQueueHelper.addEvent(KEY);
    assertEquals(1, this.bucketRegionQueue.size());

    // Create and process a ParallelQueueRemovalMessage (causes the value of the entry to be set to
    // DESTROYED)
    when(this.queueRegion.getKeyInfo(KEY, null, null)).thenReturn(new KeyInfo(KEY, null, null));
    createAndProcessParallelQueueRemovalMessage();

    // Clean up destroyed tokens and validate BucketRegionQueue
    this.bucketRegionQueueHelper.cleanUpDestroyedTokensAndMarkGIIComplete();
    assertEquals(0, this.bucketRegionQueue.size());
  }

  @Test
  public void validateDestroyFromTempQueueInUninitializedBucketRegionQueue() throws Exception {
    // Validate initial BucketRegionQueue state
    assertFalse(this.bucketRegionQueue.isInitialized());

    // Create a real ConcurrentParallelGatewaySenderQueue
    ParallelGatewaySenderEventProcessor pgsep = createConcurrentParallelGatewaySenderQueue();

    // Add a mock GatewaySenderEventImpl to the temp queue
    BlockingQueue<GatewaySenderEventImpl> tempQueue =
        createTempQueueAndAddEvent(pgsep, mock(GatewaySenderEventImpl.class));
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
    ParallelGatewaySenderEventProcessor pgsep = createConcurrentParallelGatewaySenderQueue();

    // Add an event to the BucketRegionQueue and verify BucketRegionQueue state
    GatewaySenderEventImpl gsei = this.bucketRegionQueueHelper.addEvent(KEY);
    assertEquals(1, this.bucketRegionQueue.size());

    // Add a mock GatewaySenderEventImpl to the temp queue
    BlockingQueue<GatewaySenderEventImpl> tempQueue = createTempQueueAndAddEvent(pgsep, gsei);
    assertEquals(1, tempQueue.size());

    // Create and process a ParallelQueueRemovalMessage (causes the value of the entry to be set to
    // DESTROYED)
    when(this.queueRegion.getKeyInfo(KEY, null, null)).thenReturn(new KeyInfo(KEY, null, null));
    createAndProcessParallelQueueRemovalMessage();

    // Validate temp queue is empty after processing ParallelQueueRemovalMessage
    assertEquals(0, tempQueue.size());

    // Clean up destroyed tokens
    this.bucketRegionQueueHelper.cleanUpDestroyedTokensAndMarkGIIComplete();

    // Validate BucketRegionQueue is empty after processing ParallelQueueRemovalMessage
    assertEquals(0, this.bucketRegionQueue.size());
  }

  private void createAndProcessParallelQueueRemovalMessage() {
    ParallelQueueRemovalMessage pqrm =
        new ParallelQueueRemovalMessage(createRegionToDispatchedKeysMap());
    pqrm.process(null);
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

  private ParallelGatewaySenderEventProcessor createConcurrentParallelGatewaySenderQueue() {
    ParallelGatewaySenderEventProcessor pgsep = new ParallelGatewaySenderEventProcessor(sender);
    ConcurrentParallelGatewaySenderQueue cpgsq = new ConcurrentParallelGatewaySenderQueue(sender,
        new ParallelGatewaySenderEventProcessor[] {pgsep});
    Set<RegionQueue> queues = new HashSet<>();
    queues.add(cpgsq);
    when(this.sender.getQueues()).thenReturn(queues);
    return pgsep;
  }

  private BlockingQueue<GatewaySenderEventImpl> createTempQueueAndAddEvent(
      ParallelGatewaySenderEventProcessor pgsep, GatewaySenderEventImpl gsei) {
    ParallelGatewaySenderQueue pgsq = (ParallelGatewaySenderQueue) pgsep.getQueue();
    Map<Integer, BlockingQueue<GatewaySenderEventImpl>> tempQueueMap =
        pgsq.getBucketToTempQueueMap();
    BlockingQueue<GatewaySenderEventImpl> tempQueue = new LinkedBlockingQueue();
    when(gsei.getShadowKey()).thenReturn(KEY);
    tempQueue.add(gsei);
    tempQueueMap.put(BUCKET_ID, tempQueue);
    return tempQueue;
  }

  private String getRegionQueueName() {
    return Region.SEPARATOR + GATEWAY_SENDER_ID + ParallelGatewaySenderQueue.QSTRING;
  }
}
