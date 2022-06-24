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
package org.apache.geode.cache.wan.internal.txgrouping.parallel;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegionQueue;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;

public class TxGroupingParallelGatewaySenderQueueJUnitTest {

  private AbstractGatewaySender sender;

  @Before
  public void createParallelGatewaySenderQueue() {
    GemFireCacheImpl cache = mock(GemFireCacheImpl.class);
    sender = mock(AbstractGatewaySender.class);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(sender.getCancelCriterion()).thenReturn(cancelCriterion);
    when(sender.getCache()).thenReturn(cache);
    when(sender.getMaximumQueueMemory()).thenReturn(100);
    when(sender.getLifeCycleLock()).thenReturn(new ReentrantReadWriteLock());
    when(sender.getId()).thenReturn("");
  }

  private void mockGatewaySenderStats() {
    GatewaySenderStats stats = mock(GatewaySenderStats.class);
    when(sender.getStatistics()).thenReturn(stats);
  }

  @Test
  public void peekGetsExtraEventsWhenMustGroupTransactionEventsAndNotAllEventsForTransactionsInMaxSizeBatch()
      throws Exception {

    GatewaySenderEventImpl event1 = createGatewaySenderEventImpl(1, false);
    GatewaySenderEventImpl event2 = createGatewaySenderEventImpl(2, false);
    GatewaySenderEventImpl event3 = createGatewaySenderEventImpl(1, true);
    GatewaySenderEventImpl event4 = createGatewaySenderEventImpl(2, true);
    GatewaySenderEventImpl event5 = createGatewaySenderEventImpl(3, false);
    GatewaySenderEventImpl event6 = createGatewaySenderEventImpl(3, true);

    Queue<GatewaySenderEventImpl> backingList = new LinkedList<>();
    backingList.add(event1);
    backingList.add(event2);
    backingList.add(event3);
    backingList.add(event4);
    backingList.add(event5);
    backingList.add(event6);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    List<?> peeked = queue.peek(3, 100);
    assertEquals(4, peeked.size());
    List<?> peekedAfter = queue.peek(3, 100);
    assertEquals(2, peekedAfter.size());
  }

  @Test
  public void peekGetsExtraEventsWhenMustGroupTransactionEventsAndNotAllEventsForTransactionsInBatchByTime()
      throws Exception {

    GatewaySenderEventImpl event1 = createGatewaySenderEventImpl(1, false);
    GatewaySenderEventImpl event2 = createGatewaySenderEventImpl(2, false);
    GatewaySenderEventImpl event3 = createGatewaySenderEventImpl(1, true);
    GatewaySenderEventImpl event4 = createGatewaySenderEventImpl(2, true);
    GatewaySenderEventImpl event5 = createGatewaySenderEventImpl(3, false);
    GatewaySenderEventImpl event6 = createGatewaySenderEventImpl(3, true);

    Queue<GatewaySenderEventImpl> backingList = new LinkedList<>();
    backingList.add(event1);
    backingList.add(event2);
    backingList.add(event3);
    backingList.add(null);
    backingList.add(event4);
    backingList.add(event5);
    backingList.add(event6);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    List<?> peeked = queue.peek(-1, 1);
    assertEquals(4, peeked.size());
    List<?> peekedAfter = queue.peek(-1, 100);
    assertEquals(2, peekedAfter.size());
  }

  @Test
  public void peekEventsFromIncompleteTransactionsDoesNotThrowConcurrentModificationExceptionWhenCompletingTwoTransactions() {
    mockGatewaySenderStats();
    GatewaySenderEventImpl event1 = createGatewaySenderEventImpl(1, false);
    GatewaySenderEventImpl event2 = createGatewaySenderEventImpl(2, false);
    GatewaySenderEventImpl event3 = createGatewaySenderEventImpl(1, true);
    GatewaySenderEventImpl event4 = createGatewaySenderEventImpl(2, true);

    Queue<GatewaySenderEventImpl> backingList = new LinkedList<>();
    backingList.add(event3);
    backingList.add(event4);
    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    List<GatewaySenderEventImpl> batch = new ArrayList<>(Arrays.asList(event1, event2));
    PartitionedRegion mockBucketRegion = mockPR("bucketRegion");
    queue.postProcessBatch(mockBucketRegion, batch);
  }


  private GatewaySenderEventImpl createGatewaySenderEventImpl(int transactionId,
      boolean isLastEventInTransaction) {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.getTransactionId()).thenReturn(new TXId(null, transactionId));
    when(event.makeHeapCopyIfOffHeap()).thenReturn(event);
    when(event.isLastEventInTransaction()).thenReturn(isLastEventInTransaction);
    return event;
  }

  private PartitionedRegion mockPR(String name) {
    PartitionedRegion region = mock(PartitionedRegion.class);
    when(region.getFullPath()).thenReturn(name);
    when(region.getPartitionAttributes()).thenReturn(new PartitionAttributesFactory<>().create());
    when(region.getTotalNumberOfBuckets()).thenReturn(113);
    when(region.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    return region;
  }

  private BucketRegionQueue mockBucketRegionQueue(final Queue<GatewaySenderEventImpl> backingList) {
    PartitionedRegion mockBucketRegion = mockPR("bucketRegion");
    // These next mocked return calls are for when peek is called. It ends up checking these on the
    // mocked pr region
    when(mockBucketRegion.getLocalMaxMemory()).thenReturn(100);
    when(mockBucketRegion.size()).thenReturn(backingList.size());
    BucketRegionQueue bucketRegionQueue = mock(BucketRegionQueue.class);
    when(bucketRegionQueue.getPartitionedRegion()).thenReturn(mockBucketRegion);
    when(bucketRegionQueue.peek()).thenAnswer(invocation -> pollAndWaitIfNull(backingList));
    when(bucketRegionQueue.getElementsMatching(any(), any()))
        .thenAnswer(invocation -> singletonList(getFirstNotNull(backingList)));
    return bucketRegionQueue;
  }

  private Object pollAndWaitIfNull(Queue<GatewaySenderEventImpl> queue) {
    Object object = queue.poll();
    if (object == null) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return object;
  }

  private Object getFirstNotNull(Queue<GatewaySenderEventImpl> queue) {
    Object object = queue.poll();
    while (object == null) {
      object = queue.poll();
    }
    return object;
  }

  private static class TestableParallelGatewaySenderQueue
      extends TxGroupingParallelGatewaySenderQueue {

    private BucketRegionQueue mockedAbstractBucketRegionQueue;

    public TestableParallelGatewaySenderQueue(final AbstractGatewaySender sender,
        final Set<Region<?, ?>> userRegions, final int idx, final int nDispatcher) {
      super(sender, userRegions, idx, nDispatcher, false);
    }

    public void setMockedAbstractBucketRegionQueue(BucketRegionQueue mocked) {
      mockedAbstractBucketRegionQueue = mocked;
    }

    @Override
    public boolean areLocalBucketQueueRegionsPresent() {
      return true;
    }

    @Override
    protected PartitionedRegion getRandomShadowPR() {
      return mockedAbstractBucketRegionQueue.getPartitionedRegion();
    }

    @Override
    protected int getRandomPrimaryBucket(PartitionedRegion pr) {
      return 0;
    }

    @Override
    protected BucketRegionQueue getBucketRegionQueueByBucketId(PartitionedRegion prQ,
        int bucketId) {
      return mockedAbstractBucketRegionQueue;
    }
  }

}
