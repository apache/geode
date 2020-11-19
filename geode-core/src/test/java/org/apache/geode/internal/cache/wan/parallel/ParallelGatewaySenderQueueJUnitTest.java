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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.AbstractBucketRegionQueue;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketRegionQueue;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue.MetaRegionFactory;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue.ParallelGatewaySenderQueueMetaRegion;

public class ParallelGatewaySenderQueueJUnitTest {

  private ParallelGatewaySenderQueue queue;
  private MetaRegionFactory metaRegionFactory;
  private GemFireCacheImpl cache;
  private AbstractGatewaySender sender;

  @Before
  public void createParallelGatewaySenderQueue() {
    cache = mock(GemFireCacheImpl.class);
    sender = mock(AbstractGatewaySender.class);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(sender.getCancelCriterion()).thenReturn(cancelCriterion);
    when(sender.getCache()).thenReturn(cache);
    when(sender.getMaximumQueueMemory()).thenReturn(100);
    when(sender.getLifeCycleLock()).thenReturn(new ReentrantReadWriteLock());
    when(sender.getId()).thenReturn("");
    metaRegionFactory = mock(MetaRegionFactory.class);
    queue = new ParallelGatewaySenderQueue(sender, Collections.emptySet(), 0, 1, metaRegionFactory,
        false);
  }

  @Test
  public void whenReplicatedDataRegionNotReadyShouldNotThrowException() throws Exception {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.makeHeapCopyIfOffHeap()).thenReturn(event);
    when(event.getRegion()).thenReturn(null);
    String regionPath = "/testRegion";
    when(event.getRegionPath()).thenReturn(regionPath);
    Mockito.doThrow(new IllegalStateException()).when(event).release();
    Queue backingList = new LinkedList();
    backingList.add(event);

    queue = spy(queue);
    doReturn(true).when(queue).isDREvent(any(), any());
    boolean putDone = queue.put(event);
    assertThat(putDone).isFalse();
  }

  @Test
  public void whenPartitionedDataRegionNotReadyShouldNotThrowException() throws Exception {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.makeHeapCopyIfOffHeap()).thenReturn(event);
    when(event.getRegion()).thenReturn(null);
    String regionPath = "/testRegion";
    when(event.getRegionPath()).thenReturn(regionPath);
    PartitionedRegion region = mock(PartitionedRegion.class);
    when(region.getFullPath()).thenReturn(regionPath);
    when(cache.getRegion(regionPath, true)).thenReturn(region);
    PartitionAttributes pa = mock(PartitionAttributes.class);
    when(region.getPartitionAttributes()).thenReturn(pa);
    when(pa.getColocatedWith()).thenReturn(null);

    Mockito.doThrow(new IllegalStateException()).when(event).release();
    Queue backingList = new LinkedList();
    backingList.add(event);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    queue = spy(queue);
    boolean putDone = queue.put(event);
    assertThat(putDone).isFalse();
  }

  private void testEnqueueToBrqAfterLockFailedInitialImageReadLock(boolean isTmpQueue)
      throws InterruptedException {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    String regionPath = "/userPR";
    when(event.getRegionPath()).thenReturn(regionPath);
    when(event.makeHeapCopyIfOffHeap()).thenReturn(event);
    when(event.getRegion()).thenReturn(null);
    when(event.getBucketId()).thenReturn(1);
    when(event.getShadowKey()).thenReturn(100L);
    when(sender.isPersistenceEnabled()).thenReturn(true);
    PartitionedRegionDataStore prds = mock(PartitionedRegionDataStore.class);
    PartitionedRegion prQ = mock(PartitionedRegion.class);
    AbstractBucketRegionQueue brq = mock(AbstractBucketRegionQueue.class);
    ReentrantReadWriteLock initializationLock = mock(ReentrantReadWriteLock.class);
    ReentrantReadWriteLock.ReadLock readLock = mock(ReentrantReadWriteLock.ReadLock.class);
    when(initializationLock.readLock()).thenReturn(readLock);
    doNothing().when(readLock).lock();
    doNothing().when(readLock).unlock();
    doNothing().when(brq).unlockWhenRegionIsInitializing();
    when(brq.getInitializationLock()).thenReturn(initializationLock);
    when(brq.lockWhenRegionIsInitializing()).thenReturn(true);
    when(prQ.getDataStore()).thenReturn(prds);
    when(prQ.getCache()).thenReturn(cache);
    when(prQ.getBucketName(1)).thenReturn("_B__PARALLEL_GATEWAY_SENDER_QUEUE_1");
    when(prds.getLocalBucketById(1)).thenReturn(null);
    PartitionedRegion userPR = mock(PartitionedRegion.class);
    PartitionAttributes pa = mock(PartitionAttributes.class);
    when(userPR.getPartitionAttributes()).thenReturn(pa);
    when(pa.getColocatedWith()).thenReturn(null);
    when(userPR.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_PARTITION);
    when(userPR.getFullPath()).thenReturn(regionPath);
    when(cache.getRegion("_PARALLEL_GATEWAY_SENDER_QUEUE")).thenReturn(prQ);
    when(cache.getRegion(regionPath, true)).thenReturn(userPR);
    when(prQ.getColocatedWithRegion()).thenReturn(userPR);
    RegionAdvisor ra = mock(RegionAdvisor.class);
    BucketAdvisor ba = mock(BucketAdvisor.class);
    when(userPR.getRegionAdvisor()).thenReturn(ra);
    when(ra.getBucketAdvisor(1)).thenReturn(ba);
    when(ba.isShadowBucketDestroyed("/__PR/_B__PARALLEL_GATEWAY_SENDER_QUEUE_1")).thenReturn(false);

    prepareBrq(brq, isTmpQueue);

    Mockito.doThrow(new IllegalStateException()).when(event).release();
    Queue backingList = new LinkedList();
    backingList.add(event);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    InOrder inOrder = inOrder(brq, readLock);
    queue = spy(queue);
    queue.addShadowPartitionedRegionForUserPR(userPR);
    doNothing().when(queue).putIntoBucketRegionQueue(eq(brq), any(), eq(event));
    boolean putDone = queue.put(event);
    assertThat(putDone).isTrue();
    inOrder.verify(brq).lockWhenRegionIsInitializing();
    inOrder.verify(readLock).lock();
    inOrder.verify(readLock).unlock();
    inOrder.verify(brq).unlockWhenRegionIsInitializing();
  }

  private void prepareBrq(AbstractBucketRegionQueue brq, boolean isTmpQueue) {
    if (isTmpQueue) {
      when(cache.getInternalRegionByPath("/__PR/_B__PARALLEL_GATEWAY_SENDER_QUEUE_1"))
          .thenReturn(null).thenReturn(brq);
    } else {
      when(cache.getInternalRegionByPath("/__PR/_B__PARALLEL_GATEWAY_SENDER_QUEUE_1"))
          .thenReturn(brq);
    }
  }

  @Test
  public void enqueueToInitializingBrqShouldLockFailedInitialImageReadLock() throws Exception {
    testEnqueueToBrqAfterLockFailedInitialImageReadLock(false);
  }

  @Test
  public void enqueueToTmpQueueShouldLockFailedInitialImageReadLock() throws Exception {
    testEnqueueToBrqAfterLockFailedInitialImageReadLock(true);
  }

  @Test
  public void whenEventReleaseFromOffHeapFailsExceptionShouldNotBeThrownToAckReaderThread()
      throws Exception {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.makeHeapCopyIfOffHeap()).thenReturn(event);
    Mockito.doThrow(new IllegalStateException()).when(event).release();
    Queue backingList = new LinkedList();
    backingList.add(event);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    List peeked = queue.peek(1, 100);
    assertEquals(1, peeked.size());
    queue.remove();
  }

  @Test
  public void whenGatewayEventUnableToResolveFromOffHeapTheStatForNotQueuedConflatedShouldBeIncremented()
      throws Exception {
    GatewaySenderStats stats = mockGatewaySenderStats();

    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.makeHeapCopyIfOffHeap()).thenReturn(null);
    GatewaySenderEventImpl eventResolvesFromOffHeap = mock(GatewaySenderEventImpl.class);
    when(eventResolvesFromOffHeap.makeHeapCopyIfOffHeap()).thenReturn(eventResolvesFromOffHeap);
    Queue backingList = new LinkedList();
    backingList.add(event);
    backingList.add(eventResolvesFromOffHeap);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    List peeked = queue.peek(1, 100);
    assertEquals(1, peeked.size());
    verify(stats, times(1)).incEventsNotQueuedConflated();
  }

  private GatewaySenderStats mockGatewaySenderStats() {
    GatewaySenderStats stats = mock(GatewaySenderStats.class);
    when(sender.getStatistics()).thenReturn(stats);
    return stats;
  }

  @Test
  public void whenNullPeekedEventFromBucketRegionQueueTheStatForNotQueuedConflatedShouldBeIncremented()
      throws Exception {
    GatewaySenderStats stats = mockGatewaySenderStats();

    GatewaySenderEventImpl eventResolvesFromOffHeap = mock(GatewaySenderEventImpl.class);
    when(eventResolvesFromOffHeap.makeHeapCopyIfOffHeap()).thenReturn(eventResolvesFromOffHeap);
    Queue backingList = new LinkedList();
    backingList.add(null);
    backingList.add(eventResolvesFromOffHeap);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    List peeked = queue.peek(1, 100);
    assertEquals(1, peeked.size());
    verify(stats, times(1)).incEventsNotQueuedConflated();
  }

  @Test
  public void testLocalSize() throws Exception {
    ParallelGatewaySenderQueueMetaRegion mockMetaRegion =
        mock(ParallelGatewaySenderQueueMetaRegion.class);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(mockMetaRegion.getDataStore()).thenReturn(dataStore);
    when(dataStore.getSizeOfLocalPrimaryBuckets()).thenReturn(3);
    when(metaRegionFactory.newMetataRegion(any(), any(), any(), any())).thenReturn(mockMetaRegion);
    InternalRegionFactory regionFactory = mock(InternalRegionFactory.class);
    when(regionFactory.create(any())).thenReturn(mockMetaRegion);
    when(cache.createInternalRegionFactory(any())).thenReturn(regionFactory);

    queue.addShadowPartitionedRegionForUserPR(mockPR("region1"));

    assertEquals(3, queue.localSize());
  }

  @Test
  public void isDREventReturnsTrueForDistributedRegionEvent() {
    String regionPath = "regionPath";
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.getRegionPath()).thenReturn(regionPath);
    DistributedRegion region = mock(DistributedRegion.class);
    when(cache.getRegion(regionPath)).thenReturn(region);
    ParallelGatewaySenderQueue queue = mock(ParallelGatewaySenderQueue.class);
    when(queue.isDREvent(cache, event)).thenCallRealMethod();

    assertThat(queue.isDREvent(cache, event)).isTrue();
  }

  @Test
  public void isDREventReturnsFalseForPartitionedRegionEvent() {
    String regionPath = "regionPath";
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.getRegionPath()).thenReturn(regionPath);
    PartitionedRegion region = mock(PartitionedRegion.class);
    when(cache.getRegion(regionPath)).thenReturn(region);
    ParallelGatewaySenderQueue queue = mock(ParallelGatewaySenderQueue.class);
    when(queue.isDREvent(cache, event)).thenCallRealMethod();

    assertThat(queue.isDREvent(cache, event)).isFalse();
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

    Queue backingList = new LinkedList();
    backingList.add(event1);
    backingList.add(event2);
    backingList.add(event3);
    backingList.add(event4);
    backingList.add(event5);
    backingList.add(event6);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setGroupTransactionEvents(true);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    List peeked = queue.peek(3, 100);
    assertEquals(4, peeked.size());
    List peekedAfter = queue.peek(3, 100);
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

    Queue backingList = new LinkedList();
    backingList.add(event1);
    backingList.add(event2);
    backingList.add(event3);
    backingList.add(null);
    backingList.add(event4);
    backingList.add(event5);
    backingList.add(event6);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setGroupTransactionEvents(true);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    List peeked = queue.peek(-1, 1);
    assertEquals(4, peeked.size());
    List peekedAfter = queue.peek(-1, 100);
    assertEquals(2, peekedAfter.size());
  }

  @Test
  public void peekDoesNotGetExtraEventsWhenNotMustGroupTransactionEventsAndNotAllEventsForTransactionsInBatchMaxSize()
      throws Exception {

    GatewaySenderEventImpl event1 = createGatewaySenderEventImpl(1, false);
    GatewaySenderEventImpl event2 = createGatewaySenderEventImpl(1, false);
    GatewaySenderEventImpl event3 = createGatewaySenderEventImpl(2, false);
    GatewaySenderEventImpl event4 = createGatewaySenderEventImpl(1, true);
    GatewaySenderEventImpl event5 = createGatewaySenderEventImpl(2, true);

    Queue backingList = new LinkedList();
    backingList.add(event1);
    backingList.add(event2);
    backingList.add(event3);
    backingList.add(event4);
    backingList.add(event5);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    List peeked = queue.peek(3, 100);
    assertEquals(3, peeked.size());
    List peekedAfter = queue.peek(3, 100);
    assertEquals(2, peekedAfter.size());
  }

  @Test
  public void peekDoesNotGetExtraEventsWhenMustGroupTransactionEventsAndNotAllEventsForTransactionsInBatchByTime()
      throws Exception {

    GatewaySenderEventImpl event1 = createGatewaySenderEventImpl(1, false);
    GatewaySenderEventImpl event2 = createGatewaySenderEventImpl(2, false);
    GatewaySenderEventImpl event3 = createGatewaySenderEventImpl(1, true);
    GatewaySenderEventImpl event4 = createGatewaySenderEventImpl(2, true);
    GatewaySenderEventImpl event5 = createGatewaySenderEventImpl(3, false);
    GatewaySenderEventImpl event6 = createGatewaySenderEventImpl(3, true);

    Queue backingList = new LinkedList();
    backingList.add(event1);
    backingList.add(event2);
    backingList.add(event3);
    backingList.add(null);
    backingList.add(event4);
    backingList.add(event5);
    backingList.add(event6);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setGroupTransactionEvents(false);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    List peeked = queue.peek(-1, 1);
    assertEquals(3, peeked.size());
    List peekedAfter = queue.peek(-1, 100);
    assertEquals(3, peekedAfter.size());
  }

  @Test
  public void testCalculateTimeToSleepNegativeInputReturnsZero() {
    assertEquals(0L, ParallelGatewaySenderQueue.calculateTimeToSleep(-3));
  }

  @Test
  public void testCalculateTimeToSleepZeroInputReturnsZero() {
    assertEquals(0L, ParallelGatewaySenderQueue.calculateTimeToSleep(0));
  }

  @Test
  public void testCalculateTimeToSleepInputGreaterThanOneThousand() {
    assertEquals(50L, ParallelGatewaySenderQueue.calculateTimeToSleep(1002));
  }

  @Test
  public void testCalculateTimeToSleepInputSmallerThanOneThousand() {
    assertEquals(2L, ParallelGatewaySenderQueue.calculateTimeToSleep(40));
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

  private BucketRegionQueue mockBucketRegionQueue(final Queue backingList) {
    PartitionedRegion mockBucketRegion = mockPR("bucketRegion");
    // These next mocked return calls are for when peek is called. It ends up checking these on the
    // mocked pr region
    when(mockBucketRegion.getLocalMaxMemory()).thenReturn(100);
    when(mockBucketRegion.size()).thenReturn(backingList.size());
    BucketRegionQueue bucketRegionQueue = mock(BucketRegionQueue.class);
    when(bucketRegionQueue.getPartitionedRegion()).thenReturn(mockBucketRegion);
    when(bucketRegionQueue.peek())
        .thenAnswer((Answer) invocation -> pollAndWaitIfNull(backingList));
    when(bucketRegionQueue.getElementsMatching(any(), any()))
        .thenAnswer((Answer) invocation -> Arrays
            .asList(new Object[] {getFirstNotNull(backingList)}));
    return bucketRegionQueue;
  }

  private Object pollAndWaitIfNull(Queue queue) {
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

  private Object getFirstNotNull(Queue queue) {
    Object object = queue.poll();
    while (object == null) {
      object = queue.poll();
    }
    return object;
  }

  private class TestableParallelGatewaySenderQueue extends ParallelGatewaySenderQueue {

    private BucketRegionQueue mockedAbstractBucketRegionQueue;
    private boolean groupTransactionEvents = false;

    public TestableParallelGatewaySenderQueue(final AbstractGatewaySender sender,
        final Set<Region> userRegions, final int idx, final int nDispatcher) {
      super(sender, userRegions, idx, nDispatcher, false);
    }

    public TestableParallelGatewaySenderQueue(final AbstractGatewaySender sender,
        final Set<Region> userRegions, final int idx, final int nDispatcher,
        final MetaRegionFactory metaRegionFactory) {
      super(sender, userRegions, idx, nDispatcher, metaRegionFactory, false);
    }


    public void setMockedAbstractBucketRegionQueue(BucketRegionQueue mocked) {
      this.mockedAbstractBucketRegionQueue = mocked;
    }

    public void setGroupTransactionEvents(boolean groupTransactionEvents) {
      this.groupTransactionEvents = groupTransactionEvents;
    }

    @Override
    public boolean mustGroupTransactionEvents() {
      return groupTransactionEvents;
    }

    public AbstractBucketRegionQueue getBucketRegion(final PartitionedRegion prQ,
        final int bucketId) {
      return mockedAbstractBucketRegionQueue;
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
