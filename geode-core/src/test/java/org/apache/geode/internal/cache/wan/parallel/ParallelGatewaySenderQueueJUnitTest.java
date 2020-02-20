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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.AbstractBucketRegionQueue;
import org.apache.geode.internal.cache.BucketRegionQueue;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
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
    metaRegionFactory = mock(MetaRegionFactory.class);
    queue = new ParallelGatewaySenderQueue(sender, Collections.emptySet(), 0, 1, metaRegionFactory);
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

    List peeked = queue.peek(1, 1000);
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

    List peeked = queue.peek(1, 1000);
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

    List peeked = queue.peek(1, 1000);
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
    when(bucketRegionQueue.peek()).thenAnswer((Answer) invocation -> backingList.poll());
    return bucketRegionQueue;
  }



  private class TestableParallelGatewaySenderQueue extends ParallelGatewaySenderQueue {

    private BucketRegionQueue mockedAbstractBucketRegionQueue;

    public TestableParallelGatewaySenderQueue(final AbstractGatewaySender sender,
        final Set<Region> userRegions, final int idx, final int nDispatcher) {
      super(sender, userRegions, idx, nDispatcher);
    }

    public TestableParallelGatewaySenderQueue(final AbstractGatewaySender sender,
        final Set<Region> userRegions, final int idx, final int nDispatcher,
        final MetaRegionFactory metaRegionFactory) {
      super(sender, userRegions, idx, nDispatcher, metaRegionFactory);
    }


    public void setMockedAbstractBucketRegionQueue(BucketRegionQueue mocked) {
      this.mockedAbstractBucketRegionQueue = mocked;
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

    // @Override
    // public int localSizeForProcessor() {
    // return 1;
    // }
  }

}
