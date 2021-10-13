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
package org.apache.geode.internal.cache.wan.serial;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.statistics.DummyStatisticsRegistry;
import org.apache.geode.metrics.internal.NoopMeterRegistry;

public class SerialGatewaySenderQueueJUnitTest {

  private static final String QUEUE_REGION = "queueRegion";

  private SerialGatewaySenderQueue.MetaRegionFactory metaRegionFactory;
  private AbstractGatewaySender sender;
  Region region;
  InternalRegionFactory regionFactory;

  @Before
  public void setup() {
    InternalDistributedSystem mockInternalDistributedSystem = mock(InternalDistributedSystem.class);
    when(mockInternalDistributedSystem.getStatisticsManager())
        .thenReturn(new DummyStatisticsRegistry("", 0));

    GemFireCacheImpl cache = mock(GemFireCacheImpl.class);
    when(cache.getInternalDistributedSystem()).thenReturn(mockInternalDistributedSystem);
    when(cache.getMeterRegistry()).thenReturn(new NoopMeterRegistry());

    region = createLocalRegionMock();

    regionFactory = mock(InternalRegionFactory.class, RETURNS_DEEP_STUBS);
    when(regionFactory.setInternalMetaRegion(any())
        .setDestroyLockFlag(anyBoolean())
        .setSnapshotInputStream(any())
        .setImageTarget(any())
        .setIsUsedForSerialGatewaySenderQueue(anyBoolean())
        .setInternalRegion(anyBoolean())
        .setSerialGatewaySender(any())).thenReturn(regionFactory);
    when(regionFactory.create(QUEUE_REGION)).thenReturn(region);

    when(cache.createInternalRegionFactory(any())).thenReturn(regionFactory);

    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(cache.getCancelCriterion()).thenReturn(cancelCriterion);

    sender = mock(AbstractGatewaySender.class);

    when(sender.getCancelCriterion()).thenReturn(cancelCriterion);
    when(sender.getCache()).thenReturn(cache);
    when(sender.getMaximumQueueMemory()).thenReturn(100);
    when(sender.getLifeCycleLock()).thenReturn(new ReentrantReadWriteLock());
    when(sender.getId()).thenReturn("");
    when(sender.getStatistics()).thenReturn(mock(GatewaySenderStats.class));

    metaRegionFactory = mock(SerialGatewaySenderQueue.MetaRegionFactory.class);

    SerialGatewaySenderQueue.SerialGatewaySenderQueueMetaRegion mockMetaRegion =
        mock(SerialGatewaySenderQueue.SerialGatewaySenderQueueMetaRegion.class);

    when(metaRegionFactory.newMetaRegion(any(), any(), any(), any())).thenReturn(mockMetaRegion);
  }

  @Test
  public void peekGetsExtraEventsWhenMustGroupTransactionEventsAndNotAllEventsForTransactionsInMaxSizeBatch() {
    TestableSerialGatewaySenderQueue queue = new TestableSerialGatewaySenderQueue(sender,
        QUEUE_REGION, metaRegionFactory);
    queue.setGroupTransactionEvents(true);

    List<AsyncEvent<?, ?>> peeked = queue.peek(3, 100);
    assertEquals(4, peeked.size());
    List<AsyncEvent<?, ?>> peekedAfter = queue.peek(3, 100);
    assertEquals(3, peekedAfter.size());
  }

  @Test
  public void peekGetsExtraEventsWhenMustGroupTransactionEventsAndNotAllEventsForTransactionsInBatchByTime() {
    GatewaySenderEventImpl event1 = createMockGatewaySenderEventImpl(1, false, region);
    GatewaySenderEventImpl event2 = createMockGatewaySenderEventImpl(2, false, region);
    GatewaySenderEventImpl event3 = createMockGatewaySenderEventImpl(1, true, region);
    GatewaySenderEventImpl event4 = createMockGatewaySenderEventImpl(2, true, region);
    SerialGatewaySenderQueue.KeyAndEventPair eventPair1 =
        new SerialGatewaySenderQueue.KeyAndEventPair(0L, event1);
    SerialGatewaySenderQueue.KeyAndEventPair eventPair2 =
        new SerialGatewaySenderQueue.KeyAndEventPair(1L, event2);
    SerialGatewaySenderQueue.KeyAndEventPair eventPair3 =
        new SerialGatewaySenderQueue.KeyAndEventPair(2L, event3);

    TestableSerialGatewaySenderQueue realQueue = new TestableSerialGatewaySenderQueue(sender,
        QUEUE_REGION, metaRegionFactory);

    TestableSerialGatewaySenderQueue queue = spy(realQueue);
    queue.setGroupTransactionEvents(true);

    doAnswer(invocation -> eventPair1)
        .doAnswer(invocation -> eventPair2)
        .doAnswer(invocation -> eventPair3)
        .doAnswer(invocation -> null)
        .when(queue).peekAhead();

    doAnswer(invocation -> Collections
        .singletonList(new SerialGatewaySenderQueue.KeyAndEventPair(1L, event4)))
            .when(queue).getElementsMatching(any(), any(), anyLong());

    List<AsyncEvent<?, ?>> peeked = queue.peek(-1, 1);
    assertEquals(4, peeked.size());
  }

  @Test
  public void peekDoesNotGetExtraEventsWhenNotMustGroupTransactionEventsAndNotAllEventsForTransactionsInBatchMaxSize() {
    TestableSerialGatewaySenderQueue queue = new TestableSerialGatewaySenderQueue(sender,
        QUEUE_REGION, metaRegionFactory);

    List<AsyncEvent<?, ?>> peeked = queue.peek(3, 100);
    assertEquals(3, peeked.size());
    List<AsyncEvent<?, ?>> peekedAfter = queue.peek(3, 100);
    assertEquals(3, peekedAfter.size());
    peekedAfter = queue.peek(1, 100);
    assertEquals(1, peekedAfter.size());
  }

  @Test
  public void peekDoesNotGetExtraEventsWhenNotMustGroupTransactionEventsAndNotAllEventsForTransactionsInBatchByTime() {
    GatewaySenderEventImpl event1 = createMockGatewaySenderEventImpl(1, false, region);
    GatewaySenderEventImpl event2 = createMockGatewaySenderEventImpl(2, false, region);
    GatewaySenderEventImpl event3 = createMockGatewaySenderEventImpl(1, true, region);
    GatewaySenderEventImpl event4 = createMockGatewaySenderEventImpl(2, true, region);
    SerialGatewaySenderQueue.KeyAndEventPair eventPair1 =
        new SerialGatewaySenderQueue.KeyAndEventPair(0L, event1);
    SerialGatewaySenderQueue.KeyAndEventPair eventPair2 =
        new SerialGatewaySenderQueue.KeyAndEventPair(1L, event2);
    SerialGatewaySenderQueue.KeyAndEventPair eventPair3 =
        new SerialGatewaySenderQueue.KeyAndEventPair(2L, event3);

    TestableSerialGatewaySenderQueue realQueue = new TestableSerialGatewaySenderQueue(sender,
        QUEUE_REGION, metaRegionFactory);

    TestableSerialGatewaySenderQueue queue = spy(realQueue);
    queue.setGroupTransactionEvents(false);

    doAnswer(invocation -> eventPair1)
        .doAnswer(invocation -> eventPair2)
        .doAnswer(invocation -> eventPair3)
        .doAnswer(invocation -> null)
        .when(queue).peekAhead();

    doAnswer(invocation -> Collections
        .singletonList(new SerialGatewaySenderQueue.KeyAndEventPair(2L, event4)))
            .when(queue).getElementsMatching(any(), any(), anyLong());

    List<AsyncEvent<?, ?>> peeked = queue.peek(-1, 1);
    assertEquals(3, peeked.size());
  }

  @Test
  public void peekEventsFromIncompleteTransactionsDoesNotThrowConcurrentModificationExceptionWhenCompletingTwoTransactions() {
    GatewaySenderEventImpl event1 = createMockGatewaySenderEventImpl(1, false, region);
    GatewaySenderEventImpl event2 = createMockGatewaySenderEventImpl(2, false, region);

    TestableSerialGatewaySenderQueue queue = new TestableSerialGatewaySenderQueue(sender,
        QUEUE_REGION, metaRegionFactory);

    queue.setGroupTransactionEvents(true);

    @SuppressWarnings("unchecked")
    List<AsyncEvent<?, ?>> batch = new ArrayList(Arrays.asList(event1, event2));
    queue.postProcessBatch(batch, 0);
  }

  @Test
  public void removeExtraPeekedEventDoesNotRemoveFromExtraPeekedIdsUntilPreviousEventIsRemoved() {
    TestableSerialGatewaySenderQueue queue = new TestableSerialGatewaySenderQueue(sender,
        QUEUE_REGION, metaRegionFactory);
    queue.setGroupTransactionEvents(true);
    List<AsyncEvent<?, ?>> peeked = queue.peek(3, -1);
    assertEquals(4, peeked.size());
    assertThat(queue.getLastPeekedId()).isEqualTo(2);
    assertThat(queue.getExtraPeekedIds().contains(5L)).isTrue();


    for (Object ignored : peeked) {
      queue.remove();
    }
    assertThat(queue.getExtraPeekedIds().contains(5L)).isTrue();

    peeked = queue.peek(3, -1);
    assertEquals(3, peeked.size());
    assertThat(queue.getExtraPeekedIds().contains(5L)).isTrue();

    for (Object ignored : peeked) {
      queue.remove();
    }
    assertThat(queue.getExtraPeekedIds().contains(5L)).isFalse();
  }

  private GatewaySenderEventImpl createMockGatewaySenderEventImpl(int transactionId,
      boolean isLastEventInTransaction, Region region) {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.getTransactionId()).thenReturn(new TXId(null, transactionId));
    when(event.makeHeapCopyIfOffHeap()).thenReturn(event);
    when(event.isLastEventInTransaction()).thenReturn(isLastEventInTransaction);
    when(event.getRegion()).thenReturn(region);
    return event;
  }

  private LocalRegion createLocalRegionMock() {
    GatewaySenderEventImpl event1 = createMockGatewaySenderEventImpl(1, false, region);
    GatewaySenderEventImpl event2 = createMockGatewaySenderEventImpl(2, false, region);
    GatewaySenderEventImpl event3 = createMockGatewaySenderEventImpl(1, true, region);
    GatewaySenderEventImpl event4 = createMockGatewaySenderEventImpl(3, true, region);
    GatewaySenderEventImpl event5 = createMockGatewaySenderEventImpl(4, true, region);
    GatewaySenderEventImpl event6 = createMockGatewaySenderEventImpl(2, true, region);
    GatewaySenderEventImpl event7 = createMockGatewaySenderEventImpl(5, false, region);

    LocalRegion region = mock(LocalRegion.class);

    when(region.getValueInVMOrDiskWithoutFaultIn(0L)).thenReturn(event1);
    when(region.getValueInVMOrDiskWithoutFaultIn(1L)).thenReturn(event2);
    when(region.getValueInVMOrDiskWithoutFaultIn(2L)).thenReturn(event3);
    when(region.getValueInVMOrDiskWithoutFaultIn(3L)).thenReturn(event4);
    when(region.getValueInVMOrDiskWithoutFaultIn(4L)).thenReturn(event5);
    when(region.getValueInVMOrDiskWithoutFaultIn(5L)).thenReturn(event6);
    when(region.getValueInVMOrDiskWithoutFaultIn(6L)).thenReturn(event7);

    Map<Long, AsyncEvent<?, ?>> map = new HashMap<>();
    map.put(0L, event1);
    map.put(1L, event2);
    map.put(2L, event3);
    map.put(3L, event4);
    map.put(4L, event5);
    map.put(5L, event6);
    map.put(6L, event7);

    when(region.keySet()).thenReturn(map.keySet());
    return region;
  }

  private static class TestableSerialGatewaySenderQueue extends SerialGatewaySenderQueue {

    private boolean groupTransactionEvents = false;

    public TestableSerialGatewaySenderQueue(final AbstractGatewaySender sender,
        String regionName, final MetaRegionFactory metaRegionFactory) {
      super(sender, regionName, null, false, metaRegionFactory);
    }

    public void setGroupTransactionEvents(boolean groupTransactionEvents) {
      this.groupTransactionEvents = groupTransactionEvents;
    }

    @Override
    protected void addOverflowStatisticsToMBean(Cache cache, AbstractGatewaySender sender) {}

  }
}
