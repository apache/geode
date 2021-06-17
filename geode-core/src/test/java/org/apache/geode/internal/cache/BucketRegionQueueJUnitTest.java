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
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderHelper;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.internal.statistics.DummyStatisticsFactory;
import org.apache.geode.test.fake.Fakes;

public class BucketRegionQueueJUnitTest {

  private static final String GATEWAY_SENDER_ID = "ny";
  private static final int BUCKET_ID = 85;
  private static final long KEY = 198;

  private GemFireCacheImpl cache;
  private PartitionedRegion queueRegion;
  private AbstractGatewaySender sender;
  private PartitionedRegion rootRegion;
  private BucketRegionQueue bucketRegionQueue;
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
    this.queueRegion =
        ParallelGatewaySenderHelper.createMockQueueRegion(this.cache,
            ParallelGatewaySenderHelper.getRegionQueueName(GATEWAY_SENDER_ID));
  }

  private void createGatewaySender() {
    // Mock gateway sender
    this.sender = ParallelGatewaySenderHelper.createGatewaySender(this.cache);
    when(this.queueRegion.getParallelGatewaySender()).thenReturn(this.sender);
    stats = new GatewaySenderStats(new DummyStatisticsFactory(), "gatewaySenderStats-", "ln",
        disabledClock());
    when(this.sender.getStatistics()).thenReturn(stats);
  }

  private void createRootRegion() {
    // Mock root region
    this.rootRegion = mock(PartitionedRegion.class);
    when(this.rootRegion.getFullPath())
        .thenReturn(Region.SEPARATOR + PartitionedRegionHelper.PR_ROOT_REGION_NAME);
  }

  private void createBucketRegionQueue() {
    BucketRegionQueue realBucketRegionQueue = ParallelGatewaySenderHelper
        .createBucketRegionQueue(this.cache, this.rootRegion, this.queueRegion, BUCKET_ID);
    this.bucketRegionQueue = spy(realBucketRegionQueue);
    this.bucketRegionQueue.getEventTracker().setInitialized();
  }

  @Test
  public void testBasicDestroyConflationEnabledAndValueInRegionAndIndex() {
    // Create the event
    EntryEventImpl event = EntryEventImpl.create(this.bucketRegionQueue, Operation.DESTROY,
        KEY, "value", null, false, mock(DistributedMember.class));

    // Don't allow hasSeenEvent to be invoked
    doReturn(false).when(this.bucketRegionQueue).hasSeenEvent(event);

    // Set conflation enabled and the appropriate return values for containsKey and removeIndex
    when(this.queueRegion.isConflationEnabled()).thenReturn(true);
    when(this.bucketRegionQueue.containsKey(KEY)).thenReturn(true);
    doReturn(true).when(this.bucketRegionQueue).removeIndex(KEY);

    // Invoke basicDestroy
    this.bucketRegionQueue.basicDestroy(event, true, null, false);

    // Verify mapDestroy is invoked
    verify(this.bucketRegionQueue).mapDestroy(event, true, false, null);
  }

  @Test(expected = EntryNotFoundException.class)
  public void testBasicDestroyConflationEnabledAndValueNotInRegion() {
    // Create the event
    EntryEventImpl event = EntryEventImpl.create(this.bucketRegionQueue, Operation.DESTROY,
        KEY, "value", null, false, mock(DistributedMember.class));

    // Don't allow hasSeenEvent to be invoked
    doReturn(false).when(this.bucketRegionQueue).hasSeenEvent(event);

    // Set conflation enabled and the appropriate return values for containsKey and removeIndex
    when(this.queueRegion.isConflationEnabled()).thenReturn(true);
    when(this.bucketRegionQueue.containsKey(KEY)).thenReturn(false);

    // Invoke basicDestroy
    this.bucketRegionQueue.basicDestroy(event, true, null, false);
  }

  @Test
  public void testGetElementsMatchingWithParallelGatewaySenderQueuePredicatesAndSomeEventsNotInTransactions()
      throws ForceReattemptException {
    ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(this.sender);

    TransactionId tx1 = new TXId(null, 1);
    TransactionId tx2 = new TXId(null, 2);
    TransactionId tx3 = new TXId(null, 3);

    GatewaySenderEventImpl event1 = createMockGatewaySenderEvent(1, tx1, false);
    GatewaySenderEventImpl eventNotInTransaction1 = createMockGatewaySenderEvent(2, null, false);
    GatewaySenderEventImpl event2 = createMockGatewaySenderEvent(3, tx2, false);
    GatewaySenderEventImpl event3 = createMockGatewaySenderEvent(4, tx1, true);
    GatewaySenderEventImpl event4 = createMockGatewaySenderEvent(5, tx2, true);
    GatewaySenderEventImpl event5 = createMockGatewaySenderEvent(6, tx3, false);
    GatewaySenderEventImpl event6 = createMockGatewaySenderEvent(7, tx3, false);
    GatewaySenderEventImpl event7 = createMockGatewaySenderEvent(8, tx1, true);

    this.bucketRegionQueue
        .cleanUpDestroyedTokensAndMarkGIIComplete(InitialImageOperation.GIIStatus.NO_GII);

    this.bucketRegionQueue.addToQueue(1L, event1);
    this.bucketRegionQueue.addToQueue(2L, eventNotInTransaction1);
    this.bucketRegionQueue.addToQueue(3L, event2);
    this.bucketRegionQueue.addToQueue(4L, event3);
    this.bucketRegionQueue.addToQueue(5L, event4);
    this.bucketRegionQueue.addToQueue(6L, event5);
    this.bucketRegionQueue.addToQueue(7L, event6);
    this.bucketRegionQueue.addToQueue(8L, event7);

    Predicate<GatewaySenderEventImpl> hasTransactionIdPredicate =
        ParallelGatewaySenderQueue.getHasTransactionIdPredicate(tx1);
    Predicate<GatewaySenderEventImpl> isLastEventInTransactionPredicate =
        ParallelGatewaySenderQueue.getIsLastEventInTransactionPredicate();
    List<Object> objects = this.bucketRegionQueue.getElementsMatching(hasTransactionIdPredicate,
        isLastEventInTransactionPredicate);

    assertEquals(2, objects.size());
    assertEquals(objects, Arrays.asList(new Object[] {event1, event3}));

    objects = this.bucketRegionQueue.getElementsMatching(hasTransactionIdPredicate,
        isLastEventInTransactionPredicate);
    assertEquals(1, objects.size());
    assertEquals(objects, Arrays.asList(new Object[] {event7}));

    hasTransactionIdPredicate =
        ParallelGatewaySenderQueue.getHasTransactionIdPredicate(tx2);
    objects = this.bucketRegionQueue.getElementsMatching(hasTransactionIdPredicate,
        isLastEventInTransactionPredicate);
    assertEquals(2, objects.size());
    assertEquals(objects, Arrays.asList(new Object[] {event2, event4}));
  }

  @Test
  public void testPeekedElementsArePossibleDuplicate()
      throws Exception {
    ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(this.sender);

    LocalRegion lr = mock(LocalRegion.class);
    when(lr.getFullPath()).thenReturn("/dataStoreRegion");
    when(lr.getCache()).thenReturn(this.cache);

    // Configure conflation
    when(this.sender.isBatchConflationEnabled()).thenReturn(true);
    when(sender.getStatistics()).thenReturn(mock(GatewaySenderStats.class));

    this.bucketRegionQueue
        .cleanUpDestroyedTokensAndMarkGIIComplete(InitialImageOperation.GIIStatus.NO_GII);

    // Create a batch of conflatable events with duplicate update events
    Object lastUpdateValue = "Object_13968_5";
    long lastUpdateSequenceId = 104;
    GatewaySenderEventImpl event1 = createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13964", "Object_13964_1", 1, 100);
    GatewaySenderEventImpl event2 = createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13965", "Object_13965_2", 1, 101);
    GatewaySenderEventImpl event3 = createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13966", "Object_13966_3", 1, 102);
    GatewaySenderEventImpl event4 = createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13967", "Object_13967_4", 1, 103);
    GatewaySenderEventImpl event5 = createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13968", lastUpdateValue, 1, lastUpdateSequenceId);

    this.bucketRegionQueue.addToQueue(1L, event1);
    this.bucketRegionQueue.addToQueue(2L, event2);
    this.bucketRegionQueue.addToQueue(3L, event3);
    this.bucketRegionQueue.addToQueue(4L, event4);
    this.bucketRegionQueue.addToQueue(5L, event5);

    this.bucketRegionQueue.beforeAcquiringPrimaryState();

    List<Object> objects = this.bucketRegionQueue.getHelperQueueList();

    assertThat(objects.size()).isEqualTo(5);

    for (Object o : objects) {
      assertThat(((GatewaySenderEventImpl) o).getPossibleDuplicate()).isFalse();
    }

    Object peekObj = this.bucketRegionQueue.peek();

    while (peekObj != null) {
      assertThat(((GatewaySenderEventImpl) peekObj).getPossibleDuplicate()).isTrue();
      peekObj = this.bucketRegionQueue.peek();
    }

  }

  GatewaySenderEventImpl createMockGatewaySenderEvent(Object key, TransactionId tId,
      boolean isLastEventInTx) {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.isLastEventInTransaction()).thenReturn(isLastEventInTx);
    when(event.getTransactionId()).thenReturn(tId);
    when(event.getKey()).thenReturn(key);
    return event;
  }

  private GatewaySenderEventImpl createGatewaySenderEvent(LocalRegion lr, Operation operation,
      Object key, Object value, long threadId, long sequenceId)
      throws Exception {
    when(lr.getKeyInfo(key, value, null)).thenReturn(new KeyInfo(key, null, null));
    when(lr.getTXId()).thenReturn(null);

    EntryEventImpl eei = EntryEventImpl.create(lr, operation, key, value, null, false, null);
    eei.setEventId(new EventID(new byte[16], threadId, sequenceId));

    return new GatewaySenderEventImpl(getEnumListenerEvent(operation), eei, null, true, false);
  }

  private EnumListenerEvent getEnumListenerEvent(Operation operation) {
    EnumListenerEvent ele = null;
    if (operation.isCreate()) {
      ele = EnumListenerEvent.AFTER_CREATE;
    } else if (operation.isUpdate()) {
      ele = EnumListenerEvent.AFTER_UPDATE;
    } else if (operation.isDestroy()) {
      ele = EnumListenerEvent.AFTER_DESTROY;
    }
    return ele;
  }



}
