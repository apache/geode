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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.statistics.DummyStatisticsRegistry;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.metrics.internal.NoopMeterRegistry;

public class SerialGatewaySenderQueueJUnitTest {

  private static final String TEST_REGION = "testRegion";
  private SerialGatewaySenderQueue.MetaRegionFactory metaRegionFactory;
  private GemFireCacheImpl cache;
  private AbstractGatewaySender sender;
  Region region;
  InternalRegionFactory regionFactory;

  @Before
  public void setup() {
    InternalDistributedSystem mockInternalDistributedSystem = mock(InternalDistributedSystem.class);
    when(mockInternalDistributedSystem.getStatisticsManager())
        .thenReturn(new DummyStatisticsRegistry("", 0));

    cache = mock(GemFireCacheImpl.class);
    when(cache.getInternalDistributedSystem()).thenReturn(mockInternalDistributedSystem);
    when(cache.getMeterRegistry()).thenReturn(new NoopMeterRegistry());

    region = createDistributedRegion(TEST_REGION, cache);

    regionFactory = mock(InternalRegionFactory.class, RETURNS_DEEP_STUBS);
    when(regionFactory.setInternalMetaRegion(any())
        .setDestroyLockFlag(anyBoolean())
        .setSnapshotInputStream(any())
        .setImageTarget(any())
        .setIsUsedForSerialGatewaySenderQueue(anyBoolean())
        .setInternalRegion(anyBoolean())
        .setSerialGatewaySender(any())).thenReturn(regionFactory);
    when(regionFactory.create(TEST_REGION)).thenReturn(region);

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
  public void peekedExtraEventsWhenIsGroupTransactionEvents()
      throws Exception {
    GatewaySenderEventImpl event1 = createMockGatewaySenderEventImpl(1, false, region);
    GatewaySenderEventImpl event2 = createMockGatewaySenderEventImpl(2, false, region);
    GatewaySenderEventImpl event3 = createMockGatewaySenderEventImpl(1, true, region);
    GatewaySenderEventImpl event4 = createMockGatewaySenderEventImpl(2, true, region);
    GatewaySenderEventImpl event5 = createMockGatewaySenderEventImpl(3, false, region);
    GatewaySenderEventImpl event6 = createMockGatewaySenderEventImpl(3, true, region);

    TestableSerialGatewaySenderQueue queue = new TestableSerialGatewaySenderQueue(sender,
        TEST_REGION, metaRegionFactory);
    queue.setGroupTransactionEvents(true);

    queue.put(event1);
    queue.put(event2);
    queue.put(event3);
    queue.put(event4);
    queue.put(event5);
    queue.put(event6);

    List peeked = queue.peek(3, 1000);
    assertEquals(4, peeked.size());
    List peekedAfter = queue.peek(3, 1000);
    assertEquals(2, peekedAfter.size());
  }

  @Test
  public void peekedExtraEventsWhenIsGroupTransactionEventsTimeout()
      throws Exception {
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
        TEST_REGION, metaRegionFactory);

    TestableSerialGatewaySenderQueue queue = spy(realQueue);
    queue.setGroupTransactionEvents(true);

    doAnswer(invocation -> eventPair1)
        .doAnswer(invocation -> eventPair2)
        .doAnswer(invocation -> eventPair3)
        .doAnswer(invocation -> null)
        .when(queue).peekAhead();

    doAnswer(invocation -> new SerialGatewaySenderQueue.EventsAndLastKey(
        Arrays.asList(new Object[] {event4}), 2L))
            .when(queue).getElementsMatching(any(), any(), anyLong());

    List peeked = queue.peek(-1, 1);
    assertEquals(4, peeked.size());
  }

  @Test
  public void peekedMaxEventsWhenNotIsGroupTransactionEvents()
      throws Exception {
    GatewaySenderEventImpl event1 = createMockGatewaySenderEventImpl(1, false, region);
    GatewaySenderEventImpl event2 = createMockGatewaySenderEventImpl(1, false, region);
    GatewaySenderEventImpl event3 = createMockGatewaySenderEventImpl(2, false, region);
    GatewaySenderEventImpl event4 = createMockGatewaySenderEventImpl(1, true, region);
    GatewaySenderEventImpl event5 = createMockGatewaySenderEventImpl(2, true, region);

    TestableSerialGatewaySenderQueue queue = new TestableSerialGatewaySenderQueue(sender,
        TEST_REGION, metaRegionFactory);

    queue.put(event1);
    queue.put(event2);
    queue.put(event3);
    queue.put(event4);
    queue.put(event5);

    List peeked = queue.peek(3, 1000);
    assertEquals(3, peeked.size());
    List peekedAfter = queue.peek(3, 1000);
    assertEquals(2, peekedAfter.size());
  }

  private static GatewaySenderEventImpl createMockGatewaySenderEventImpl(int transactionId,
      boolean isLastEventInTransaction, Region region) {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.getTransactionId()).thenReturn(new TXId(null, transactionId));
    when(event.makeHeapCopyIfOffHeap()).thenReturn(event);
    when(event.isLastEventInTransaction()).thenReturn(isLastEventInTransaction);
    when(event.getRegion()).thenReturn(region);
    return event;
  }

  private Region createDistributedRegion(String regionName, Cache cache) {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.NORMAL);
    RegionAttributes attrs = factory.create();
    InternalRegionArguments internalRegionArgs = new InternalRegionArguments();
    return new TestLocalRegion(regionName, attrs, null, (InternalCache) cache, internalRegionArgs,
        new TestStatisticsClock());
  }

  public class TestLocalRegion extends LocalRegion {
    Map map = new ConcurrentHashMap();

    public TestLocalRegion(String regionName, RegionAttributes attrs,
        LocalRegion parentRegion, InternalCache cache,
        InternalRegionArguments internalRegionArgs,
        StatisticsClock statisticsClock) {
      super(regionName, attrs, parentRegion, cache, internalRegionArgs, statisticsClock);
    }

    @Override
    public Object get(Object key) {
      return map.get(key);
    }

    @Override
    public Object put(Object key, Object value) {
      map.put(key, value);
      return key;
    }

    @Override
    public Set entrySet() {
      return map.entrySet();
    }

    @Override
    public long cacheTimeMillis() {
      return 0;
    }

    @Override
    public boolean getConcurrencyChecksEnabled() {
      return false;
    }

    @Override
    public void checkReadiness() {}

    @Override
    public Object getValueInVMOrDiskWithoutFaultIn(Object key) {
      return get(key);
    }

    @Override
    public int size() {
      return map.size();
    }
  }

  public class TestStatisticsClock implements StatisticsClock {
    @Override
    public long getTime() {
      return 0;
    }
  }

  private class TestableSerialGatewaySenderQueue extends SerialGatewaySenderQueue {

    private boolean isGroupTransactionEvents = false;

    public TestableSerialGatewaySenderQueue(final AbstractGatewaySender sender,
        String regionName, final MetaRegionFactory metaRegionFactory) {
      super(sender, regionName, null, false, metaRegionFactory);
    }

    public void setGroupTransactionEvents(boolean isGroupTransactionEvents) {
      this.isGroupTransactionEvents = isGroupTransactionEvents;
    }

    @Override
    public boolean isGroupTransactionEvents() {
      return isGroupTransactionEvents;
    }

    @Override
    protected void addOverflowStatisticsToMBean(Cache cache, AbstractGatewaySender sender) {}
  }
}
