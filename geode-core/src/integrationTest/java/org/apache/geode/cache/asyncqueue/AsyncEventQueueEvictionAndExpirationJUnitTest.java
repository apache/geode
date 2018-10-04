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
package org.apache.geode.cache.asyncqueue;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.test.junit.categories.AEQTest;

@Category({AEQTest.class})
public class AsyncEventQueueEvictionAndExpirationJUnitTest {

  private AsyncEventQueue aeq;
  private Cache cache;
  private Region region;
  String aeqId;
  List<AsyncEvent> events = new ArrayList<AsyncEvent>();

  @Rule
  public TestName name = new TestName();

  @Before
  public void setup() {
    events.clear();
    try {
      cache = CacheFactory.getAnyInstance();
    } catch (Exception e) {
      // ignore
    }
    if (null == cache) {
      cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").create();
    }
    aeqId = name.getMethodName();
  }

  @After
  public void tearDown() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache = null;
    }
  }


  @Test
  public void isForwardExpirationDestroyAttributeFalseByDefault() {
    AsyncEventListener al = mock(AsyncEventListener.class);
    aeq = cache.createAsyncEventQueueFactory().create("aeq", al);
    // Test for default value of isIgnoreEvictionAndExpiration setting.
    assertFalse(aeq.isForwardExpirationDestroy());
  }

  @Test
  public void canSetTrueForForwardExpirationDestroy() {
    AsyncEventListener al = mock(AsyncEventListener.class);
    aeq = cache.createAsyncEventQueueFactory().setForwardExpirationDestroy(true).create("aeq", al);
    // Test for default value of isIgnoreEvictionAndExpiration setting.
    assertTrue(aeq.isForwardExpirationDestroy());
  }


  @Test
  public void evictionDestroyOpEventsNotPropogatedByDefault() {
    // For Replicated Region with eviction-destroy op.
    // Number of expected events 2. Two for create and none for eviction destroy.
    createRegionAeqAndPopulate(false /* isPR */, false /* forwardExpirationDestroy */,
        true /* eviction */, false /* evictionOverflow */, false /* expirationDestroy */,
        false /* expirationInvalidate */);

    // Wait for region to evict/expire events.
    await()
        .untilAsserted(() -> assertEquals(1, region.size()));

    // Validate events that are not queued.
    // This guarantees that eviction/expiration is performed and events are
    // sent all the way to Gateway.
    await()
        .untilAsserted(() -> assertEquals(1, getEventsNotQueuedSize(aeqId)));

    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(2, events.size()));

  }

  @Test
  public void evictionDestroyOpEventsNotPropogatedByDefaultForPR() {
    // For PR with eviction-destroy op.
    // Number of expected events 2. Two for create and none for eviction destroy.
    createRegionAeqAndPopulate(true /* isPR */, false /* forwardExpirationDestroy */,
        true /* eviction */, false /* evictionOverflow */, false /* expirationDestroy */,
        false /* expirationInvalidate */);

    // Wait for region to evict/expire events.
    await()
        .untilAsserted(() -> assertEquals(1, region.size()));

    // Validate events that are not queued.
    // This guarantees that eviction/expiration is performed and events are
    // sent all the way to Gateway.
    // In case of eviction one event is evicted that should not be queued.
    await()
        .untilAsserted(() -> assertEquals(1, getEventsNotQueuedSize(aeqId)));

    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(2, events.size()));

  }

  @Test
  public void expirationDestroyOpEventsNotPropogatedByDefault() {
    // For Replicated Region with expiration-destroy op.
    // Number of expected events 2. Two for create and none for eviction destroy.
    createRegionAeqAndPopulate(false /* isPR */, false /* forwardExpirationDestroy */,
        false /* eviction */, false /* evictionOverflow */, true /* expirationDestroy */,
        false /* expirationInvalidate */);

    // Wait for region to evict/expire events.
    await()
        .untilAsserted(() -> assertEquals(0, region.size()));

    // Validate events that are not queued.
    // This guarantees that eviction/expiration is performed and events are
    // sent all the way to Gateway.
    await()
        .untilAsserted(() -> assertEquals(2, getEventsNotQueuedSize(aeqId)));

    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(2, events.size()));

  }

  @Test
  public void expirationDestroyOpEventsNotPropogatedByDefaultForPR() {
    // For PR with expiration-destroy op.
    // Number of expected events 2. Two for create and none for eviction destroy.
    createRegionAeqAndPopulate(true /* isPR */, false /* forwardExpirationDestroy */,
        false /* eviction */, false /* evictionOverflow */, true /* expirationDestroy */,
        false /* expirationInvalidate */);

    // Wait for region to evict/expire events.
    await()
        .untilAsserted(() -> assertEquals(0, region.size()));

    // Validate events that are not queued.
    // This guarantees that eviction/expiration is performed and events are
    // sent all the way to Gateway.
    await()
        .untilAsserted(() -> assertEquals(2, getEventsNotQueuedSize(aeqId)));

    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(2, events.size()));

  }

  @Test
  public void expirationInvalidOpEventsNotPropogatedByDefault() {
    // For Replicated Region with expiration-invalid op.
    // Number of expected events 2. Two for create and none for eviction destroy.
    createRegionAeqAndPopulate(false /* isPR */, false /* forwardExpirationDestroy */,
        false /* eviction */, false /* evictionOverflow */, false /* expirationDestroy */,
        true /* expirationInvalidate */);

    LocalRegion lr = (LocalRegion) region;
    // Wait for region to evict/expire events.
    await()
        .untilAsserted(() -> assertEquals(2, lr.getCachePerfStats().getInvalidates()));
    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(2, events.size()));


  }

  @Test
  public void expirationInvalidOpEventsNotPropogatedByDefaultForPR() {
    // For Replicated Region with expiration-invalid op.
    // Number of expected events 2. Two for create and none for eviction destroy.
    createRegionAeqAndPopulate(true /* isPR */, false /* forwardExpirationDestroy */,
        false /* eviction */, false /* evictionOverflow */, false /* expirationDestroy */,
        true /* expirationInvalidate */);

    LocalRegion lr = (LocalRegion) region;
    // Wait for region to evict/expire events.
    await()
        .untilAsserted(() -> assertEquals(2, lr.getCachePerfStats().getInvalidates()));
    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(2, events.size()));
  }

  @Test
  public void evictionNotPropogatedUsingForwardExpirationDestroyAttribute() {
    // For Replicated Region with eviction-destroy op.
    // Number of expected events 2. Two for create and none for eviction destroy.
    createRegionAeqAndPopulate(false /* isPR */, true /* forwardExpirationDestroy */,
        true /* eviction */, false /* evictionOverflow */, false /* expirationDestroy */,
        false /* expirationInvalidate */);

    // Wait for region to evict/expire events.
    await()
        .untilAsserted(() -> assertEquals(1, region.size()));

    // Validate events that are not queued.
    // This guarantees that eviction/expiration is performed and events are
    // sent all the way to Gateway.
    await()
        .untilAsserted(() -> assertEquals(1, getEventsNotQueuedSize(aeqId)));

    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(2, events.size()));


  }

  @Test
  public void evictionNotPropogatedUsingForwardExpirationDestroyAttributeForPR() {
    // For PR with eviction-destroy op.
    // Number of expected events 3. Two for create and none for eviction destroy.
    createRegionAeqAndPopulate(true /* isPR */, true /* forwardExpirationDestroy */,
        true /* eviction */, false /* evictionOverflow */, false /* expirationDestroy */,
        false /* expirationInvalidate */);

    // Wait for region to evict/expire events.
    await()
        .untilAsserted(() -> assertEquals(1, region.size()));

    // Validate events that are not queued.
    // This guarantees that eviction/expiration is performed and events are
    // sent all the way to Gateway.
    await()
        .untilAsserted(() -> assertEquals(1, getEventsNotQueuedSize(aeqId)));

    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(2, events.size()));


  }

  @Test
  public void overflowNotPropogatedUsingForwardExpirationDestroyAttribute() {
    // For Replicated Region with eviction-overflow op.
    // Number of expected events 2. Two for create and non for eviction overflow.
    createRegionAeqAndPopulate(false /* isPR */, true /* forwardExpirationDestroy */,
        false /* eviction */, true /* evictionOverflow */, false /* expirationDestroy */,
        false /* expirationInvalidate */);

    // Wait for region to evict/expire events.
    LocalRegion lr = (LocalRegion) region;
    await()
        .untilAsserted(() -> assertEquals(1, lr.getDiskRegion().getStats().getNumOverflowOnDisk()));
    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(2, events.size()));
  }

  @Test
  public void overflowNotPropogatedUsingForwardExpirationDestroyAttributeForPR() {
    // For PR with eviction-overflow op.
    // Number of expected events 2. Two for create and non for eviction overflow.
    createRegionAeqAndPopulate(true /* isPR */, true /* forwardExpirationDestroy */,
        false /* eviction */, true /* evictionOverflow */, false /* expirationDestroy */,
        false /* expirationInvalidate */);

    // Wait for region to evict/expire events.
    PartitionedRegion pr = (PartitionedRegion) region;
    await()
        .untilAsserted(() -> assertEquals(1, pr.getDiskRegionStats().getNumOverflowOnDisk()));
    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(2, events.size()));

  }

  @Test
  public void expirationDestroyPropogatedUsingForwardExpirationDestroyAttribute() {
    // For Replicated Region with expiration-destroy op.
    // Number of expected events 4. Two for create and Two for expiration destroy.
    createRegionAeqAndPopulate(false /* isPR */, true /* forwardExpirationDestroy */,
        false /* eviction */, false /* evictionOverflow */, true /* expirationDestroy */,
        false /* expirationInvalidate */);

    // Wait for region to evict/expire events.
    await()
        .untilAsserted(() -> assertEquals(0, region.size()));

    await()
        .untilAsserted(() -> assertEquals(4, getEventsReceived(aeqId)));

    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(4, events.size()));

    assertTrue("Expiration event not arrived", checkForOperation(events, false, true));

  }

  @Test
  public void expirationDestroyPropogatedUsingForwardExpirationDestroyAttributeForPR() {
    // For PR with expiration-destroy op.
    // Number of expected events 4. Two for create and Two for expiration destroy.
    createRegionAeqAndPopulate(true /* isPR */, true /* forwardExpirationDestroy */,
        false /* eviction */, false /* evictionOverflow */, true /* expirationDestroy */,
        false /* expirationInvalidate */);

    // Wait for region to evict/expire events.
    await()
        .untilAsserted(() -> assertEquals(0, region.size()));

    await()
        .untilAsserted(() -> assertEquals(4, getEventsReceived(aeqId)));

    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(4, events.size()));

    assertTrue("Expiration event not arrived", checkForOperation(events, false, true));

  }

  @Test
  public void expirationInvalidateNotPropogatedUsingForwardExpirationDestroyAttribute() {
    // For Replicated Region with expiration-invalidate op.
    // Currently invalidate event callbacks are not made to GateWay sender.
    // Invalidates are not sent to AEQ.
    // Number of expected events 2. None for expiration invalidate.
    createRegionAeqAndPopulate(false /* isPR */, true /* forwardExpirationDestroy */,
        false /* eviction */, false /* evictionOverflow */, false /* expirationDestroy */,
        true /* expirationInvalidate */);

    // Wait for region to evict/expire events.
    LocalRegion lr = (LocalRegion) region;
    await()
        .untilAsserted(() -> assertEquals(2, lr.getCachePerfStats().getInvalidates()));

    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(2, events.size()));

  }

  @Test
  public void expirationInvalidateNotPropogatedUsingForwardExpirationDestroyAttributeForPR() {
    // For PR with expiration-invalidate op.
    // Currently invalidate event callbacks are not made to GateWay sender.
    // Invalidates are not sent to AEQ.
    // Number of expected events 2. None for expiration invalidate.
    createRegionAeqAndPopulate(true /* isPR */, true /* forwardExpirationDestroy */,
        false /* eviction */, false /* evictionOverflow */, false /* expirationDestroy */,
        true /* expirationInvalidate */);

    // Wait for region to evict/expire events.
    LocalRegion lr = (LocalRegion) region;
    await()
        .untilAsserted(() -> assertEquals(2, lr.getCachePerfStats().getInvalidates()));

    // The AQListner should get expected events.
    await()
        .untilAsserted(() -> assertEquals(2, events.size()));

  }



  private void createRegionAeqAndPopulate(boolean isPR, boolean forwardExpirationDestroy,
      boolean eviction, boolean evictionOverflow, boolean expirationDestroy,
      boolean expirationInvalidate) {
    // String aeqId = "AEQTest";
    String aeqId = name.getMethodName();

    // Create AEQ
    createAsyncEventQueue(aeqId, forwardExpirationDestroy, events);

    region = createRegion("ReplicatedRegionForAEQ", isPR, aeqId, eviction, evictionOverflow,
        expirationDestroy, expirationInvalidate);

    // Populate region with two entries.
    region.put("Key-1", "Value-1");
    region.put("Key-2", "Value-2");

  }


  private void waitForAEQEventsNotQueued() {
    await().until(() -> {
      return (getEventsNotQueuedSize(aeqId) >= 1);
    });
  }


  private boolean checkForOperation(List<AsyncEvent> events, boolean invalidate, boolean destroy) {
    boolean found = false;
    for (AsyncEvent e : events) {
      if (invalidate && e.getOperation().isInvalidate()) {
        found = true;
        break;
      }
      if (destroy && e.getOperation().isDestroy()) {
        found = true;
        break;
      }
    }
    return found;
  }

  public int getEventsNotQueuedSize(String aeqId) {
    AsyncEventQueueImpl aeq = (AsyncEventQueueImpl) cache.getAsyncEventQueue(aeqId);
    AbstractGatewaySender sender = (AbstractGatewaySender) aeq.getSender();
    return sender.getStatistics().getEventsNotQueued();
  }


  public int getEventsReceived(String aeqId) {
    AsyncEventQueueImpl aeq = (AsyncEventQueueImpl) cache.getAsyncEventQueue(aeqId);
    AbstractGatewaySender sender = (AbstractGatewaySender) aeq.getSender();
    return sender.getStatistics().getEventsReceived();
  }

  private void createAsyncEventQueue(String id, boolean forwardExpirationDestroy,
      List<AsyncEvent> storeEvents) {
    AsyncEventListener al = this.createAsyncListener(storeEvents);
    aeq = cache.createAsyncEventQueueFactory().setParallel(false)
        .setForwardExpirationDestroy(forwardExpirationDestroy).setBatchSize(1)
        .setBatchTimeInterval(1).create(id, al);
  }

  private Region createRegion(String name, boolean isPR, String aeqId, boolean evictionDestroy,
      boolean evictionOverflow, boolean expirationDestroy, boolean expirationInvalidate) {
    RegionFactory rf;
    if (isPR) {
      rf = cache.createRegionFactory(RegionShortcut.PARTITION);
    } else {
      rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    }
    // Set AsyncQueue.
    rf.addAsyncEventQueueId(aeqId);
    if (evictionDestroy) {
      rf.setEvictionAttributes(
          EvictionAttributes.createLIFOEntryAttributes(1, EvictionAction.LOCAL_DESTROY));
    }
    if (evictionOverflow) {
      rf.setEvictionAttributes(
          EvictionAttributes.createLIFOEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    }
    if (expirationDestroy) {
      rf.setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.DESTROY));
    }
    if (expirationInvalidate) {
      rf.setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.INVALIDATE));
    }

    return rf.create(name);
  }

  private AsyncEventListener createAsyncListener(List<AsyncEvent> list) {
    AsyncEventListener listener = new AsyncEventListener() {
      private List<AsyncEvent> aeList = list;

      @Override
      public void close() {}

      @Override
      public boolean processEvents(List<AsyncEvent> arg0) {
        try {
          synchronized (aeList) {
            aeList.add(arg0.get(0));
          }
        } catch (Exception ex) {
        }
        return true;
      }
    };
    return listener;
  }


}
