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
package org.apache.geode.internal.cache.wan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.RegionQueue;

public class AbstractGatewaySenderTest {

  @Test
  public void getSynchronizationEventCanHandleRegionIsNullCase() {
    AbstractGatewaySender sender = mock(AbstractGatewaySender.class);
    Object key = new Object();
    long timestamp = 1;
    GatewaySenderEventImpl gatewaySenderEvent = mock(GatewaySenderEventImpl.class);
    when(gatewaySenderEvent.getKey()).thenReturn(key);
    when(gatewaySenderEvent.getVersionTimeStamp()).thenReturn(timestamp);
    Region region = mock(Region.class);
    Collection collection = new ArrayList();
    collection.add(gatewaySenderEvent);
    when(region.values()).thenReturn(collection);
    Set<RegionQueue> queues = new HashSet<>();
    RegionQueue queue1 = mock(RegionQueue.class);
    RegionQueue queue2 = mock(RegionQueue.class);
    queues.add(queue2);
    queues.add(queue1);
    when(queue1.getRegion()).thenReturn(null);
    when(queue2.getRegion()).thenReturn(region);
    when(sender.getQueues()).thenReturn(queues);
    when(sender.getStatistics()).thenReturn(mock(GatewaySenderStats.class));
    when(sender.getSynchronizationEvent(key, timestamp)).thenCallRealMethod();

    GatewayQueueEvent event = sender.getSynchronizationEvent(key, timestamp);

    assertThat(event).isSameAs(gatewaySenderEvent);
  }

  @Test
  public void distributeFinishesWorkWhenInterrupted() throws InterruptedException {
    DummyGatewaySenderEventProcessor processor = new DummyGatewaySenderEventProcessor();
    TestableGatewaySender gatewaySender = new TestableGatewaySender(processor);
    EnumListenerEvent operationType = EnumListenerEvent.AFTER_CREATE;
    EntryEventImpl event = mock(EntryEventImpl.class);
    when(event.getKeyInfo()).thenReturn(mock(KeyInfo.class));
    Operation operation = mock(Operation.class);
    when(operation.isLocal()).thenReturn(false);
    when(operation.isExpiration()).thenReturn(false);
    when(event.getOperation()).thenReturn(operation);
    InternalRegion region = mock(InternalRegion.class);
    when(region.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    when(event.getRegion()).thenReturn(region);
    List<Integer> allRemoteDSIds = Collections.singletonList(1);

    CountDownLatch lockAcquiredLatch = new CountDownLatch(1);
    CountDownLatch unlockLatch = new CountDownLatch(1);

    // Get lifeCycleLock in write mode in new thread so that
    // the thread calling distribute will not be able
    // to acquire it
    Thread thread = new Thread(() -> {
      gatewaySender.getLifeCycleLock().writeLock().lock();
      lockAcquiredLatch.countDown();
      try {
        unlockLatch.await();
      } catch (InterruptedException ignore) {
      }
      gatewaySender.getLifeCycleLock().writeLock().unlock();
    });
    thread.start();
    lockAcquiredLatch.await();

    // Send interrupted and then call distribute
    Thread.currentThread().interrupt();
    gatewaySender.distribute(operationType, event, allRemoteDSIds, true);

    unlockLatch.countDown();

    // Check that the interrupted exception has been reset
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    // Check that the work was finished even if the interrupt signal was set
    assertThat(processor.getTimesRegisterEventDroppedInPrimaryQueueCalled()).isEqualTo(1);
  }

  public static class TestableGatewaySender extends AbstractGatewaySender {
    private int isRunningTimesCalled = 0;

    public TestableGatewaySender(AbstractGatewaySenderEventProcessor eventProcessor) {
      this.eventProcessor = eventProcessor;
      enqueuedAllTempQueueEvents = true;
    }

    @Override
    public void fillInProfile(DistributionAdvisor.Profile profile) {}

    @Override
    public void start() {}

    @Override
    public boolean isPrimary() {
      return true;
    }

    @Override
    public void startWithCleanQueue() {}

    @Override
    public void prepareForStop() {}

    @Override
    public void stop() {}

    @Override
    public void setModifiedEventId(EntryEventImpl clonedEvent) {}

    @Override
    public GatewaySenderStats getStatistics() {
      return mock(GatewaySenderStats.class);
    }

    @Override
    public GatewaySenderAdvisor getSenderAdvisor() {
      return mock(GatewaySenderAdvisor.class);
    }

    @Override
    public boolean isRunning() {
      if (isRunningTimesCalled++ == 0) {
        return true;
      }
      return false;
    }

    @Override
    public String getId() {
      return "test";
    }
  }

  public static class DummyGatewaySenderEventProcessor extends AbstractGatewaySenderEventProcessor {

    private int timesEnqueueEventCalled = 0;
    private int timesRegisterEventDroppedInPrimaryQueueCalled = 0;

    public DummyGatewaySenderEventProcessor() {
      super("", new DummyGatewaySender(), null);
    }

    @Override
    public void enqueueEvent(EnumListenerEvent operation, EntryEvent event, Object substituteValue,
        boolean isLastEventInTransaction) throws IOException, CacheException {
      timesEnqueueEventCalled++;
    }

    public int getTimesEnqueueEventCalled() {
      return timesEnqueueEventCalled;
    }

    @Override
    protected void initializeMessageQueue(String id, boolean cleanQueues) {}

    @Override
    protected void rebalance() {}

    public int getTimesRegisterEventDroppedInPrimaryQueueCalled() {
      return timesRegisterEventDroppedInPrimaryQueueCalled;
    }

    @Override
    protected void registerEventDroppedInPrimaryQueue(EntryEventImpl droppedEvent) {
      timesRegisterEventDroppedInPrimaryQueueCalled++;
    }

    @Override
    public void initializeEventDispatcher() {}

    @Override
    protected void enqueueEvent(GatewayQueueEvent event) {}
  }

  public static class DummyGatewaySender extends AbstractGatewaySender {
    @Override
    public void fillInProfile(DistributionAdvisor.Profile profile) {}

    @Override
    public void start() {}

    @Override
    public void startWithCleanQueue() {}

    @Override
    public void prepareForStop() {}

    @Override
    public void stop() {}

    @Override
    public void setModifiedEventId(EntryEventImpl clonedEvent) {}

  }
}
