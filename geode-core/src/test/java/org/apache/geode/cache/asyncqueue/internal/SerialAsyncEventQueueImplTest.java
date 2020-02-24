/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache.asyncqueue.internal;

import static org.apache.geode.cache.wan.GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderAdvisor;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.AEQTest;

/**
 * Extracted from AsyncEventListenerDistributedTest.
 */
@Category(AEQTest.class)
public class SerialAsyncEventQueueImplTest {

  private InternalCache cache;

  private SerialAsyncEventQueueImpl serialAsyncEventQueue;
  private StatisticsFactory statisticsFactory;
  private GatewaySenderAttributes gatewaySenderAttributes;
  private StatisticsClock statisticsClock;
  private InternalRegionFactory regionFactory;

  @Before
  public void setUp() throws Exception {
    cache = Fakes.cache();
    when(cache.getRegion(any())).thenReturn(null);
    regionFactory = mock(InternalRegionFactory.class);
    when(regionFactory.create(any())).thenReturn(mock(LocalRegion.class));
    when(cache.createInternalRegionFactory(any())).thenReturn(regionFactory);

    statisticsFactory = mock(StatisticsFactory.class);
    when(statisticsFactory.createAtomicStatistics(any(), any())).thenReturn(mock(Statistics.class));

    gatewaySenderAttributes = mock(GatewaySenderAttributes.class);
    when(gatewaySenderAttributes.getId()).thenReturn("sender");
    when(gatewaySenderAttributes.getRemoteDSId()).thenReturn(DEFAULT_DISTRIBUTED_SYSTEM_ID);
    when(gatewaySenderAttributes.getMaximumQueueMemory()).thenReturn(10);
    when(gatewaySenderAttributes.getDispatcherThreads()).thenReturn(1);
    when(gatewaySenderAttributes.isForInternalUse()).thenReturn(false);

    statisticsClock = mock(StatisticsClock.class);

    DistributedLockService distributedLockService = mock(DistributedLockService.class);
    when(distributedLockService.lock(any(), anyLong(), anyLong())).thenReturn(true);
    when(cache.getGatewaySenderLockService()).thenReturn(distributedLockService);
  }

  private SerialAsyncEventQueueImpl createSerialAsyncEventQueueImplSpy() {
    GatewaySenderAdvisor gatewaySenderAdvisor = mock(GatewaySenderAdvisor.class);
    when(gatewaySenderAdvisor.isPrimary()).thenReturn(true);

    AbstractGatewaySenderEventProcessor eventProcessor =
        mock(AbstractGatewaySenderEventProcessor.class);
    when(eventProcessor.isStopped()).thenReturn(false);
    when(eventProcessor.getRunningStateLock()).thenReturn(mock(Object.class));

    SerialAsyncEventQueueImpl serialAsyncEventQueue =
        new SerialAsyncEventQueueImpl(cache, statisticsFactory, statisticsClock,
            gatewaySenderAttributes);
    SerialAsyncEventQueueImpl spySerialAsyncEventQueue = spy(serialAsyncEventQueue);
    doReturn(gatewaySenderAdvisor).when(spySerialAsyncEventQueue).getSenderAdvisor();
    doReturn(eventProcessor).when(spySerialAsyncEventQueue).createEventProcessor();
    doReturn(null).when(spySerialAsyncEventQueue).getQueues();

    return spySerialAsyncEventQueue;
  }

  @Test
  public void whenStartedShouldCreateEventProcessor() {
    serialAsyncEventQueue = createSerialAsyncEventQueueImplSpy();

    serialAsyncEventQueue.start();

    assertThat(serialAsyncEventQueue.getEventProcessor()).isNotNull();
  }

  @Test
  public void whenStoppedShouldResetTheEventProcessor() {
    serialAsyncEventQueue = createSerialAsyncEventQueueImplSpy();

    serialAsyncEventQueue.stop();

    assertThat(serialAsyncEventQueue.getEventProcessor()).isNull();
  }

}
