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

import static org.apache.geode.cache.wan.GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
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
import org.apache.geode.test.junit.categories.WanTest;

@Category(WanTest.class)
public class SerialGatewaySenderImplTest {

  private InternalCache cache;

  private SerialGatewaySenderImpl serialGatewaySender;
  private GatewaySenderAttributes gatewaySenderAttributes;
  private StatisticsClock statisticsClock;

  AbstractGatewaySenderEventProcessor eventProcessor1;
  AbstractGatewaySenderEventProcessor eventProcessor2;


  @Before
  public void setUp() {
    cache = Fakes.cache();
    when(cache.getRegion(any())).thenReturn(null);
    InternalRegionFactory<Object, Object> regionFactory =
        uncheckedCast(mock(InternalRegionFactory.class));
    when(regionFactory.create(any())).thenReturn(uncheckedCast(mock(LocalRegion.class)));
    when(cache.createInternalRegionFactory(any())).thenReturn(regionFactory);

    StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
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

  private SerialGatewaySenderImpl createSerialGatewaySenderImplSpy() {
    return createSerialGatewaySenderImplSpy(false);
  }

  private SerialGatewaySenderImpl createSerialGatewaySenderImplSpy(
      boolean mustGroupTransactionEvents) {
    GatewaySenderAdvisor gatewaySenderAdvisor = mock(GatewaySenderAdvisor.class);
    when(gatewaySenderAdvisor.isPrimary()).thenReturn(true);

    eventProcessor1 = mock(AbstractGatewaySenderEventProcessor.class);
    eventProcessor2 = mock(AbstractGatewaySenderEventProcessor.class);

    when(eventProcessor1.isStopped()).thenReturn(false);
    when(eventProcessor1.getRunningStateLock()).thenReturn(mock(Object.class));

    when(eventProcessor2.isStopped()).thenReturn(false);
    when(eventProcessor2.getRunningStateLock()).thenReturn(mock(Object.class));

    SerialGatewaySenderImpl serialGatewaySender =
        new SerialGatewaySenderImpl(cache, statisticsClock, gatewaySenderAttributes);
    SerialGatewaySenderImpl spySerialGatewaySender = spy(serialGatewaySender);
    doReturn(gatewaySenderAdvisor).when(spySerialGatewaySender).getSenderAdvisor();
    doReturn(eventProcessor1).when(spySerialGatewaySender).createEventProcessor(false);
    doReturn(eventProcessor2).when(spySerialGatewaySender).createEventProcessor(true);

    doReturn(null).when(spySerialGatewaySender).getQueues();

    if (mustGroupTransactionEvents) {
      doReturn(true).when(spySerialGatewaySender).mustGroupTransactionEvents();
    }

    return spySerialGatewaySender;
  }

  @Test
  public void whenStartedShouldCreateEventProcessor() {
    serialGatewaySender = createSerialGatewaySenderImplSpy();

    serialGatewaySender.start();

    assertThat(serialGatewaySender.getEventProcessor()).isEqualTo(eventProcessor1);
  }

  @Test
  public void whenStartedwithCleanShouldCreateEventProcessor() {
    serialGatewaySender = createSerialGatewaySenderImplSpy();

    serialGatewaySender.startWithCleanQueue();

    assertThat(serialGatewaySender.getEventProcessor()).isEqualTo(eventProcessor2);
  }

  @Test
  public void whenStoppedShouldResetTheEventProcessor() {
    serialGatewaySender = createSerialGatewaySenderImplSpy();

    serialGatewaySender.stop();

    assertThat(serialGatewaySender.getEventProcessor()).isNull();
  }

  @Test
  public void whenStoppedTwiceCloseInTimeWithGroupTransactionEventsPreStopWaitsTwice() {
    serialGatewaySender = createSerialGatewaySenderImplSpy(true);

    long start = System.currentTimeMillis();

    Thread t1 = new Thread(this::stopGatewaySenderAndCheckTime);
    Thread t2 = new Thread(this::stopGatewaySenderAndCheckTime);
    t1.start();
    t2.start();
    try {
      t1.join();
      t2.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    long finish = System.currentTimeMillis();
    long timeElapsed = finish - start;

    // Each call to preStop waits for 1 second but these waits execute in parallel
    assertThat(timeElapsed).isGreaterThan(1000);

    assertThat(serialGatewaySender.getEventProcessor()).isNull();
  }

  private void stopGatewaySenderAndCheckTime() {
    long start = System.currentTimeMillis();
    serialGatewaySender.stop();
    long finish = System.currentTimeMillis();
    long timeElapsed = finish - start;
    assertThat(timeElapsed).isGreaterThan(1000);
  }
}
