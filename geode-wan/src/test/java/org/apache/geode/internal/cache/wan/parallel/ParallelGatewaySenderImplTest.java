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

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.test.junit.categories.WanTest;


@Category(WanTest.class)
public class ParallelGatewaySenderImplTest {
  private InternalCache cache;
  private StatisticsClock statisticsClock;
  private GatewaySenderAttributes attrs;
  private ParallelGatewaySenderImpl gatewaysender;

  @Before
  public void setUp() {
    cache = mock(InternalCache.class);
    statisticsClock = mock(StatisticsClock.class);
    attrs = new GatewaySenderAttributes();
    attrs.isParallel = true;
    attrs.id = "sender";
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    Statistics stats = mock(Statistics.class);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(cache.getCancelCriterion()).thenReturn(cancelCriterion);
    when(cancelCriterion.isCancelInProgress()).thenReturn(false);
    ClusterDistributionManager distributionManager = mock(ClusterDistributionManager.class);
    when(system.getDistributionManager()).thenReturn(distributionManager);
    when(system.createAtomicStatistics(any(), any())).thenReturn(stats);
    when(distributionManager.getDistributedSystemId()).thenReturn(-1);

    DistributedLockService distributedLockService = mock(DistributedLockService.class);
    when(distributedLockService.lock(any(), anyLong(), anyLong())).thenReturn(true);

    when(cache.getGatewaySenderLockService()).thenReturn(distributedLockService);

    LocalRegion region = mock(LocalRegion.class);
    when(cache.getRegion(any())).thenReturn(uncheckedCast(region));
    when(region.containsKey(any())).thenReturn(true);
    when(region.get(any())).thenReturn(1);

    TypeRegistry pdxRegistryMock = mock(TypeRegistry.class);
    when(cache.getPdxRegistry()).thenReturn(pdxRegistryMock);

    gatewaysender = new ParallelGatewaySenderImpl(cache, statisticsClock, attrs);
  }

  @Test
  public void testStart() {
    gatewaysender.start();
    RegionQueue queue = gatewaysender.getQueue();
    assertFalse(((ConcurrentParallelGatewaySenderQueue) queue).getCleanQueues());
  }

  @Test
  public void testStartWithCleanQueue() {
    gatewaysender.startWithCleanQueue();
    RegionQueue queue = gatewaysender.getQueue();
    assertTrue(((ConcurrentParallelGatewaySenderQueue) queue).getCleanQueues());
  }

  @Test
  public void whenStoppedTwiceCloseInTimeWithGroupTransactionEventsPreStopWaitsTwice() {
    attrs.groupTransactionEvents = true;
    gatewaysender = new ParallelGatewaySenderImpl(cache, statisticsClock, attrs);
    gatewaysender.start();

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

    assertThat(gatewaysender.isRunning()).isEqualTo(false);
  }

  private void stopGatewaySenderAndCheckTime() {
    long start = System.currentTimeMillis();
    gatewaysender.stop();
    long finish = System.currentTimeMillis();
    long timeElapsed = finish - start;
    assertThat(timeElapsed).isGreaterThan(1000);
  }
}
