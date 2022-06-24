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
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.test.junit.categories.AEQTest;

@Category(AEQTest.class)
public class ParallelAsyncEventQueueImplTest {
  private ParallelAsyncEventQueueImpl asyncEventQueue;

  @Before
  public void setUp() {
    InternalCache cache = mock(InternalCache.class, RETURNS_DEEP_STUBS);
    StatisticsClock statisticsClock = mock(StatisticsClock.class);
    StatisticsFactory statsFactory = mock(StatisticsFactory.class);
    GatewaySenderAttributes attrs = mock(GatewaySenderAttributes.class);
    when(attrs.isParallel()).thenReturn(true);
    when(attrs.getId()).thenReturn("AsyncEventQueue_");
    when(attrs.getDispatcherThreads()).thenReturn(1);
    when(attrs.getRemoteDSId()).thenReturn(DEFAULT_DISTRIBUTED_SYSTEM_ID);

    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(cache.getCancelCriterion().isCancelInProgress()).thenReturn(false);
    ClusterDistributionManager distributionManager = mock(ClusterDistributionManager.class);
    when(system.getDistributionManager()).thenReturn(distributionManager);
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

    asyncEventQueue = new ParallelAsyncEventQueueImpl(cache, statsFactory, statisticsClock, attrs);
  }

  @Test
  public void testStart() {
    asyncEventQueue.start();
    RegionQueue queue = asyncEventQueue.getQueue();
    assertFalse(((ConcurrentParallelGatewaySenderQueue) queue).getCleanQueues());
  }

  @Test
  public void testStartWithCleanQueue() {
    asyncEventQueue.startWithCleanQueue();
    RegionQueue queue = asyncEventQueue.getQueue();
    assertTrue(((ConcurrentParallelGatewaySenderQueue) queue).getCleanQueues());
  }

}
