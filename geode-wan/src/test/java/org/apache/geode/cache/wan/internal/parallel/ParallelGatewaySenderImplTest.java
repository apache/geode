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
package org.apache.geode.cache.wan.internal.parallel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
    attrs.setParallel(true);
    attrs.setId("sender");
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(cache.getDistributedSystem()).thenReturn(system);
    ClusterDistributionManager distributionManager = mock(ClusterDistributionManager.class);
    when(system.getDistributionManager()).thenReturn(distributionManager);
    when(distributionManager.getDistributedSystemId()).thenReturn(-1);

    DistributedLockService distributedLockService = mock(DistributedLockService.class);
    when(distributedLockService.lock(any(), anyLong(), anyLong())).thenReturn(true);

    when(cache.getGatewaySenderLockService()).thenReturn(distributedLockService);

    LocalRegion region = mock(LocalRegion.class);
    when(cache.getRegion(any())).thenReturn(region);
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
}
