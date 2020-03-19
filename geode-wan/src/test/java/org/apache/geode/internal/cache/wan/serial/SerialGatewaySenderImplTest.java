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

import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.metrics.internal.InternalDistributedSystemMetricsService;
import org.apache.geode.metrics.internal.MetricsService;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.WanTest;


@Category(WanTest.class)
public class SerialGatewaySenderImplTest {

  private InternalCache cache;

  private SerialGatewaySenderImpl serialGatewaySender;
  private GatewaySenderAttributes gatewaySenderAttributes;
  private StatisticsClock statisticsClock;

  @Before
  public void setUp() throws Exception {
    cache = Fakes.cache();
    when(cache.getRegion(any())).thenReturn(null);
    when(cache.createVMRegion(any(), any(), any())).thenReturn(mock(LocalRegion.class));

    gatewaySenderAttributes = mock(GatewaySenderAttributes.class);
    when(gatewaySenderAttributes.getId()).thenReturn("sender");
    when(gatewaySenderAttributes.getRemoteDSId()).thenReturn(DEFAULT_DISTRIBUTED_SYSTEM_ID);
    when(gatewaySenderAttributes.getMaximumQueueMemory()).thenReturn(10);
    when(gatewaySenderAttributes.getDispatcherThreads()).thenReturn(1);
    when(gatewaySenderAttributes.isForInternalUse()).thenReturn(false);

    MetricsService.Builder metricsSessionBuilder =
        new InternalDistributedSystemMetricsService.Builder()
            .setIsClient(true);

    InternalDistributedSystem.connectInternal(null, null, metricsSessionBuilder);
    InternalCacheForClientAccess intCacheFCA = new InternalCacheForClientAccess(cache);
    when(cache.getCacheForProcessingClientRequests()).thenReturn(intCacheFCA);

    statisticsClock = mock(StatisticsClock.class);

    DistributedLockService distributedLockService = mock(DistributedLockService.class);
    when(distributedLockService.lock(any(), anyLong(), anyLong())).thenReturn(true);
    when(cache.getGatewaySenderLockService()).thenReturn(distributedLockService);

    serialGatewaySender =
        new SerialGatewaySenderImpl(cache, statisticsClock,
            gatewaySenderAttributes);
    serialGatewaySender.setIsPrimary(true);

  }

  @Test
  public void whenStartedShouldCreateEventProcessor() {
    serialGatewaySender.start();

    assertThat(serialGatewaySender.getEventProcessor()).isNotNull();
    AbstractGatewaySenderEventProcessor processor = serialGatewaySender.getEventProcessor();
    RegionQueue queue = processor.getQueue();
    assertFalse(((SerialGatewaySenderQueue) queue).getCleanQueues());

  }

  @Test
  public void whenStartWithCleanQueueShouldCreateEventProcessor() {
    serialGatewaySender.startWithCleanQueue();

    assertThat(serialGatewaySender.getEventProcessor()).isNotNull();
    AbstractGatewaySenderEventProcessor processor = serialGatewaySender.getEventProcessor();
    RegionQueue queue = processor.getQueue();
    assertTrue(((SerialGatewaySenderQueue) queue).getCleanQueues());

  }


}
