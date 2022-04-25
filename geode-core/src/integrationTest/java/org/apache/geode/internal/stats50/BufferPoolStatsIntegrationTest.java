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

package org.apache.geode.internal.stats50;

import static java.lang.management.ManagementFactory.getPlatformMXBeans;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.management.BufferPoolMXBean;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;

class BufferPoolStatsIntegrationTest {

  @Test
  void refreshAfterDirectBufferAllocationChangesStatistics() {
    assumeThat(getPlatformMXBeans(BufferPoolMXBean.class))
        .anySatisfy(p -> assertThat(p.getName()).contains("direct"));

    final StatisticsTypeFactory statisticsTypeFactory = mock(StatisticsTypeFactory.class);
    final StatisticsType statisticsType = mock(StatisticsType.class);
    when(statisticsTypeFactory.createType(anyString(), anyString(), any()))
        .thenReturn(statisticsType);
    when(statisticsType.nameToId(eq("count"))).thenReturn(0);
    when(statisticsType.nameToId(eq("totalCapacity"))).thenReturn(1);
    when(statisticsType.nameToId(eq("memoryUsed"))).thenReturn(2);
    final StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
    final Statistics statistics = mock(Statistics.class);
    when(statisticsFactory.createStatistics(any(), contains("direct"), anyLong()))
        .thenReturn(statistics);

    final BufferPoolStats bufferPoolStats = new BufferPoolStats(statisticsTypeFactory);
    bufferPoolStats.init(statisticsFactory, 42);

    bufferPoolStats.refresh();

    final ArgumentCaptor<Long> count = ArgumentCaptor.forClass(Long.class);
    verify(statistics).setLong(eq(0), count.capture());
    final ArgumentCaptor<Long> totalCapacity = ArgumentCaptor.forClass(Long.class);
    verify(statistics).setLong(eq(1), totalCapacity.capture());
    final ArgumentCaptor<Long> memoryUsed = ArgumentCaptor.forClass(Long.class);
    verify(statistics).setLong(eq(2), memoryUsed.capture());

    clearInvocations(statistics);

    final ByteBuffer directBuffer = ByteBuffer.allocateDirect(1000);

    bufferPoolStats.refresh();

    final ArgumentCaptor<Long> countAfterAllocate = ArgumentCaptor.forClass(Long.class);
    verify(statistics).setLong(eq(0), countAfterAllocate.capture());
    final ArgumentCaptor<Long> totalCapacityAfterAllocate = ArgumentCaptor.forClass(Long.class);
    verify(statistics).setLong(eq(1), totalCapacityAfterAllocate.capture());
    final ArgumentCaptor<Long> memoryUsedAfterAllocate = ArgumentCaptor.forClass(Long.class);
    verify(statistics).setLong(eq(2), memoryUsedAfterAllocate.capture());

    assertThat(countAfterAllocate.getValue()).isGreaterThan(count.getValue());
    assertThat(totalCapacityAfterAllocate.getValue()).isGreaterThan(totalCapacity.getValue());
    assertThat(memoryUsedAfterAllocate.getValue()).isGreaterThan(memoryUsed.getValue());

    // Used only to prevent GC of directBuffer during test
    assertThat(directBuffer).isNotNull();
  }


}
