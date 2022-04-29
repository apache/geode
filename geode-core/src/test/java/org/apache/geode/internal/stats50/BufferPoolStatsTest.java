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

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.lang.management.BufferPoolMXBean;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;

class BufferPoolStatsTest {

  @Test
  void constructorCreatesStatisticsType() {
    final StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
    when(statisticsFactory.createType(anyString(), anyString(), any()))
        .thenReturn(mock(StatisticsType.class));

    new BufferPoolStats(statisticsFactory, 42, Collections.emptyList());

    verify(statisticsFactory).createType(eq("PlatformBufferPoolStats"), anyString(), any());
    verify(statisticsFactory).createLongGauge(eq("count"), anyString(), eq("buffers"));
    verify(statisticsFactory).createLongGauge(eq("totalCapacity"), anyString(), eq("bytes"));
    verify(statisticsFactory).createLongGauge(eq("memoryUsed"), anyString(), eq("bytes"));
    verifyNoMoreInteractions(statisticsFactory);
  }

  @Test
  void constructorCreatesStatistics() {
    final StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
    final StatisticsType statisticsType = mock(StatisticsType.class);
    when(statisticsFactory.createType(anyString(), anyString(), any()))
        .thenReturn(statisticsType);
    when(statisticsFactory.createStatistics(any(), anyString(), anyLong()))
        .thenReturn(mock(Statistics.class), mock(Statistics.class));
    final BufferPoolMXBean bufferPoolMXBean1 = mock(BufferPoolMXBean.class);
    when(bufferPoolMXBean1.getName()).thenReturn("mocked1");
    final BufferPoolMXBean bufferPoolMXBean2 = mock(BufferPoolMXBean.class);
    when(bufferPoolMXBean2.getName()).thenReturn("mocked2");
    final List<BufferPoolMXBean> platformMXBeans = asList(bufferPoolMXBean1, bufferPoolMXBean2);

    final BufferPoolStats bufferPoolStats =
        new BufferPoolStats(statisticsFactory, 42, platformMXBeans);

    verify(statisticsFactory).createType(eq("PlatformBufferPoolStats"), anyString(), any());
    verify(statisticsFactory).createLongGauge(eq("count"), anyString(), eq("buffers"));
    verify(statisticsFactory).createLongGauge(eq("totalCapacity"), anyString(), eq("bytes"));
    verify(statisticsFactory).createLongGauge(eq("memoryUsed"), anyString(), eq("bytes"));
    verify(bufferPoolMXBean1).getName();
    verify(bufferPoolMXBean2).getName();
    verify(statisticsFactory).createStatistics(same(statisticsType), contains("mocked1"), eq(42L));
    verify(statisticsFactory).createStatistics(same(statisticsType), contains("mocked2"), eq(42L));
    verifyNoMoreInteractions(bufferPoolMXBean1, bufferPoolMXBean2, statisticsFactory);
  }

  @Test
  void refreshUpdatesStatistics() {
    final StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
    when(statisticsFactory.createType(anyString(), anyString(), any()))
        .thenReturn(mock(StatisticsType.class));
    final Statistics statistics1 = mock(Statistics.class);
    final Statistics statistics2 = mock(Statistics.class);
    when(statisticsFactory.createStatistics(any(), anyString(), anyLong()))
        .thenReturn(statistics1, statistics2);
    final BufferPoolMXBean bufferPoolMXBean1 = mock(BufferPoolMXBean.class);
    when(bufferPoolMXBean1.getName()).thenReturn("mocked1");
    when(bufferPoolMXBean1.getCount()).thenReturn(1200L);
    when(bufferPoolMXBean1.getTotalCapacity()).thenReturn(2400L);
    when(bufferPoolMXBean1.getMemoryUsed()).thenReturn(9600L);
    final BufferPoolMXBean bufferPoolMXBean2 = mock(BufferPoolMXBean.class);
    when(bufferPoolMXBean2.getName()).thenReturn("mocked2");
    when(bufferPoolMXBean2.getCount()).thenReturn(1L);
    when(bufferPoolMXBean2.getTotalCapacity()).thenReturn(2L);
    when(bufferPoolMXBean2.getMemoryUsed()).thenReturn(3L);
    final List<BufferPoolMXBean> platformMXBeans = asList(bufferPoolMXBean1, bufferPoolMXBean2);

    final BufferPoolStats bufferPoolStats =
        new BufferPoolStats(statisticsFactory, 42, platformMXBeans);
    bufferPoolStats.refresh();

    verify(bufferPoolMXBean1).getName();
    verify(bufferPoolMXBean2).getName();
    verify(bufferPoolMXBean1).getCount();
    verify(bufferPoolMXBean1).getTotalCapacity();
    verify(bufferPoolMXBean1).getMemoryUsed();
    verify(bufferPoolMXBean2).getCount();
    verify(bufferPoolMXBean2).getTotalCapacity();
    verify(bufferPoolMXBean2).getMemoryUsed();
    verify(statistics1).setLong(anyInt(), eq(1200L));
    verify(statistics1).setLong(anyInt(), eq(2400L));
    verify(statistics1).setLong(anyInt(), eq(9600L));
    verify(statistics2).setLong(anyInt(), eq(1L));
    verify(statistics2).setLong(anyInt(), eq(2L));
    verify(statistics2).setLong(anyInt(), eq(3L));
    verifyNoMoreInteractions(bufferPoolMXBean1, bufferPoolMXBean2, statistics1, statistics2);
  }

  @Test
  void closeClosesAllStatistics() {
    final StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
    when(statisticsFactory.createType(anyString(), anyString(), any()))
        .thenReturn(mock(StatisticsType.class));
    final Statistics statistics1 = mock(Statistics.class);
    final Statistics statistics2 = mock(Statistics.class);
    when(statisticsFactory.createStatistics(any(), anyString(), anyLong()))
        .thenReturn(statistics1, statistics2);
    final List<BufferPoolMXBean> platformMXBeans =
        asList(mock(BufferPoolMXBean.class), mock(BufferPoolMXBean.class));

    final BufferPoolStats bufferPoolStats =
        new BufferPoolStats(statisticsFactory, 42, platformMXBeans);
    bufferPoolStats.close();

    verify(statistics1).close();
    verify(statistics2).close();
    verifyNoMoreInteractions(statistics1, statistics2);
  }

}
