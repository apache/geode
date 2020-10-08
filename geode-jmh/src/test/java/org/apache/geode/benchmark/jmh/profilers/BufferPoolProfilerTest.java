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

package org.apache.geode.benchmark.jmh.profilers;

import static java.lang.management.ManagementFactory.getPlatformMXBeans;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.lang.management.BufferPoolMXBean;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.openjdk.jmh.results.Result;

public class BufferPoolProfilerTest {
  private final BufferPoolMXBean mockBufferPool1 = mock(BufferPoolMXBean.class);
  private final BufferPoolMXBean mockBufferPool2 = mock(BufferPoolMXBean.class);
  private final BufferPoolProfiler bufferPoolProfiler =
      new BufferPoolProfiler(asList(mockBufferPool1, mockBufferPool2));

  @Before
  public void before() {
    when(mockBufferPool1.getName()).thenReturn("Mock 1");
    when(mockBufferPool1.getCount()).thenReturn(0L);
    when(mockBufferPool1.getMemoryUsed()).thenReturn(0L);
    when(mockBufferPool1.getTotalCapacity()).thenReturn(0L);
    when(mockBufferPool2.getName()).thenReturn("Mock 2");
    when(mockBufferPool2.getCount()).thenReturn(1L, 2L);
    when(mockBufferPool2.getMemoryUsed()).thenReturn(1L, 3L);
    when(mockBufferPool2.getTotalCapacity()).thenReturn(1L, 4L);
  }

  @Test
  public void defaultConstructorGetsBufferPoolsFromPlatformMXBeans() {
    assertThat(new BufferPoolProfiler().pools)
        .isEqualTo(getPlatformMXBeans(BufferPoolMXBean.class));
  }

  @Test
  public void beforeIteration() {
    bufferPoolProfiler.beforeIteration(null, null);

    verify(mockBufferPool1).getCount();
    verify(mockBufferPool1).getMemoryUsed();
    verify(mockBufferPool1).getTotalCapacity();
    verify(mockBufferPool2).getCount();
    verify(mockBufferPool2).getMemoryUsed();
    verify(mockBufferPool2).getTotalCapacity();
    verifyNoMoreInteractions(mockBufferPool1, mockBufferPool2);
  }

  @Test
  public void afterIteration() {
    bufferPoolProfiler.beforeIteration(null, null);
    final Collection<? extends Result> results =
        bufferPoolProfiler.afterIteration(null, null, null);

    assertThat(results).hasSize(3)
        .anySatisfy(result -> {
          assertThat(result.getLabel()).isEqualTo("Mock 2.count");
          assertThat(result.getStatistics().getMean()).isEqualTo(1.0);
        })
        .anySatisfy(result -> {
          assertThat(result.getLabel()).isEqualTo("Mock 2.memoryUsed");
          assertThat(result.getStatistics().getMean()).isEqualTo(2.0);
        })
        .anySatisfy(result -> {
          assertThat(result.getLabel()).isEqualTo("Mock 2.totalCapacity");
          assertThat(result.getStatistics().getMean()).isEqualTo(3.0);
        });

    verify(mockBufferPool1, times(2)).getCount();
    verify(mockBufferPool1).getMemoryUsed();
    verify(mockBufferPool1).getTotalCapacity();
    verify(mockBufferPool2).getName();
    verify(mockBufferPool2, times(2)).getCount();
    verify(mockBufferPool2, times(2)).getMemoryUsed();
    verify(mockBufferPool2, times(2)).getTotalCapacity();
    verifyNoMoreInteractions(mockBufferPool1, mockBufferPool2);
  }
}
