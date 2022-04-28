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

import java.lang.management.BufferPoolMXBean;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;

/**
 * Polls Java platform buffer pool statistics from {@link BufferPoolMXBean}.
 */
public class BufferPoolStats {

  private final StatisticsType bufferPoolType;
  private final int bufferPoolCountId;
  private final int bufferPoolTotalCapacityId;
  private final int bufferPoolMemoryUsedId;

  private final Map<BufferPoolMXBean, Statistics> bufferPoolStatistics =
      new IdentityHashMap<>();

  BufferPoolStats(final @NotNull StatisticsTypeFactory typeFactory) {
    bufferPoolType =
        typeFactory.createType("PlatformBufferPoolStats", "Java platform buffer pools.",
            new StatisticDescriptor[] {
                typeFactory.createLongGauge("count",
                    "An estimate of the number of buffers in this pool.", "buffers"),
                typeFactory.createLongGauge("totalCapacity",
                    "An estimate of the total capacity of the buffers in this pool in bytes.",
                    "bytes"),
                typeFactory.createLongGauge("memoryUsed",
                    "An estimate of the memory that the Java virtual machine is using for this buffer pool in bytes, or -1L if an estimate of the memory usage is not available.",
                    "bytes")});
    bufferPoolCountId = bufferPoolType.nameToId("count");
    bufferPoolTotalCapacityId = bufferPoolType.nameToId("totalCapacity");
    bufferPoolMemoryUsedId = bufferPoolType.nameToId("memoryUsed");
  }

  void init(final @NotNull StatisticsFactory statisticsFactory, final long id) {
    init(statisticsFactory, id, getPlatformMXBeans(BufferPoolMXBean.class));
  }

  void init(final StatisticsFactory statisticsFactory, final long id,
      final List<BufferPoolMXBean> platformMXBeans) {
    platformMXBeans.forEach(
        pool -> bufferPoolStatistics.computeIfAbsent(pool,
            k -> statisticsFactory.createStatistics(bufferPoolType, k.getName() + " buffer pool",
                id)));
  }

  void refresh() {
    bufferPoolStatistics.forEach((bufferPool, statistics) -> {
      statistics.setLong(bufferPoolCountId, bufferPool.getCount());
      statistics.setLong(bufferPoolTotalCapacityId, bufferPool.getTotalCapacity());
      statistics.setLong(bufferPoolMemoryUsedId, bufferPool.getMemoryUsed());
    });
  }

  public void close() {
    bufferPoolStatistics.values().forEach(Statistics::close);
  }
}
