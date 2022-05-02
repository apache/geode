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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;

/**
 * Polls Java platform buffer pool statistics from {@link BufferPoolMXBean}.
 */
public class BufferPoolStats {

  private final StatisticsType bufferPoolType;
  private final int bufferPoolCountId;
  private final int bufferPoolTotalCapacityId;
  private final int bufferPoolMemoryUsedId;

  private final List<BufferPoolMXBeanStatistics> bufferPoolStatistics;

  BufferPoolStats(final @NotNull StatisticsFactory statisticsFactory, final long id) {
    this(statisticsFactory, id, getPlatformMXBeans(BufferPoolMXBean.class));
  }

  BufferPoolStats(final @NotNull StatisticsFactory statisticsFactory, final long id,
      final List<BufferPoolMXBean> platformMXBeans) {
    bufferPoolType =
        statisticsFactory.createType("PlatformBufferPoolStats", "Java platform buffer pools.",
            new StatisticDescriptor[] {
                statisticsFactory.createLongGauge("count",
                    "An estimate of the number of buffers in this pool.", "buffers"),
                statisticsFactory.createLongGauge("totalCapacity",
                    "An estimate of the total capacity of the buffers in this pool in bytes.",
                    "bytes"),
                statisticsFactory.createLongGauge("memoryUsed",
                    "An estimate of the memory that the Java virtual machine is using for this buffer pool in bytes, or -1L if an estimate of the memory usage is not available.",
                    "bytes")});
    bufferPoolCountId = bufferPoolType.nameToId("count");
    bufferPoolTotalCapacityId = bufferPoolType.nameToId("totalCapacity");
    bufferPoolMemoryUsedId = bufferPoolType.nameToId("memoryUsed");

    ArrayList<BufferPoolMXBeanStatistics> statList = new ArrayList<>(platformMXBeans.size());
    platformMXBeans
        .forEach(bean -> statList.add(new BufferPoolMXBeanStatistics(bean, statisticsFactory, id)));
    bufferPoolStatistics = Collections.unmodifiableList(statList);
  }

  void refresh() {
    bufferPoolStatistics.forEach(BufferPoolMXBeanStatistics::refresh);
  }

  public void close() {
    bufferPoolStatistics.forEach(BufferPoolMXBeanStatistics::close);
  }

  private class BufferPoolMXBeanStatistics {
    private final BufferPoolMXBean bean;
    private final Statistics statistics;

    BufferPoolMXBeanStatistics(final BufferPoolMXBean bean,
        final StatisticsFactory statisticsFactory, final long id) {
      this.bean = bean;
      this.statistics =
          statisticsFactory.createStatistics(bufferPoolType, bean.getName() + "BufferPool", id);
    }

    void refresh() {
      statistics.setLong(bufferPoolCountId, bean.getCount());
      statistics.setLong(bufferPoolTotalCapacityId, bean.getTotalCapacity());
      statistics.setLong(bufferPoolMemoryUsedId, bean.getMemoryUsed());
    }

    void close() {
      statistics.close();
    }
  }
}
