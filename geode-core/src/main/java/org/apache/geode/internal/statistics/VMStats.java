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
package org.apache.geode.internal.statistics;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;

/**
 * Statistics related to a Java VM. Currently they all come from {@link java.lang.Runtime}.
 */
public class VMStats implements VMStatsContract {
  @Immutable
  private static final StatisticsType vmType;
  private static final int cpusId;
  private static final int freeMemoryId;
  private static final int totalMemoryId;
  private static final int maxMemoryId;
  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    vmType = f.createType("VMStats", "Stats available on any java virtual machine.",
        new StatisticDescriptor[] {
            f.createIntGauge("cpus", "Number of cpus available to the java VM on its machine.",
                "cpus", true),
            f.createLongGauge("freeMemory",
                "An approximation of the total amount of memory currently available for future allocated objects, measured in bytes.",
                "bytes", true),
            f.createLongGauge("totalMemory",
                "The total amount of memory currently available for current and future objects, measured in bytes.",
                "bytes"),
            f.createLongGauge("maxMemory",
                "The maximum amount of memory that the VM will attempt to use, measured in bytes.",
                "bytes", true)});
    cpusId = vmType.nameToId("cpus");
    freeMemoryId = vmType.nameToId("freeMemory");
    totalMemoryId = vmType.nameToId("totalMemory");
    maxMemoryId = vmType.nameToId("maxMemory");
  }

  private final Statistics vmStats;


  public VMStats(StatisticsFactory f, long id) {
    vmStats = f.createStatistics(vmType, "vmStats", id);
  }

  @Override
  public void refresh() {
    Runtime rt = Runtime.getRuntime();
    vmStats.setInt(cpusId, rt.availableProcessors());
    vmStats.setLong(freeMemoryId, rt.freeMemory());
    vmStats.setLong(totalMemoryId, rt.totalMemory());
    vmStats.setLong(maxMemoryId, rt.maxMemory());

  }

  @Override
  public void close() {
    vmStats.close();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.statistics.VMStatsContract#getFdsOpen()
   */
  public long getFdsOpen() {
    return -1;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.statistics.VMStatsContract#getFdLimit()
   */
  public long getFdLimit() {
    return 0;
  }
}
