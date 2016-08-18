/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.statistics;

import com.gemstone.gemfire.*;

/**
 * Statistics related to a Java VM. Currently they all come from
 * {@link java.lang.Runtime}.
 */
public class VMStats implements VMStatsContract {
  private final static StatisticsType vmType;
  private final static int cpusId;
  private final static int freeMemoryId;
  private final static int totalMemoryId;
  private final static int maxMemoryId;
  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    vmType = f.createType("VMStats",
                          "Stats available on any java virtual machine.",
                          new StatisticDescriptor[] {
                            f.createIntGauge("cpus",
                                             "Number of cpus available to the java VM on its machine.",
                                             "cpus", true),
                            f.createLongGauge("freeMemory",
                                              "An approximation fo the total amount of memory currently available for future allocated objects, measured in bytes.",
                                              "bytes", true),
                            f.createLongGauge("totalMemory",
                                              "The total amount of memory currently available for current and future objects, measured in bytes.",
                                              "bytes"),
                            f.createLongGauge("maxMemory",
                                              "The maximum amount of memory that the VM will attempt to use, measured in bytes.",
                                              "bytes", true)
                          });
    cpusId = vmType.nameToId("cpus");
    freeMemoryId = vmType.nameToId("freeMemory");
    totalMemoryId = vmType.nameToId("totalMemory");
    maxMemoryId = vmType.nameToId("maxMemory");
  }
  
  private final Statistics vmStats;


  public VMStats(StatisticsFactory f, long id) {
    this.vmStats = f.createStatistics(vmType, "vmStats", id);
  }

  public void refresh() {
    Runtime rt = Runtime.getRuntime();
    this.vmStats.setInt(cpusId, rt.availableProcessors());
    this.vmStats.setLong(freeMemoryId, rt.freeMemory());
    this.vmStats.setLong(totalMemoryId, rt.totalMemory());
    this.vmStats.setLong(maxMemoryId, rt.maxMemory()); 
    
  }
  public void close() {
    this.vmStats.close();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.statistics.VMStatsContract#getFdsOpen()
   */
  public long getFdsOpen() {
    return -1;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.statistics.VMStatsContract#getFdLimit()
   */
  public long getFdLimit() {
    return 0;
  }
}
