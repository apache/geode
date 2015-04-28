/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal;

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
   * @see com.gemstone.gemfire.internal.VMStatsContract#getFdsOpen()
   */
  public long getFdsOpen() {
    return -1;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.VMStatsContract#getFdLimit()
   */
  public long getFdLimit() {
    return 0;
  }
}
