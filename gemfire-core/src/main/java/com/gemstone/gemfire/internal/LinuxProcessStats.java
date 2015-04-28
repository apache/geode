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
 * <P>This class provides the interface for statistics about a
 * Linux operating system process that is using a GemFire system.
 */
public class LinuxProcessStats
{
  final static int imageSizeINT = 0;
  final static int rssSizeINT = 1;

  private final static StatisticsType myType;

  private static void checkOffset(String name, int offset) {
    int id = myType.nameToId(name);
    Assert.assertTrue(offset == id, "Expected the offset for " + name + " to be " + offset + " but it was " + id);
  }
  
  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    myType = f.createType("LinuxProcessStats",
                          "Statistics on a Linux process.",
                          new StatisticDescriptor[] {
                            f.createIntGauge("imageSize",
                                             "The size of the process's image in megabytes.",
                                             "megabytes"),
                            f.createIntGauge("rssSize",
                                             "The size of the process's resident set size in megabytes. (assumes PAGESIZE=4096, specify -Dgemfire.statistics.linux.pageSize=<pagesize> to adjust)",
                                             "megabytes"),
                          });
    checkOffset("imageSize", imageSizeINT);
    checkOffset("rssSize", rssSizeINT);
  }

  private LinuxProcessStats() {
    // no instances allowed
  }
  public static StatisticsType getType() {
    return myType;
  }

  /**
   * Returns a <code>ProcessStats</code> that wraps Linux process
   * <code>Statistics</code>. 
   *
   * @since 3.5
   */
  static ProcessStats createProcessStats(final Statistics stats) {
    if (stats instanceof LocalStatisticsImpl) {
      HostStatHelper.refresh((LocalStatisticsImpl) stats);
    } // otherwise its a Dummy implementation so do nothing
    return new ProcessStats(stats) {
      @Override
        public long getProcessSize() {
          return stats.getInt(rssSizeINT);
        }
      };
  }

}
