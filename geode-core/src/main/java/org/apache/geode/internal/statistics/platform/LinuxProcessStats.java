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

package org.apache.geode.internal.statistics.platform;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * <P>
 * This class provides the interface for statistics about a Linux operating system process that is
 * using a GemFire system.
 */
public class LinuxProcessStats {
  static final int imageSizeLONG;
  static final int rssSizeLONG;

  @Immutable
  private static final StatisticsType myType;

  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    myType = f.createType("LinuxProcessStats", "Statistics on a Linux process.",
        new StatisticDescriptor[] {
            f.createLongGauge("imageSize", "The size of the process's image in megabytes.",
                "megabytes"),
            f.createLongGauge("rssSize",
                "The size of the process's resident set size in megabytes. (assumes PAGESIZE=4096, specify -Dgemfire.statistics.linux.pageSize=<pagesize> to adjust)",
                "megabytes"),});
    imageSizeLONG = myType.nameToId("imageSize");
    rssSizeLONG = myType.nameToId("rssSize");
  }

  private LinuxProcessStats() {
    // no instances allowed
  }

  public static StatisticsType getType() {
    return myType;
  }

  /**
   * Returns a <code>ProcessStats</code> that wraps Linux process <code>Statistics</code>.
   *
   * @since GemFire 3.5
   */
  public static ProcessStats createProcessStats(final Statistics stats) {
    return new ProcessStats(stats) {
      @Override
      public long getProcessSize() {
        return stats.getLong(rssSizeLONG);
      }
    };
  }

}
