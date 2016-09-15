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

package org.apache.geode.internal.statistics.platform;

import org.apache.geode.*;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.statistics.HostStatHelper;
import org.apache.geode.internal.statistics.LocalStatisticsImpl;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * <P>This class provides the interface for statistics about a
 * Mac OS X operating system process that is using a GemFire system.
 */
public class OSXProcessStats
{
//  private final static int imageSizeINT = 0;
//  private final static int rssSizeINT = 1;

  private final static StatisticsType myType;

  private static void checkOffset(String name, int offset) {
    int id = myType.nameToId(name);
    Assert.assertTrue(offset == id, "Expected the offset for " + name + " to be " + offset + " but it was " + id);
  }
  
  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    myType = f.createType("OSXProcessStats",
                          "Statistics on a OS X process.",
                          new StatisticDescriptor[] {
                              f.createIntGauge("dummyStat",
                                            "Placeholder",
                                             "megabytes")
//                              f.createIntGauge("imageSize",
//                                             "The size of the process's image in megabytes.",
//                                             "megabytes"),
//                            f.createIntGauge("rssSize",
//                                             "The size of the process's resident set size in megabytes.",
//                                             "megabytes"),
                          });
//    checkOffset("imageSize", imageSizeINT);
//    checkOffset("rssSize", rssSizeINT);
  }

  private OSXProcessStats() {
    // no instances allowed
  }
  public static StatisticsType getType() {
    return myType;
  }

  /**
   * Returns a <code>ProcessStats</code> that wraps OS X process
   * <code>Statistics</code>. 
   *
   * @since GemFire 3.5
   */
  public static ProcessStats createProcessStats(final Statistics stats) { // TODO: was package-protected
    if (stats instanceof LocalStatisticsImpl) {
      HostStatHelper.refresh((LocalStatisticsImpl) stats);
    } // otherwise its a Dummy implementation so do nothing
    return new ProcessStats(stats) {
      @Override
        public long getProcessSize() {
//          return stats.getInt(rssSizeINT);
            return 0L;
        }
      };
  }

}
