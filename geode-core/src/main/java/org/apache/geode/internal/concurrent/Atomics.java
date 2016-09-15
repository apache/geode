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

package org.apache.geode.internal.concurrent;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.statistics.LocalStatisticsImpl;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.internal.statistics.StatisticsTypeImpl;
import org.apache.geode.internal.stats50.Atomic50StatisticsImpl;

import java.util.concurrent.atomic.AtomicLong;

public class Atomics {
  private Atomics() { }
  
  /**
   * Whether per-thread stats are used.  Striping is disabled for the
   * IBM JVM due to bug 38226
   */
  private static final boolean STRIPED_STATS_DISABLED = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "STRIPED_STATS_DISABLED")
    || "IBM Corporation".equals(System.getProperty("java.vm.vendor", "unknown"));

  
  public static Statistics createAtomicStatistics(StatisticsType type, String textId,
      long nId, long uId, StatisticsManager mgr) {
    Statistics result = null;
    if (((StatisticsTypeImpl) type).getDoubleStatCount() == 0
        && !STRIPED_STATS_DISABLED) {
      result = new Atomic50StatisticsImpl(type, textId, nId, uId, mgr);
    } else {
      result = new LocalStatisticsImpl(type, textId, nId, uId, true, 0, mgr);
    }
    return result;
  }
  
  /**
   * Use it only when threads are doing incremental updates. If updates are random 
   * then this method may not be optimal.
   */
  public static boolean setIfGreater(AtomicLong atom, long update) {
    while (true) {
      long cur = atom.get();

      if (update > cur) {
        if (atom.compareAndSet(cur, update))
          return true;
      } else
        return false;
    }
  }
}
