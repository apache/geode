/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.concurrent;

import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.LocalStatisticsImpl;
import com.gemstone.gemfire.internal.StatisticsManager;
import com.gemstone.gemfire.internal.StatisticsTypeImpl;
import com.gemstone.gemfire.internal.stats50.Atomic50StatisticsImpl;

public class Atomics {
  private Atomics() { }
  
  /**
   * Whether per-thread stats are used.  Striping is disabled for the
   * IBM JVM due to bug 38226
   */
  private static final boolean STRIPED_STATS_DISABLED = Boolean.getBoolean("gemfire.STRIPED_STATS_DISABLED")
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
