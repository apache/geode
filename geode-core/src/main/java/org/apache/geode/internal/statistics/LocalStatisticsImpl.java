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

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;

/**
 * An implementation of {@link Statistics} that stores its statistics in local java memory.
 *
 * @see <A href="package-summary.html#statistics">Package introduction</A>
 *
 *
 * @since GemFire 3.0
 *
 */
public class LocalStatisticsImpl extends StatisticsImpl {

  /** In JOM Statistics, the values of the long statistics */
  private final long[] longStorage;

  /** In JOM Statistics, the values of the double statistics */
  private final double[] doubleStorage;

  private final int longCount;
  /**
   * An array containing the JOM object used to lock a long statistic when it is incremented.
   */
  private final transient Object[] longLocks;

  /**
   * An array containing the JOM object used to lock a double statistic when it is incremented.
   */
  private final transient Object[] doubleLocks;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new statistics instance of the given type
   *
   * @param type A description of the statistics
   * @param textId Text that identifies this statistic when it is monitored
   * @param numericId A number that displayed when this statistic is monitored
   * @param uniqueId A number that uniquely identifies this instance
   * @param atomicIncrements Are increment operations atomic? If only one application thread
   *        increments a statistic, then a <code>false</code> value may yield better performance.
   * @param osStatFlags Non-zero if stats require system calls to collect them; for internal use
   *        only
   * @param statisticsManager The statistics manager that is creating this instance
   */
  public LocalStatisticsImpl(StatisticsType type, String textId, long numericId, long uniqueId,
      boolean atomicIncrements, int osStatFlags, StatisticsManager statisticsManager) {
    super(type, textId, numericId, uniqueId, osStatFlags, statisticsManager);

    StatisticsTypeImpl realType = (StatisticsTypeImpl) type;
    longCount = realType.getLongStatCount();
    int doubleCount = realType.getDoubleStatCount();

    if (longCount > 0) {
      longStorage = new long[longCount];
      if (atomicIncrements) {
        longLocks = new Object[longCount];
        for (int i = 0; i < longLocks.length; i++) {
          longLocks[i] = new Object();
        }
      } else {
        longLocks = null;
      }
    } else {
      longStorage = null;
      longLocks = null;
    }

    if (doubleCount > 0) {
      doubleStorage = new double[doubleCount];
      if (atomicIncrements) {
        doubleLocks = new Object[doubleCount];
        for (int i = 0; i < doubleLocks.length; i++) {
          doubleLocks[i] = new Object();
        }
      } else {
        doubleLocks = null;
      }
    } else {
      doubleStorage = null;
      doubleLocks = null;
    }
  }

  /**
   * Creates a new non-atomic statistics instance of the given type
   *
   * @param type A description of the statistics
   * @param textId Text that identifies this statistic when it is monitored
   * @param numericId A number that displayed when this statistic is monitored
   * @param uniqueId A number that uniquely identifies this instance
   *        increments a statistic, then a <code>false</code> value may yield better performance.
   * @param osStatFlags Non-zero if stats require system calls to collect them; for internal use
   *        only
   * @param statisticsManager The distributed system that determines whether or not these statistics
   *        are stored
   *        (and collected) in GemFire shared memory or in the local VM
   */
  public static Statistics createNonAtomic(StatisticsType type, String textId, long numericId,
      long uniqueId, int osStatFlags, StatisticsManager statisticsManager) {
    return new LocalStatisticsImpl(type, textId, numericId, uniqueId, false, osStatFlags,
        statisticsManager);
  }

  @Override
  public boolean isAtomic() {
    return longLocks != null || doubleLocks != null;
  }

  private int getOffsetFromLongId(int id) {
    return id;
  }

  private int getOffsetFromDoubleId(int id) {
    return id - longCount;
  }

  //////////////////////// store() Methods ///////////////////////

  @Override
  protected void _setLong(int id, long value) {
    int offset = getOffsetFromLongId(id);
    longStorage[offset] = value;
  }

  @Override
  protected void _setDouble(int id, double value) {
    int offset = getOffsetFromDoubleId(id);
    doubleStorage[offset] = value;
  }

  /////////////////////// get() Methods ///////////////////////

  @Override
  protected long _getLong(int id) {
    int offset = getOffsetFromLongId(id);
    return longStorage[offset];
  }

  @Override
  protected double _getDouble(int id) {
    int offset = getOffsetFromDoubleId(id);
    return doubleStorage[offset];
  }

  //////////////////////// inc() Methods ////////////////////////

  @Override
  protected void _incLong(int id, long delta) {
    int offset = getOffsetFromLongId(id);
    if (longLocks != null) {
      synchronized (longLocks[offset]) {
        longStorage[offset] += delta;
      }
    } else {
      longStorage[offset] += delta;
    }
  }

  @Override
  protected void _incDouble(int id, double delta) {
    int offset = getOffsetFromDoubleId(id);
    if (doubleLocks != null) {
      synchronized (doubleLocks[offset]) {
        doubleStorage[offset] += delta;
      }
    } else {
      doubleStorage[offset] += delta;
    }
  }
}
