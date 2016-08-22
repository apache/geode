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
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.util.IntArray;
//import com.gemstone.gemfire.util.LongArray;

/**
 * An implementation of {@link Statistics} that stores its statistics
 * in local java memory.
 *
 * @see <A href="package-summary.html#statistics">Package introduction</A>
 *
 *
 * @since GemFire 3.0
 *
 */
public class LocalStatisticsImpl extends StatisticsImpl {

  /** In JOM Statistics, the values of the int statistics */
  private final int[] intStorage;

  /** In JOM Statistics, the values of the long statistics */
  private final long[] longStorage;

  /** In JOM Statistics, the values of the double statistics */
  private final double[] doubleStorage;

  /** An array containing the JOM object used to lock a int statistic when
   * it is incremented. */
  private transient final Object[] intLocks;

  /** An array containing the JOM object used to lock a long statistic when
   * it is incremented. */
  private transient final Object[] longLocks;

  /** An array containing the JOM object used to lock a double statistic when
   * it is incremented. */
  private transient final Object[] doubleLocks;

  /** The StatisticsFactory that created this instance */
  private final StatisticsManager dSystem;
  
  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new statistics instance of the given type
   *
   * @param type
   *        A description of the statistics
   * @param textId
   *        Text that identifies this statistic when it is monitored
   * @param numericId
   *        A number that displayed when this statistic is monitored
   * @param uniqueId
   *        A number that uniquely identifies this instance
   * @param atomicIncrements
   *        Are increment operations atomic?  If only one application
   *        thread increments a statistic, then a <code>false</code>
   *        value may yield better performance.
   * @param osStatFlags
   *        Non-zero if stats require system calls to collect them; for internal use only
   * @param system
   *        The distributed system that determines whether or not these
   *        statistics are stored (and collected) in GemFire shared
   *        memory or in the local VM
   */
  public LocalStatisticsImpl(StatisticsType type, String textId,
                             long numericId,
                             long uniqueId,
                             boolean atomicIncrements,
                             int osStatFlags,
                             StatisticsManager system) {
    super(type, calcTextId(system, textId), calcNumericId(system, numericId),
          uniqueId, osStatFlags);

    this.dSystem = system;

    StatisticsTypeImpl realType = (StatisticsTypeImpl)type;
    int intCount = realType.getIntStatCount();
    int longCount = realType.getLongStatCount();
    int doubleCount = realType.getDoubleStatCount();

    if (intCount > 0) {
      this.intStorage = new int[intCount];
      if (atomicIncrements) {
        this.intLocks = new Object[intCount];
        for (int i = 0; i < intLocks.length; i++) {
          intLocks[i] = new Object();
        }
      } else {
        this.intLocks = null;
      }
    } else {
      this.intStorage = null;
      this.intLocks = null;
    }

    if (longCount > 0) {
      this.longStorage = new long[longCount];
      if (atomicIncrements) {
        this.longLocks = new Object[longCount];
        for (int i = 0; i < longLocks.length; i++) {
          longLocks[i] = new Object();
        }
      } else {
        this.longLocks = null;
      }
    } else {
      this.longStorage = null;
      this.longLocks = null;
    }

    if (doubleCount > 0) {
      this.doubleStorage = new double[doubleCount];
      if (atomicIncrements) {
        this.doubleLocks = new Object[doubleCount];
        for (int i = 0; i < doubleLocks.length; i++) {
          doubleLocks[i] = new Object();
        }
      } else {
        this.doubleLocks = null;
      }
    } else {
      this.doubleStorage = null;
      this.doubleLocks = null;
    }
  }

  //////////////////////  Static Methods  //////////////////////

  private static long calcNumericId(StatisticsManager system, long userValue) {
    if (userValue != 0) {
      return userValue;
    } else {
      long result = OSProcess.getId(); // fix for bug 30239
      if (result == 0) {
        if (system != null) {
          result = system.getId();
        }
      }
      return result;
    }
  }

  private static String calcTextId(StatisticsManager system, String userValue) {
    if (userValue != null && !userValue.equals("")) {
      return userValue;
    } else {
      if (system != null) {
        return system.getName();
      } else {
        return "";
      }
    }
  }

  //////////////////////  Instance Methods  //////////////////////

  @Override
  public final boolean isAtomic() {
    return intLocks != null || longLocks != null || doubleLocks != null;
  }

  @Override
  public void close() {
    super.close();
    if (this.dSystem != null) {
      dSystem.destroyStatistics(this);
    }
  }

  ////////////////////////  store() Methods  ///////////////////////

  @Override
  protected final void _setInt(int offset, int value) {
    this.intStorage[offset] = value;
  }

  @Override
  protected final void _setLong(int offset, long value) {
    this.longStorage[offset] = value;
  }

  @Override
  protected final void _setDouble(int offset, double value) {
    this.doubleStorage[offset] = value;
  }

  ///////////////////////  get() Methods  ///////////////////////

  @Override
  protected final int _getInt(int offset) {
    return this.intStorage[offset];
  }

  @Override
  protected final long _getLong(int offset) {
    return this.longStorage[offset];
  }

  @Override
  protected final double _getDouble(int offset) {
    return this.doubleStorage[offset];
  }

  ////////////////////////  inc() Methods  ////////////////////////

  @Override
  protected final void _incInt(int offset, int delta) {
    if (this.intLocks != null) {
      synchronized (this.intLocks[offset]) {
        this.intStorage[offset] += delta;
      }
    } else {
      this.intStorage[offset] += delta;
    }
  }

  @Override
  protected final void _incLong(int offset, long delta) {
    if (this.longLocks != null) {
      synchronized (this.longLocks[offset]) {
        this.longStorage[offset] += delta;
      }
    } else {
      this.longStorage[offset] += delta;
    }
  }

  @Override
  protected final void _incDouble(int offset, double delta) {
    if (this.doubleLocks != null) {
      synchronized (this.doubleLocks[offset]) {
        this.doubleStorage[offset] += delta;
      }
    } else {
      this.doubleStorage[offset] += delta;
    }
  }

  /////////////////// internal package methods //////////////////

  final int[] _getIntStorage() {
    return this.intStorage;
  }
  final long[] _getLongStorage() {
    return this.longStorage;
  }
  final double[] _getDoubleStorage() {
    return this.doubleStorage;
  }
}
