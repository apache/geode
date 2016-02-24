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
package com.gemstone.gemfire.internal.stats50;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import java.util.concurrent.atomic.AtomicIntegerArray; // don't use backport here!
import java.util.concurrent.atomic.AtomicLongArray; // don't use backport here!
import java.util.concurrent.ConcurrentLinkedQueue; // don't use backport here!
import java.util.concurrent.CopyOnWriteArrayList; // don't use backport here!
import java.util.*;

/**
 * An implementation of {@link Statistics} that stores its statistics
 * in local java memory.
 *
 * @see <A href="package-summary.html#statistics">Package introduction</A>
 *
 *
 * @since 3.0
 *
 */
public class Atomic50StatisticsImpl extends StatisticsImpl {

  /** In JOM Statistics, the values of the int statistics */
  private final AtomicIntegerArray intStorage;
  private final AtomicIntegerArray intDirty;
  private final Object[] intReadPrepLock;

  /** In JOM Statistics, the values of the long statistics */
  private final AtomicLongArray longStorage;
  private final AtomicIntegerArray longDirty;
  private final Object[] longReadPrepLock;

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
   * @param system
   *        The distributed system that determines whether or not these
   *        statistics are stored (and collected) in GemFire shared
   *        memory or in the local VM
   */
  public Atomic50StatisticsImpl(StatisticsType type, String textId,
                             long numericId,
                             long uniqueId,
                             StatisticsManager system) {
    super(type, calcTextId(system, textId), calcNumericId(system, numericId),
          uniqueId, 0);
    this.dSystem = system;

    StatisticsTypeImpl realType = (StatisticsTypeImpl)type;
    if (realType.getDoubleStatCount() > 0) {
      throw new IllegalArgumentException(LocalizedStrings.Atomic50StatisticsImpl_ATOMICS_DO_NOT_SUPPORT_DOUBLE_STATS.toLocalizedString());
    }
    int intCount = realType.getIntStatCount();
    int longCount = realType.getLongStatCount();

    if (intCount > 0) {
      this.intStorage = new AtomicIntegerArray(intCount);
      this.intDirty = new AtomicIntegerArray(intCount);
      this.intReadPrepLock = new Object[intCount];
      for (int i=0; i < intCount; i++) {
        this.intReadPrepLock[i] = new Object();
      }
    } else {
      this.intStorage = null;
      this.intDirty = null;
      this.intReadPrepLock = null;
    }

    if (longCount > 0) {
      this.longStorage = new AtomicLongArray(longCount);
      this.longDirty = new AtomicIntegerArray(longCount);
      this.longReadPrepLock = new Object[longCount];
      for (int i=0; i < longCount; i++) {
        this.longReadPrepLock[i] = new Object();
      }
    } else {
      this.longStorage = null;
      this.longDirty = null;
      this.longReadPrepLock = null;
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
    return true;
  }

  @Override
  public void close() {
    super.close();
    if (this.dSystem != null) {
      dSystem.destroyStatistics(this);
    }
  }

  /**
   * Queue of new ThreadStorage instances.
   */
  private final ConcurrentLinkedQueue<ThreadStorage> threadStoreQ = new ConcurrentLinkedQueue<ThreadStorage>();
  /**
   * List of ThreadStorage instances that will be used to roll up stat values
   * on this instance. They come from the threadStoreQ.
   */
  private final CopyOnWriteArrayList<ThreadStorage> threadStoreList = new CopyOnWriteArrayList<ThreadStorage>();

  /**
   * The workspace each thread that modifies statistics will use to do the
   * mods locally.
   */
  private static class ThreadStorage {
    private final Thread owner;
    public volatile boolean dirty = false;
    public final AtomicIntegerArray intStore;
    public final AtomicLongArray longStore;

    public boolean isAlive() {
      return this.owner.isAlive();
    }
    
    public ThreadStorage(int intSize, int longSize) {
      this.owner = Thread.currentThread();
      if (intSize > 0) {
        this.intStore = new AtomicIntegerArray(intSize);
      } else {
        this.intStore = null;
      }
      if (longSize > 0) {
        this.longStore = new AtomicLongArray(longSize);
      } else {
        this.longStore = null;
      }
    }
  }
  private final ThreadLocal<ThreadStorage> threadStore = new ThreadLocal<ThreadStorage>();

  private ThreadStorage getThreadStorage() {
    ThreadStorage result = this.threadStore.get();
    if (result == null) {
      int intSize = 0;
      int longSize = 0;
      if (this.intStorage != null) {
        intSize = this.intStorage.length();
      }
      if (this.longStorage != null) {
        longSize = this.longStorage.length();
      }
      result = new ThreadStorage(intSize, longSize);
      this.threadStore.set(result);
      this.threadStoreQ.add(result);
    }
    return result;
  }
  private ThreadStorage getThreadStorageForWrite() {
    ThreadStorage result = getThreadStorage();
    if (!result.dirty) result.dirty = true;
    return result;
  }
  private AtomicIntegerArray getThreadIntStorage() {
    return getThreadStorageForWrite().intStore;
  }
  
  private AtomicLongArray getThreadLongStorage() {
    return getThreadStorageForWrite().longStore;
  }

  ////////////////////////  store() Methods  ///////////////////////

  @Override
  protected final void _setInt(int offset, int value) {
    doIntWrite(offset, value);
  }

  @Override
  protected final void _setLong(int offset, long value) {
    doLongWrite(offset, value);
  }

  @Override
  protected final void _setDouble(int offset, double value) {
    throw new IllegalStateException(LocalizedStrings.Atomic50StatisticsImpl_DOUBLE_STATS_NOT_ON_ATOMIC50.toLocalizedString());
  }

  ///////////////////////  get() Methods  ///////////////////////

  @Override
  protected final int _getInt(int offset) {
    return doIntRead(offset);
  }

  @Override
  protected final long _getLong(int offset) {
    return doLongRead(offset);
  }

  @Override
  protected final double _getDouble(int offset) {
    throw new IllegalStateException(LocalizedStrings.Atomic50StatisticsImpl_DOUBLE_STATS_NOT_ON_ATOMIC50.toLocalizedString());
  }

  ////////////////////////  inc() Methods  ////////////////////////

  @Override
  protected final void _incInt(int offset, int delta) {
    getThreadIntStorage().getAndAdd(offset, delta);
    setIntDirty(offset);
  }

  @Override
  protected final void _incLong(int offset, long delta) {
    getThreadLongStorage().getAndAdd(offset, delta);
    setLongDirty(offset);
  }

  @Override
  protected final void _incDouble(int offset, double delta) {
    throw new IllegalStateException(LocalizedStrings.Atomic50StatisticsImpl_DOUBLE_STATS_NOT_ON_ATOMIC50.toLocalizedString());
  }

  private static final ThreadLocal samplerThread = new ThreadLocal();

  /**
   * Prepare the threadStoreList by moving into it all the new instances in Q.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="JLM_JSR166_UTILCONCURRENT_MONITORENTER",
  justification="findbugs complains about this synchronize. It could be changed to a sync on a dedicated Object instance to make findbugs happy. see comments below")
  private void prepareThreadStoreList() {
    // The following sync is for the rare case when this method is called concurrently.
    // In that case it would be sub-optimal for both threads to concurrently create their
    // own ArrayList and then for both of them to call addAll.
    // findbugs complains about this synchronize. It could be changed to a sync on a dedicated Object instance to make findbugs happy.
    synchronized(threadStoreList) {
      ThreadStorage ts = this.threadStoreQ.poll();
      if (ts == null) return;
      ArrayList<ThreadStorage> tmp = new ArrayList<ThreadStorage>(64);
      do {
        tmp.add(ts);
        ts = this.threadStoreQ.poll();      
      } while (ts != null);
      if (tmp.size() > 0) {
        this.threadStoreList.addAll(tmp);
      }
    }
  }
  /**
   * Used to take striped thread stats and "roll them up" into a single
   * shared stat.
   * @since 5.1
   */
  @Override
  public void prepareForSample() {
    // mark this thread as the sampler
    if (samplerThread.get() == null) samplerThread.set(Boolean.TRUE);
    prepareThreadStoreList();
    ArrayList<ThreadStorage> removed = null;
    for (ThreadStorage ts: this.threadStoreList) {
      if (!ts.isAlive()) {
        if (removed == null) {
          removed = new ArrayList<ThreadStorage>(64);
        }
        removed.add(ts);
      }
      if (ts.dirty) {
        ts.dirty = false;
        if (ts.intStore != null) {
          for (int i=0; i < ts.intStore.length(); i++) {
            synchronized (this.intReadPrepLock[i]) {
              int delta = ts.intStore.getAndSet(i, 0);
              if (delta != 0) {
                this.intStorage.getAndAdd(i, delta);
              }
            }
          }
        }
        if (ts.longStore != null) {
          for (int i=0; i < ts.longStore.length(); i++) {
            synchronized (this.longReadPrepLock[i]) {
              long delta = ts.longStore.getAndSet(i, 0);
              if (delta != 0) {
                this.longStorage.getAndAdd(i, delta);
              }
            }
          }
        }
      }
    }
    if (removed != null) {
      this.threadStoreList.removeAll(removed);
    }
  }
  
  private final boolean isIntDirty(final int idx) {
    return this.intDirty.get(idx) != 0;
  }
  private final boolean isLongDirty(final int idx) {
    return this.longDirty.get(idx) != 0;
  }
  private final boolean clearIntDirty(final int idx) {
    if (!this.intDirty.weakCompareAndSet(idx, 1/*expected*/, 0/*update*/)) {
      return this.intDirty.compareAndSet(idx, 1/*expected*/, 0/*update*/);
    }
    return true;
  }
  private final boolean clearLongDirty(final int idx) {
    if (!this.longDirty.weakCompareAndSet(idx, 1/*expected*/, 0/*update*/)) {
      return this.longDirty.compareAndSet(idx, 1/*expected*/, 0/*update*/);
    }
    return true;
  }
  private final void setIntDirty(final int idx) {
    if (!this.intDirty.weakCompareAndSet(idx, 0/*expected*/, 1/*update*/)) {
      if (!isIntDirty(idx)) {
        this.intDirty.set(idx, 1);
      }
    }
  }
  private final void setLongDirty(final int idx) {
    if (!this.longDirty.weakCompareAndSet(idx, 0/*expected*/, 1/*update*/)) {
      if (!isLongDirty(idx)) {
        this.longDirty.set(idx, 1);
      }
    }
  }

  private final int doIntRead(final int idx) {
    // early out for sampler; it called prepareForSample
    if (samplerThread.get() != null) {
      return this.intStorage.get(idx);
    }
    synchronized (this.intReadPrepLock[idx]) {
      if (!isIntDirty(idx)) {
        // no need to prepare if not dirty
        return this.intStorage.get(idx);
      }
    }
    // this can take a while so release sync
    prepareThreadStoreList();
    synchronized (this.intReadPrepLock[idx]) {
      if (!clearIntDirty(idx)) {
        // no need to prepare if not dirty
        return this.intStorage.get(idx);
      }
      int delta = 0;
      for (ThreadStorage ts: this.threadStoreList) {
        delta += ts.intStore.getAndSet(idx, 0);
      }
      if (delta != 0) {
        return this.intStorage.addAndGet(idx, delta);
      }
      else {
        return this.intStorage.get(idx);
      }
    }
  }
  private final void doIntWrite(final int idx, int value) {
    synchronized (this.intReadPrepLock[idx]) {
      if (!isIntDirty(idx)) {
        // no need to prepare if not dirty
        this.intStorage.set(idx, value);
        return;
      }
    }
    prepareThreadStoreList();
    synchronized (this.intReadPrepLock[idx]) {
      if (clearIntDirty(idx)) {
        for (ThreadStorage ts: this.threadStoreList) {
          if (ts.intStore.get(idx) != 0) {
            ts.intStore.set(idx, 0);
          }
        }
      }
      this.intStorage.set(idx, value);
    }
  }
  private final long doLongRead(final int idx) {
    if (samplerThread.get() != null) {
      return this.longStorage.get(idx);
    }
    synchronized (this.longReadPrepLock[idx]) {
      if (!isLongDirty(idx)) {
        // no need to prepare if not dirty
        return this.longStorage.get(idx);
      }
    }
    // this can take a while so release sync
    prepareThreadStoreList();
    synchronized (this.longReadPrepLock[idx]) {
      if (!clearLongDirty(idx)) {
        // no need to prepare if not dirty
        return this.longStorage.get(idx);
      }
      long delta = 0;
      for (ThreadStorage ts: this.threadStoreList) {
        delta += ts.longStore.getAndSet(idx, 0);
      }
      if (delta != 0) {
        return this.longStorage.addAndGet(idx, delta);
      }
      else {
        return this.longStorage.get(idx);
      }
    }
  }
  private final void doLongWrite(int idx, long value) {
    synchronized (this.longReadPrepLock[idx]) {
      if (!isLongDirty(idx)) {
        // no need to prepare if not dirty
        this.longStorage.set(idx, value);
        return;
      }
    }
    // this can take a while so release sync
    prepareThreadStoreList();
    synchronized (this.longReadPrepLock[idx]) {
      if (clearLongDirty(idx)) {
        for (ThreadStorage ts: this.threadStoreList) {
          if (ts.longStore.get(idx) != 0) {
            ts.longStore.set(idx, 0);
          }
        }
      }
      this.longStorage.set(idx, value);
    }
  }

  /////////////////// internal package methods //////////////////

  final int[] _getIntStorage() {
    throw new IllegalStateException(LocalizedStrings.Atomic50StatisticsImpl_DIRECT_ACCESS_NOT_ON_ATOMIC50.toLocalizedString());
  }
  final long[] _getLongStorage() {
    throw new IllegalStateException(LocalizedStrings.Atomic50StatisticsImpl_DIRECT_ACCESS_NOT_ON_ATOMIC50.toLocalizedString());
  }
  final double[] _getDoubleStorage() {
    throw new IllegalStateException(LocalizedStrings.Atomic50StatisticsImpl_DIRECT_ACCESS_NOT_ON_ATOMIC50.toLocalizedString());
  }
}
