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

package org.apache.geode.internal.offheap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.internal.cache.RegionEntry;

/**
 * All access to this class should be done through the static methods of ReferenceCountHelper.
 */
class ReferenceCountHelperImpl {
  private boolean trackRefCounts;
  private boolean trackFreedRefCounts;
  private ConcurrentMap<Long, List<RefCountChangeInfo>> stacktraces;
  private ConcurrentMap<Long, List<RefCountChangeInfo>> freedStacktraces;
  private ThreadLocal<Object> refCountOwner;
  private ThreadLocal<AtomicInteger> refCountReenterCount;
  private static final Object SKIP_REF_COUNT_TRACKING = new Object();
  @MakeNotStatic
  private static final List<RefCountChangeInfo> LOCKED = Collections.emptyList();

  ReferenceCountHelperImpl(boolean trackRefCounts, boolean trackFreedRefCounts) {
    this.trackRefCounts = trackRefCounts;
    this.trackFreedRefCounts = trackFreedRefCounts;
    if (trackRefCounts) {
      stacktraces = new ConcurrentHashMap<Long, List<RefCountChangeInfo>>();
      if (trackFreedRefCounts) {
        freedStacktraces = new ConcurrentHashMap<Long, List<RefCountChangeInfo>>();
      } else {
        freedStacktraces = null;
      }
      refCountOwner = new ThreadLocal<Object>();
      refCountReenterCount = new ThreadLocal<AtomicInteger>();
    } else {
      stacktraces = null;
      freedStacktraces = null;
      refCountOwner = null;
      refCountReenterCount = null;
    }
  }

  /**
   * Returns true if reference count tracking is enabled.
   */
  public boolean trackReferenceCounts() {
    return trackRefCounts;
  }

  /**
   * Returns true if free operation tracking is enabled.
   */
  public boolean trackFreedReferenceCounts() {
    return trackFreedRefCounts;
  }

  /**
   * Optional call to tell the tracker the logical "owner" of the reference count. For example you
   * could set the particular EntryEventImpl instance that incremented the reference count and is
   * responsible for decrementing it. Calling this method is a noop if !trackReferenceCounts.
   */
  public void setReferenceCountOwner(Object owner) {
    if (trackReferenceCounts()) {
      if (refCountOwner.get() != null) {
        AtomicInteger ai = refCountReenterCount.get();
        if (owner != null) {
          ai.incrementAndGet();
        } else {
          if (ai.decrementAndGet() <= 0) {
            refCountOwner.set(null);
            ai.set(0);
          }
        }
      } else {
        AtomicInteger ai = refCountReenterCount.get();
        if (ai == null) {
          ai = new AtomicInteger(0);
          refCountReenterCount.set(ai);
        }
        if (owner != null) {
          ai.set(1);
        } else {
          ai.set(0);
        }
        refCountOwner.set(owner);
      }
    }
  }

  /**
   * Create, set, and return a generic reference count owner object. Calling this method is a noop
   * and returns null if !trackReferenceCounts.
   */
  public Object createReferenceCountOwner() {
    Object result = null;
    if (trackReferenceCounts()) {
      result = new Object();
      setReferenceCountOwner(result);
    }
    return result;
  }

  /**
   * Call this method before incrementing a reference count if you know that tracking is not needed
   * because you know that the allocate and free will always be done in the same code block. Callers
   * of this method must also call unskipRefCountTracking after the allocation or free is done.
   */
  public void skipRefCountTracking() {
    setReferenceCountOwner(SKIP_REF_COUNT_TRACKING);
  }

  /**
   * Returns true if currently tracking reference counts.
   */
  public boolean isRefCountTracking() {
    if (!trackReferenceCounts()) {
      return false;
    }
    return !(getReferenceCountOwner() == SKIP_REF_COUNT_TRACKING);
  }

  /**
   * Call this method to undo a call to skipRefCountTracking.
   */
  public void unskipRefCountTracking() {
    setReferenceCountOwner(null);
  }

  /**
   * Returns a list of any reference count tracking information for the given Chunk address.
   */
  public List<RefCountChangeInfo> getRefCountInfo(long address) {
    if (!trackReferenceCounts()) {
      return null;
    }
    List<RefCountChangeInfo> result = stacktraces.get(address);

    getReferenceCountInfoTestHook(stacktraces, address);

    while (result != null && !stacktraces.replace(address, result, LOCKED)) {
      result = stacktraces.get(address);
    }
    return result;
  }

  /*
   * This method is overridden during testing to allow simulation of a concurrent update occurring
   * between stacktraces.get and stacktraces.replace
   */
  protected void getReferenceCountInfoTestHook(
      ConcurrentMap<Long, List<RefCountChangeInfo>> stacktraces, long address) {}

  /**
   * Returns a list of any reference count tracking information for the given Chunk address without
   * locking.
   */
  public List<RefCountChangeInfo> peekRefCountInfo(long address) {
    if (!trackReferenceCounts()) {
      return null;
    }
    return stacktraces.get(address);
  }

  /**
   * Used internally to report that a reference count has changed.
   */
  void refCountChanged(Long address, boolean decRefCount, int rc) {
    if (!trackReferenceCounts()) {
      return;
    }
    final Object owner = refCountOwner.get();
    if (owner == SKIP_REF_COUNT_TRACKING) {
      return;
    }
    List<RefCountChangeInfo> list = stacktraces.get(address);
    if (list == null) {
      List<RefCountChangeInfo> newList = new ArrayList<RefCountChangeInfo>();

      refCountChangedTestHook(address, decRefCount, rc);

      List<RefCountChangeInfo> old = stacktraces.putIfAbsent(address, newList);
      if (old == null) {
        list = newList;
      } else {
        list = old;
      }
    }
    if (decRefCount) {
      if (owner != null) {
        synchronized (list) {
          for (int i = 0; i < list.size(); i++) {
            RefCountChangeInfo info = list.get(i);
            if (owner instanceof RegionEntry) {
              if (owner == info.getOwner()) {
                if (info.getUseCount() > 0) {
                  info.decUseCount();
                } else {
                  list.remove(i);
                }
                return;
              }
            } else if (owner.equals(info.getOwner())) {
              if (info.getUseCount() > 0) {
                info.decUseCount();
              } else {
                list.remove(i);
              }
              return;
            }
          }
        }
      }
    }
    if (list == LOCKED) {
      MemoryAllocatorImpl.debugLog("refCount " + (decRefCount ? "deced" : "inced")
          + " after orphan detected for @" + Long.toHexString(address), true);
      return;
    }
    RefCountChangeInfo info = new RefCountChangeInfo(decRefCount, rc, owner);
    synchronized (list) {
      // if (list.size() == 16) {
      // debugLog("dumping @" + Long.toHexString(address) + " history=" + list, false);
      // list.clear();
      // }
      for (RefCountChangeInfo e : list) {
        if (e.isSameCaller(info)) {
          // No need to add it just increment useCount
          e.incUseCount();
          return;
        }
      }
      list.add(info);
    }
  }

  /*
   * This method is overridden during testing to allow simulation of a race to be the first to
   * reference a given address
   */
  protected void refCountChangedTestHook(Long address, boolean decRefCount, int rc) {}

  /**
   * Called internally when free operations are tracked to record that a free has happened of the
   * given address.
   */
  void freeRefCountInfo(Long address) {
    if (!trackReferenceCounts()) {
      return;
    }
    List<RefCountChangeInfo> freedInfo = stacktraces.remove(address);
    if (freedInfo == LOCKED) {
      MemoryAllocatorImpl.debugLog("freed after orphan detected for @" + Long.toHexString(address),
          true);
    } else if (trackFreedReferenceCounts()) {
      if (freedInfo != null) {
        freedStacktraces.put(address, freedInfo);
      } else {
        freedStacktraces.remove(address);
      }
    }
  }

  /**
   * Returns the thread local owner
   */
  Object getReferenceCountOwner() {
    if (!trackReferenceCounts()) {
      return null;
    }
    return refCountOwner.get();
  }

  /**
   * Returns the thread local count of the number of times ref count has been updated
   */
  AtomicInteger getReenterCount() {
    if (!trackReferenceCounts()) {
      return null;
    }
    return refCountReenterCount.get();
  }

  /**
   * Returns a list of any free operation tracking information. This is used to describe who did the
   * previous free(s) when an extra one ends up being done and fails.
   */
  public List<RefCountChangeInfo> getFreeRefCountInfo(long address) {
    if (!trackReferenceCounts() || !trackFreedReferenceCounts()) {
      return null;
    }
    return freedStacktraces.get(address);
  }

}
