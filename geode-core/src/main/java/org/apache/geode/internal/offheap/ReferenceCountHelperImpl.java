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

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.cache.RegionEntry;

/**
 * All access to this class should be done through the static methods of ReferenceCountHelper.
 */
class ReferenceCountHelperImpl {

  private static final Object SKIP_REF_COUNT_TRACKING = new Object();

  @Immutable
  private static final List<RefCountChangeInfo> LOCKED = emptyList();

  // TODO: ThreadLocals should be static final

  private final ThreadLocal<Object> refCountOwner;
  private final ThreadLocal<AtomicInteger> refCountReenterCount;

  private final boolean trackRefCounts;
  private final boolean trackFreedRefCounts;
  private final ConcurrentMap<Long, List<RefCountChangeInfo>> stacktraces;
  private final ConcurrentMap<Long, List<RefCountChangeInfo>> freedStacktraces;
  private final BiConsumer<String, Boolean> debugLogger;

  ReferenceCountHelperImpl(boolean trackRefCounts, boolean trackFreedRefCounts) {
    this(trackRefCounts, trackFreedRefCounts, MemoryAllocatorImpl::debugLog);
  }

  ReferenceCountHelperImpl(boolean trackRefCounts, boolean trackFreedRefCounts,
      BiConsumer<String, Boolean> debugLogger) {
    this.trackRefCounts = trackRefCounts;
    this.trackFreedRefCounts = trackFreedRefCounts;
    if (trackRefCounts) {
      stacktraces = new ConcurrentHashMap<>();
      if (trackFreedRefCounts) {
        freedStacktraces = new ConcurrentHashMap<>();
      } else {
        freedStacktraces = null;
      }
      refCountOwner = new ThreadLocal<>();
      refCountReenterCount = new ThreadLocal<>();
    } else {
      stacktraces = null;
      freedStacktraces = null;
      refCountOwner = null;
      refCountReenterCount = null;
    }
    this.debugLogger = debugLogger;
  }

  /**
   * Returns true if reference count tracking is enabled.
   */
  boolean trackReferenceCounts() {
    return trackRefCounts;
  }

  /**
   * Returns true if free operation tracking is enabled.
   */
  boolean trackFreedReferenceCounts() {
    return trackFreedRefCounts;
  }

  /**
   * Optional call to tell the tracker the logical "owner" of the reference count. For example you
   * could set the particular EntryEventImpl instance that incremented the reference count and is
   * responsible for decrementing it. Calling this method is a noop if !trackReferenceCounts.
   */
  void setReferenceCountOwner(Object owner) {
    if (trackReferenceCounts()) {
      if (refCountOwner.get() != null) {
        AtomicInteger reenterCount = refCountReenterCount.get();
        if (owner != null) {
          reenterCount.incrementAndGet();
        } else {
          if (reenterCount.decrementAndGet() <= 0) {
            refCountOwner.set(null);
            reenterCount.set(0);
          }
        }
      } else {
        AtomicInteger reenterCount = refCountReenterCount.get();
        if (reenterCount == null) {
          reenterCount = new AtomicInteger(0);
          refCountReenterCount.set(reenterCount);
        }
        if (owner != null) {
          reenterCount.set(1);
        } else {
          reenterCount.set(0);
        }
        refCountOwner.set(owner);
      }
    }
  }

  /**
   * Create, set, and return a generic reference count owner object. Calling this method is a noop
   * and returns null if !trackReferenceCounts.
   */
  Object createReferenceCountOwner() {
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
  boolean isRefCountTracking() {
    if (!trackReferenceCounts()) {
      return false;
    }
    return getReferenceCountOwner() != SKIP_REF_COUNT_TRACKING;
  }

  /**
   * Call this method to undo a call to skipRefCountTracking.
   */
  void unskipRefCountTracking() {
    setReferenceCountOwner(null);
  }

  /**
   * Returns a list of any reference count tracking information for the given Chunk address.
   */
  List<RefCountChangeInfo> getRefCountInfo(Long address) {
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

  /**
   * Returns a list of any reference count tracking information for the given Chunk address without
   * locking.
   */
  List<RefCountChangeInfo> peekRefCountInfo(long address) {
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
      refCountChangedTestHook(address, decRefCount, rc);

      List<RefCountChangeInfo> newList = new ArrayList<>();
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
      String message = "refCount " + (decRefCount ? "deced" : "inced")
          + " after orphan detected for @" + Long.toHexString(address);
      debugLogger.accept(message, true);
      return;
    }

    RefCountChangeInfo refCountChangeInfo = new RefCountChangeInfo(decRefCount, rc, owner);

    synchronized (list) {
      for (RefCountChangeInfo otherRefCountChangeInfo : list) {
        if (otherRefCountChangeInfo.isSameCaller(refCountChangeInfo)) {
          // No need to add it just increment useCount
          otherRefCountChangeInfo.incUseCount();
          return;
        }
      }
      list.add(refCountChangeInfo);
    }
  }

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
      debugLogger.accept("freed after orphan detected for @" + Long.toHexString(address), true);

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
  List<RefCountChangeInfo> getFreeRefCountInfo(long address) {
    if (!trackReferenceCounts() || !trackFreedReferenceCounts()) {
      return null;
    }
    return freedStacktraces.get(address);
  }

  /**
   * This method is overridden during testing to allow simulation of a concurrent update occurring
   * between stacktraces.get and stacktraces.replace
   */
  void getReferenceCountInfoTestHook(ConcurrentMap<Long, List<RefCountChangeInfo>> stacktraces,
      long address) {
    // nothing
  }

  /**
   * This method is overridden during testing to allow simulation of a race to be the first to
   * reference a given address
   */
  void refCountChangedTestHook(Long address, boolean decRefCount, int rc) {
    // nothing
  }
}
