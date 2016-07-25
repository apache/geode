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

package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class provides static methods to help
 * debug off-heap reference count problems.
 * To enable reference count tracking set: -Dgemfire.trackOffHeapRefCounts=true.
 * To enable free operation tracking set: -Dgemfire.trackOffHeapFreedRefCounts=true.
 */
public class ReferenceCountHelper {

  public static final String TRACK_OFFHEAP_REFERENCES = DistributionConfig.GEMFIRE_PREFIX + "trackOffHeapRefCounts";
  public static final String TRACK_OFFHEAP_FREES = DistributionConfig.GEMFIRE_PREFIX + "trackOffHeapFreedRefCounts";

  private static final ReferenceCountHelperImpl inst = new ReferenceCountHelperImpl(Boolean.getBoolean(TRACK_OFFHEAP_REFERENCES), Boolean.getBoolean(TRACK_OFFHEAP_FREES));

  /* Do not allow any instances */
  private ReferenceCountHelper() {}
  
  static ReferenceCountHelperImpl getInstance() {
    return inst;
  }
  
  /**
   * Returns true if reference count tracking is enabled.
   */
  public static boolean trackReferenceCounts() {
    return getInstance().trackReferenceCounts();
  }

  /**
   * Returns true if free operation tracking is enabled.
   */
  public static boolean trackFreedReferenceCounts() {
    return getInstance().trackFreedReferenceCounts();
  }

  /**
   * Optional call to tell the tracker the logical "owner"
   * of the reference count. For example you could set
   * the particular EntryEventImpl instance that incremented
   * the reference count and is responsible for decrementing it.
   * Calling this method is a noop if !trackReferenceCounts.
   */
  public static void setReferenceCountOwner(Object owner) {
    getInstance().setReferenceCountOwner(owner);
  }

  /**
   * Create, set, and return a generic reference count owner object.
   * Calling this method is a noop and returns null if !trackReferenceCounts.
   */
  public static Object createReferenceCountOwner() {
    return getInstance().createReferenceCountOwner();
  }

  /**
   * Call this method before incrementing a reference count
   * if you know that tracking is not needed because you know
   * that the allocate and free will always be done in the same
   * code block.
   * Callers of this method must also call unskipRefCountTracking
   * after the allocation or free is done.
   */
  public static void skipRefCountTracking() {
    getInstance().skipRefCountTracking();
  }

  /**
   * Returns true if currently tracking reference counts.
   */
  public static boolean isRefCountTracking() {
    return getInstance().isRefCountTracking();
  }

  /**
   * Call this method to undo a call to skipRefCountTracking.
   */
  public static void unskipRefCountTracking() {
    getInstance().unskipRefCountTracking();
  }

  /**
   * Returns a list of any reference count tracking information for
   * the given Chunk address.
   */
  public static List<RefCountChangeInfo> getRefCountInfo(long address) {
    return getInstance().getRefCountInfo(address);
  }

  /**
   * Used internally to report that a reference count has changed.
   */  
  static void refCountChanged(Long address, boolean decRefCount, int rc) {
    getInstance().refCountChanged(address, decRefCount, rc);
  }

  /**
   * Called internally when free operations are tracked to record
   * that a free has happened of the given address.
   */
  static void freeRefCountInfo(Long address) {
    getInstance().freeRefCountInfo(address);
  }
  
  /**
   * Returns the thread local owner
   */
  static Object getReferenceCountOwner() {
    return getInstance().getReferenceCountOwner();
  }

  /**
   * Returns the thread local count of the
   * number of times ref count has been updated
   */
  static AtomicInteger getReenterCount() {
    return getInstance().getReenterCount();
  }

  /**
   * Returns a list of any free operation tracking information.
   * This is used to describe who did the previous free(s) when an extra one
   * ends up being done and fails.
   */
  public static List<RefCountChangeInfo> getFreeRefCountInfo(long address) {
    return getInstance().getFreeRefCountInfo(address);
  }

  /**
   * Returns a list of any reference count tracking information for
   * the given Chunk address without locking.
   */
  static List<RefCountChangeInfo> peekRefCountInfo(long address) {
    return getInstance().peekRefCountInfo(address);
  }
}
