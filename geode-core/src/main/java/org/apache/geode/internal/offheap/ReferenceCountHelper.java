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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * This class provides static methods to help debug off-heap reference count problems. To enable
 * reference count tracking set: -Dgemfire.trackOffHeapRefCounts=true. To enable free operation
 * tracking set: -Dgemfire.trackOffHeapFreedRefCounts=true.
 */
public class ReferenceCountHelper {

  private static final String TRACK_OFFHEAP_REFERENCES =
      GeodeGlossary.GEMFIRE_PREFIX + "trackOffHeapRefCounts";
  private static final String TRACK_OFFHEAP_FREES =
      GeodeGlossary.GEMFIRE_PREFIX + "trackOffHeapFreedRefCounts";

  @Immutable
  private static final ReferenceCountHelper INSTANCE = new ReferenceCountHelper(
      new ReferenceCountHelperImpl(Boolean.getBoolean(TRACK_OFFHEAP_REFERENCES),
          Boolean.getBoolean(TRACK_OFFHEAP_FREES)));

  private final ReferenceCountHelperImpl delegate;

  private ReferenceCountHelper(ReferenceCountHelperImpl delegate) {
    this.delegate = delegate;
  }

  private static ReferenceCountHelperImpl delegate() {
    return INSTANCE.delegate;
  }

  /**
   * Returns true if reference count tracking is enabled.
   */
  public static boolean trackReferenceCounts() {
    return delegate().trackReferenceCounts();
  }

  /**
   * Returns true if free operation tracking is enabled.
   */
  static boolean trackFreedReferenceCounts() {
    return delegate().trackFreedReferenceCounts();
  }

  /**
   * Optional call to tell the tracker the logical "owner" of the reference count. For example you
   * could set the particular EntryEventImpl instance that incremented the reference count and is
   * responsible for decrementing it. Calling this method is a noop if !trackReferenceCounts.
   */
  public static void setReferenceCountOwner(Object owner) {
    delegate().setReferenceCountOwner(owner);
  }

  /**
   * Create, set, and return a generic reference count owner object. Calling this method is a noop
   * and returns null if !trackReferenceCounts.
   */
  public static Object createReferenceCountOwner() {
    return delegate().createReferenceCountOwner();
  }

  /**
   * Call this method before incrementing a reference count if you know that tracking is not needed
   * because you know that the allocate and free will always be done in the same code block. Callers
   * of this method must also call unskipRefCountTracking after the allocation or free is done.
   */
  public static void skipRefCountTracking() {
    delegate().skipRefCountTracking();
  }

  /**
   * Returns true if currently tracking reference counts.
   */
  public static boolean isRefCountTracking() {
    return delegate().isRefCountTracking();
  }

  /**
   * Call this method to undo a call to skipRefCountTracking.
   */
  public static void unskipRefCountTracking() {
    delegate().unskipRefCountTracking();
  }

  /**
   * Returns a list of any reference count tracking information for the given Chunk address.
   */
  public static List<RefCountChangeInfo> getRefCountInfo(long address) {
    return delegate().getRefCountInfo(address);
  }

  /**
   * Used internally to report that a reference count has changed.
   */
  public static void refCountChanged(Long address, boolean decRefCount, int rc) {
    delegate().refCountChanged(address, decRefCount, rc);
  }

  /**
   * Called internally when free operations are tracked to record that a free has happened of the
   * given address.
   */
  public static void freeRefCountInfo(Long address) {
    delegate().freeRefCountInfo(address);
  }

  /**
   * Returns the thread local owner
   */
  public static Object getReferenceCountOwner() {
    return delegate().getReferenceCountOwner();
  }

  /**
   * Returns the thread local count of the number of times ref count has been updated
   */
  public static AtomicInteger getReenterCount() {
    return delegate().getReenterCount();
  }

  /**
   * Returns a list of any free operation tracking information. This is used to describe who did the
   * previous free(s) when an extra one ends up being done and fails.
   */
  public static List<RefCountChangeInfo> getFreeRefCountInfo(long address) {
    return delegate().getFreeRefCountInfo(address);
  }

  /**
   * Returns a list of any reference count tracking information for the given Chunk address without
   * locking.
   */
  public static List<RefCountChangeInfo> peekRefCountInfo(long address) {
    return delegate().peekRefCountInfo(address);
  }
}
