/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.distributed.LockNotHeldException;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;

public interface IndexUpdater {

  /**
   * This method is invoked when an entry is added, updated or destroyed in a
   * region for index maintenance. This method will do some pre-update
   * operations for the index like constraint checks or any other logging that
   * may be required, and any index updates if required.
   * 
   * @param owner
   *          the {@link Region} that owns this event; will be different from
   *          {@link EntryEvent#getRegion()} for partitioned regions
   * @param event
   *          the {@link EntryEvent} representing the operation.
   * @param entry
   *          the region entry.
   */
  public void onEvent(LocalRegion owner, EntryEventImpl event, RegionEntry entry);

  /**
   * This method is invoked after an entry has been added, updated or destroyed
   * in a region for index maintenance. This method will commit the changes to
   * the indexes or may rollback some of the changes done in {@link #onEvent} if
   * the entry operation failed for some reason.
   * 
   * @param owner
   *          the {@link Region} that owns this event; will be different from
   *          {@link EntryEvent#getRegion()} for partitioned regions
   * @param event
   *          the {@link EntryEvent} representing the operation.
   * @param entry
   *          the region entry.
   * @param success
   *          true if the entry operation succeeded and false otherwise.
   */
  public void postEvent(LocalRegion owner, EntryEventImpl event,
      RegionEntry entry, boolean success);

  /**
   * Invoked to clear all index entries for a bucket before destroying it.
   * 
   * @param baseBucket
   *          the {@link BucketRegion} being destroyed
   * @param bucketId
   *          the ID of the bucket being destroyed
   */
  public void clearIndexes(BucketRegion baseBucket, int bucketId);

  /**
   * Take a read lock indicating that bucket/region GII is in progress to block
   * index list updates during the process.
   * 
   * This is required to be a reentrant lock. The corresponding write lock that
   * will be taken by the implementation internally should also be reentrant.
   * 
   * @throws TimeoutException
   *           in case of timeout in acquiring the lock
   */
  public void lockForGII() throws TimeoutException;

  /**
   * Release the read lock taken for GII by {@link #lockForGII()}.
   * 
   * @throws LockNotHeldException
   *           if the current thread does not hold the read lock for GII
   */
  public void unlockForGII() throws LockNotHeldException;

  /**
   * Take a read lock to wait for completion of any index load in progress
   * during initial DDL replay. This is required since no table level locks are
   * acquired during initial DDL replay to avoid blocking most (if not all) DMLs
   * in the system whenever a new node comes up.
   * 
   * This will be removed at some point when we allow for concurrent loading and
   * initialization of index even while operations are in progress using
   * something similar to region GII token mode for indexes or equivalent (bug
   * 40899).
   * 
   * This is required to be a reentrant lock. The corresponding write lock that
   * will be taken by the implementation internally should also be reentrant.
   * 
   * @return true if locking was required and was acquired and false if it was
   *         not required
   * @throws TimeoutException
   *           in case of timeout in acquiring the lock
   */
  public boolean lockForIndexGII() throws TimeoutException;

  /**
   * Release the read lock taken for GII by {@link #lockForIndexGII()}.
   * 
   * @throws LockNotHeldException
   *           if the current thread does not hold the read lock
   */
  public void unlockForIndexGII() throws LockNotHeldException;
}
