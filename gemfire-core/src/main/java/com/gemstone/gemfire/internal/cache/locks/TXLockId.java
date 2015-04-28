/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache.locks;

import com.gemstone.gemfire.distributed.internal.locks.DLockBatchId;
import com.gemstone.gemfire.distributed.internal.locks.LockGrantorId;
import com.gemstone.gemfire.distributed.internal.membership.*;

/** Specifies a set of keys to try-lock within the scope of a region */
public interface TXLockId extends DLockBatchId {
  /** Gets the member id of the owner of this lock */
  public InternalDistributedMember getMemberId();
  /** Gets the count that identifies the lock id in this member */
  public int getCount();
  /** Sets the lock grantor id that granted this lock */
  public void setLockGrantorId(LockGrantorId lockGrantorId);
  /** Gets the lock grantor id that granted this lock */
  public LockGrantorId getLockGrantorId();
}

