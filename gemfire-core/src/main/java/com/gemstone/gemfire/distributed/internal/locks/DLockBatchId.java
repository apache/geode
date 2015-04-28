/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.distributed.internal.locks;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.distributed.internal.locks.LockGrantorId;

/** 
 * Identifies a {@link DLockBatch}.
 * @author Kirk Lund
 */
public interface DLockBatchId extends DataSerializable {
  /** Gets the lock grantor id that granted this lock */
  public LockGrantorId getLockGrantorId();
}

