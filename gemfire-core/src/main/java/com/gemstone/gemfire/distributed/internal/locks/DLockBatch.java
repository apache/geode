/*=========================================================================
 * Copyright (c) 2003-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.distributed.internal.locks;

import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Collection of distributed locks to be processed as a batch. 
 *
 * @author Kirk Lund
 */
public interface DLockBatch {
  
  /** Returns the originator (owner) of this batch of locks */
  public InternalDistributedMember getOwner();
  
  /** Returns the identity object of the batch; like a hash map key */
  public DLockBatchId getBatchId();
  
  /** Returns the list of TXRegionLockRequest instances */
  public List getReqs();
  
  /** 
   * Specifies the lock grantor id that granted this lock batch.
   * @param lockGrantorId the lock grantor id that granted this lock batch
   */
  public void grantedBy(LockGrantorId lockGrantorId);
  
}

