/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin;

import java.io.Serializable;
import java.util.Date;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Represents display information about a single distributed lock
 */
public interface DLockInfo extends Serializable {
  public String getService();
  public String getThreadId();
  public String getLockName();
  public boolean isAcquired();
  public int getRecursionCount();    
  public InternalDistributedMember getOwner();
  public Date getLeaseExpireTime();
}
