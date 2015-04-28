/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

public class LockObject {
  public Object key;
  public long lockedTimeStamp;
  private boolean removed;
    
  public LockObject(Object key, long lockedTimeStamp) {
    this.key = key;
    this.lockedTimeStamp = lockedTimeStamp;
  }
  
  /**Always updated when the monitor is held on this object */
  public void setRemoved() {
    this.removed = true;
  }

  /**Always checked when the monitor is held on this object */
  public boolean isRemoved() {
    return this.removed;
  }
  
  public String toString() {
    return "{LockObject="+key+"("+lockedTimeStamp+")}";
  }
}
