/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.io.Serializable;
import java.lang.management.LockInfo;
import java.lang.management.MonitorInfo;

import com.gemstone.gemfire.internal.concurrent.LI;

/**
 * This class is serializable version of the java 1.6 lock info
 * class. It also holds a locality field to identify the VM
 * where the lock is held.
 * 
 * @author dsmith
 *
 */
class LocalLockInfo implements Serializable {
  private static final long serialVersionUID = 1L;
  
  private final Serializable locatility;
  private final LI info;
  
  public LocalLockInfo(Serializable locatility, LockInfo sync) {
    super();
    this.locatility = locatility;
    //LockInfo and Monitor info aren't serializable, so copy the information from
    //them. For backwards compatibility, use the LI class which is used
    //in older versions of gemfire.
    if(sync instanceof MonitorInfo) {
      this.info = new LI(sync.getClassName(), sync.getIdentityHashCode(), ((MonitorInfo) sync).getLockedStackFrame());
    } else {
      this.info = new LI(sync.getClassName(), sync.getIdentityHashCode());
    }
  }

  public Serializable getLocality() {
    return locatility;
  }

  public LI getInfo() {
    return info;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((info == null) ? 0 : info.getClassName().hashCode());
    result = prime * result + ((info == null) ? 0 : info.getIdentityHashCode());
    result = prime * result
        + ((locatility == null) ? 0 : locatility.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof LocalLockInfo))
      return false;
    LocalLockInfo other = (LocalLockInfo) obj;
    if (info == null) {
      if (other.info != null)
        return false;
    } else if (!info.getClassName().equals(other.info.getClassName())) {
      return false;
    } else if (info.getIdentityHashCode() != other.info.getIdentityHashCode()) {
      return false;
    }
    if (locatility == null) {
      if (other.locatility != null)
        return false;
    } else if (!locatility.equals(other.locatility))
      return false;
    return true;
  }
  
  @Override
  public String toString() {
    return locatility + ":" + info; 
    
  }
}