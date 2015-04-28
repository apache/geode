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
import java.lang.management.ThreadInfo;
/**
* This class is serializable version of the java 1.6 ThreadInfo
* class. It also holds a locality field to identify the VM
* where the thread exists.
* 
* @author dsmith
*
*/
public class LocalThread implements Serializable, ThreadReference {
  private static final long serialVersionUID = 1L;
  
  private final Serializable locality;
  private final String threadName;
  private final long threadId;
  private final String threadStack;
  
  public LocalThread(Serializable locatility, ThreadInfo info) {
    this.locality = locatility;
    this.threadName = info.getThreadName();
    this.threadStack = generateThreadStack(info);
    this.threadId = info.getThreadId();
  }
  
  private String generateThreadStack(ThreadInfo info) {
    //This is annoying, but the to string method on info sucks.
    StringBuilder result = new StringBuilder();
    result.append(info.getThreadName()).append(" ID=")
        .append(info.getThreadId()).append(" state=")
        .append(info.getThreadState());
    
    
    if(info.getLockInfo() != null) {
      result.append("\n\twaiting to lock <" + info.getLockInfo() + ">");
    }
    for(StackTraceElement element : info.getStackTrace()) {
      result.append("\n\tat " + element);
      for(MonitorInfo monitor: info.getLockedMonitors()) {
        if(element.equals(monitor.getLockedStackFrame())) {
          result.append("\n\tlocked <" + monitor + ">");
        }
      }
    }
    
    if(info.getLockedSynchronizers().length > 0) {
      result.append("\nLocked synchronizers:");
      for(LockInfo sync : info.getLockedSynchronizers()) {
        result.append("\n" + sync.getClassName() + "@" + sync.getIdentityHashCode());
        
      }
    }
    
    return result.toString();
  }
  public Serializable getLocatility() {
    return locality;
  }
  public String getThreadName() {
    return threadName;
  }
  public long getThreadId() {
    return threadId;
  }
  public String getThreadStack() {
    return threadStack;
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int)(threadId ^ (threadId >>> 32));;
    result = prime * result
        + ((locality == null) ? 0 : locality.hashCode());
    return result;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof LocalThread))
      return false;
    LocalThread other = (LocalThread) obj;
    if (threadId != other.threadId)
      return false;
    if (locality == null) {
      if (other.locality != null)
        return false;
    } else if (!locality.equals(other.locality))
      return false;
    return true;
  }
  
  @Override
  public String toString() {
    return locality + ":" + threadName; 
  }
}