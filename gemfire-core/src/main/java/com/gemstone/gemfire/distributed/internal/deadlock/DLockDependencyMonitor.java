/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.distributed.internal.locks.DLockToken;

/**
 * Report dependencies on dlocks that exist in theis VM.
 * 
 * @author dsmith
 *
 */
public class DLockDependencyMonitor implements DependencyMonitor {
  static final DLockDependencyMonitor INSTANCE;
  
  static {
    INSTANCE = new DLockDependencyMonitor();
//    DependencyMonitorManager.addMonitor(INSTANCE);
  }
  
  

  public Set<Dependency<Thread, Serializable>> getBlockedThreads(
      Thread[] allThreads) {
    Set<Dependency<Thread, Serializable>> results = new HashSet<Dependency<Thread, Serializable>>();

    DLockService.dumpAllServices(); // for investigating bug #43496
    
    Map<String, DLockService> services = DLockService.snapshotAllServices();
    for (Map.Entry<String, DLockService> entry : services.entrySet()) {
      String serviceName = entry.getKey();
      DLockService service = entry.getValue();

      UnsafeThreadLocal<Object> blockedThreadLocal = service.getBlockedOn();
      for (Thread thread : allThreads) {
        Object lockName = blockedThreadLocal.get(thread);
        if (lockName != null) {
          results.add(new Dependency<Thread, Serializable>(thread, new LockId(
              serviceName, (Serializable) lockName)));
        }
      }
    }

    return results;
  }

  public Set<Dependency<Serializable, Thread>> getHeldResources(
      Thread[] allThreads) {
    
    InternalDistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    if(ds == null) {
      return Collections.emptySet();
    }
    Set<Dependency<Serializable, Thread>> results= new HashSet<Dependency<Serializable, Thread>>();
    
    Map<String, DLockService> services = DLockService.snapshotAllServices();
    for(Map.Entry<String, DLockService> entry : services.entrySet()) {
      String serviceName = entry.getKey();
      DLockService service = entry.getValue();
      Map<Object, DLockToken> tokens = service.snapshotService();
      for(Map.Entry<Object, DLockToken> tokenEntry : tokens.entrySet()) {
        Object tokenName = tokenEntry.getKey();
        DLockToken token = tokenEntry.getValue();
        synchronized(token) {
          Thread holdingThread = token.getThread();
          if(holdingThread != null) {
            results.add(new Dependency(new LockId(serviceName,
                (Serializable) tokenName), holdingThread));
          }
        }
      }
    }
    
    return results;
  }
  
  private static class LockId implements Serializable {
    private final String serviceName;
    private final Serializable tokenName;
    
    public LockId(String serviceName, Serializable tokenName) {
      this.serviceName = serviceName;
      this.tokenName = tokenName;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((serviceName == null) ? 0 : serviceName.hashCode());
      result = prime * result
          + ((tokenName == null) ? 0 : tokenName.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof LockId))
        return false;
      LockId other = (LockId) obj;
      if (serviceName == null) {
        if (other.serviceName != null)
          return false;
      } else if (!serviceName.equals(other.serviceName))
        return false;
      if (tokenName == null) {
        if (other.tokenName != null)
          return false;
      } else if (!tokenName.equals(other.tokenName))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "DLock(" + serviceName +", " + tokenName + ")";
    }
  }

}
