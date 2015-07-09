/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.locks;

import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * DLockGrantorHelper provides testing operations that are not normally
 * exposed in the DLockGrantor.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public class DLockGrantorHelper {
  
  /**
   * Forces DLockGrantor to <code>handleDepartureOf<code> specified member,
   * which causes the grantor release all locks held by that member.
   * <p>
   * If the specified service instance is not the grantor, this method will
   * return without performing anything.
   */
  public static void forceDepartureOf(DistributedLockService dlock,
                                      InternalDistributedMember member) {
    DLockGrantor grantor = null;
    try {
      grantor = DLockGrantor.waitForGrantor(
          (DLockService) dlock);
      if (grantor != null) {
        grantor.handleDepartureOf(member);
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
  
//  public static void setReentrantSuspendLockingAllowed(DistributedLockService dlock,
//                                                       boolean value) {
//    ((DLockService) dlock).setReentrantSuspendLockingAllowed(value);
//  }
  
  /** Returns true if the grantor has waiting lock requests for the named lock */
  // TODO use this method
  public static boolean hasWaitingRequests(DistributedLockService dlock,
                                           Object name) {
    DLockGrantor grantor = null;
    try {
      grantor = DLockGrantor.waitForGrantor((DLockService) dlock);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (grantor != null) {
      return grantor.hasWaitingRequests(name);
    }
    return false;
  }

  /** Returns the DLockService.SUSPEND_LOCKING_TOKEN for testing */  
  public static Object getSuspendLockingToken() {
    return DLockService.SUSPEND_LOCKING_TOKEN;
  }
  
//  /** Returns true if process is DM elder for this system. */
//  public static boolean isElder() {
//    InternalDistributedSystem sys = 
//        (InternalDistributedSystem) InternalDistributedSystem.getAnyInstance();
//    DM dm = sys.getDistributionManager();
//    return dm.isElder();
//  }
  
//  /** Changes grantorInfo in Elder state to require grantor recovery. */
//  public static void forceGrantorRecovery(DistributedLockService dlock) {
//    InternalDistributedSystem sys = 
//        (InternalDistributedSystem) InternalDistributedSystem.getAnyInstance();
//    DistributionManager dm = (DistributionManager) sys.getDistributionManager();
//    ElderState es = dm.getElderState();
//    es.forceGrantorRecovery.dlock.getName();
//  }
  
}

