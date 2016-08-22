/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal.locks;

import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * DLockGrantorHelper provides testing operations that are not normally
 * exposed in the DLockGrantor.
 *
 * @since GemFire 5.0
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

