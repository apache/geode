/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.cache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.locks.TXLockId;
import org.apache.geode.internal.cache.locks.TXLockService;
import org.apache.geode.internal.cache.locks.TXRegionLockRequest;

/**
 * TXLockRequest represents all the locks that need to be made for a single transaction.
 *
 *
 * @since GemFire 4.0
 *
 */
public class TXLockRequest {
  private boolean localLockHeld;
  private TXLockId distLockId;
  private IdentityArrayList localLocks; // of TXRegionLockRequest
  private ArrayList<TXRegionLockRequest> distLocks; // of TXRegionLockRequest
  private Set otherMembers;

  public TXLockRequest() {
    localLockHeld = false;
    distLockId = null;
    localLocks = null;
    distLocks = null;
    otherMembers = null;
  }

  void setOtherMembers(Set s) {
    otherMembers = s;
  }

  public void addLocalRequest(TXRegionLockRequest req) {
    if (localLocks == null) {
      localLocks = new IdentityArrayList();
    }
    localLocks.add(req);
  }

  public TXRegionLockRequest getRegionLockRequest(String regionFullPath) {
    if (localLocks == null || regionFullPath == null) {
      return null;
    }
    Iterator<TXRegionLockRequestImpl> it = localLocks.iterator();
    while (it.hasNext()) {
      TXRegionLockRequestImpl rlr = it.next();
      if (rlr.getRegionFullPath().equals(regionFullPath)) {
        return rlr;
      }
    }
    return null;
  }

  void addDistributedRequest(TXRegionLockRequest req) {
    if (distLocks == null) {
      distLocks = new ArrayList<>();
    }
    distLocks.add(req);
  }

  public void obtain(InternalDistributedSystem system) throws CommitConflictException {
    if (localLocks != null && !localLocks.isEmpty()) {
      txLocalLock(localLocks);
      localLockHeld = true;
    }
    if (distLocks != null && !distLocks.isEmpty()) {
      distLockId = TXLockService.createDTLS(system).txLock(distLocks, otherMembers);
    }
  }

  /**
   * Release any local locks obtained by this request
   */
  public void releaseLocal() {
    if (localLockHeld) {
      txLocalRelease(localLocks);
      localLockHeld = false;
    }
  }

  /**
   * Release any distributed locks obtained by this request
   */
  public void releaseDistributed(InternalDistributedSystem system) {
    if (distLockId != null) {
      try {
        TXLockService txls = TXLockService.createDTLS(system);
        txls.release(distLockId);
      } catch (IllegalStateException ignore) {
        // IllegalStateException: TXLockService cannot be created
        // until connected to distributed system
        // could be thrown if a jvm is disconnected from the ds,
        // and tries to createDTLS() during clean up
      }
      distLockId = null;
    }
  }

  public TXLockId getDistributedLockId() {
    return distLockId;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getCanonicalName()).append("@").append(System.identityHashCode(this));
    sb.append(" RegionLockRequests:");
    if (localLocks != null) {
      Iterator it = localLocks.iterator();
      while (it.hasNext()) {
        TXRegionLockRequest rlr = (TXRegionLockRequest) it.next();
        sb.append(" TXRegionLockRequest:");
        sb.append(rlr.getRegionFullPath()).append(" keys:").append(rlr.getKeys());
      }
    }
    return sb.toString();
  }

  public void cleanup(InternalDistributedSystem system) {
    releaseLocal();
    releaseDistributed(system);
  }

  @MakeNotStatic
  private static final TXReservationMgr resMgr = new TXReservationMgr(true);

  /**
   * @param localLocks is a list of TXRegionLockRequest instances
   */
  private static void txLocalLock(IdentityArrayList localLocks) throws CommitConflictException {
    resMgr.makeReservation(localLocks);
  }

  private static void txLocalRelease(IdentityArrayList localLocks) {
    resMgr.releaseReservation(localLocks);
  }
}
