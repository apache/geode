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

package org.apache.geode.internal.cache;
import org.apache.geode.cache.*;
import org.apache.geode.internal.cache.locks.*;

import java.util.*;

/** TXLockRequest represents all the locks that need to be made
 * for a single transaction.
 *
 * 
 * @since GemFire 4.0
 * 
 */
public class TXLockRequest {
  private boolean localLockHeld;
  private TXLockId distLockId;
  private IdentityArrayList localLocks;  // of TXRegionLockRequest
  private ArrayList<TXRegionLockRequest> distLocks; // of TXRegionLockRequest
  private Set otherMembers;

  public TXLockRequest()
  {
    this.localLockHeld = false;
    this.distLockId = null;
    this.localLocks = null;
    this.distLocks = null;
    this.otherMembers = null;
  }
  void setOtherMembers(Set s) {
    this.otherMembers = s;
  }
  public void addLocalRequest(TXRegionLockRequest req) {
    if (this.localLocks == null) {
      this.localLocks = new IdentityArrayList();
    }
    this.localLocks.add(req);
  }
  public TXRegionLockRequest getRegionLockRequest(String regionFullPath) {
    if (this.localLocks == null || regionFullPath == null) {
      return null;
    }
    Iterator<TXRegionLockRequestImpl> it = this.localLocks.iterator();
    while (it.hasNext()) {
      TXRegionLockRequestImpl rlr = it.next();
      if (rlr.getRegionFullPath().equals(regionFullPath)) {
        return rlr;
      }
    }
    return null;
  }
  void addDistributedRequest(TXRegionLockRequest req) {
    if (this.distLocks == null) {
      this.distLocks = new ArrayList<TXRegionLockRequest>();
    }
    this.distLocks.add(req);
  }
  public void obtain() throws CommitConflictException {
    if (this.localLocks != null && !this.localLocks.isEmpty()) {
      txLocalLock(this.localLocks);
      this.localLockHeld = true;
    }
    if (this.distLocks != null && !this.distLocks.isEmpty()) {
      this.distLockId = TXLockService.createDTLS().txLock(this.distLocks,
                                                          this.otherMembers);
    }
  }
  /**
   * Release any local locks obtained by this request
   */
  public void releaseLocal() {
    if (this.localLockHeld) {
      txLocalRelease(this.localLocks);
      this.localLockHeld = false;
    }
  }
  /**
   * Release any distributed locks obtained by this request
   */
  public void releaseDistributed() {
    if (this.distLockId != null) {
      try {
        TXLockService txls = TXLockService.createDTLS();
        txls.release(this.distLockId);
      } catch (IllegalStateException ignore) {
        //IllegalStateException: TXLockService cannot be created 
        //until connected to distributed system
        //could be thrown if a jvm is disconnected from the ds, 
        //and tries to createDTLS() during clean up
      }
      this.distLockId = null;
    }
  }

  public TXLockId getDistributedLockId() {
    return this.distLockId;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getCanonicalName()).append("@").append(System.identityHashCode(this));
    sb.append(" RegionLockRequests:");
    if (this.localLocks != null) {
      Iterator it = this.localLocks.iterator();
      while (it.hasNext()) {
        TXRegionLockRequest rlr = (TXRegionLockRequest) it.next();
        sb.append(" TXRegionLockRequest:");
        sb.append(rlr.getRegionFullPath()).append(" keys:").append(rlr.getKeys());
      }
    }
    return sb.toString();
  }

  public void cleanup() {
    releaseLocal();
    releaseDistributed();
  }
  static private final TXReservationMgr resMgr = new TXReservationMgr(true);

  /**
   * @param localLocks is a list of TXRegionLockRequest instances
   */
  private static void txLocalLock(IdentityArrayList localLocks)
    throws CommitConflictException
  {
    resMgr.makeReservation(localLocks);
  }
  private static void txLocalRelease(IdentityArrayList localLocks) {
    resMgr.releaseReservation(localLocks);
  }
}
