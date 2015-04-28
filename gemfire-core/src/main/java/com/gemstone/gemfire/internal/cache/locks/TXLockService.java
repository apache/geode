/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache.locks;

import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.distributed.internal.locks.*;

import java.util.*;

/** 
 * Provides transaction locking service for coordinating transactions.
 * <p>
 * This is an abstract class defining the public facade for the transaction
 * locking service. 
 *
 * @author Kirk Lund 
 */
public abstract class TXLockService {
  
  // -------------------------------------------------------------------------
  //   TLS instances
  // -------------------------------------------------------------------------
  
  /** The distributed transaction lock service */
  static TXLockService DTLS = null;
  
  // -------------------------------------------------------------------------
  //   Static methods
  // -------------------------------------------------------------------------
  
  /** Returns the instance of the distributed TXLockService */
  public static TXLockService getDTLS() {
    return DTLS;
  }
  
  /** Returns (or creates) the instance of the distributed TXLockService */
  public static TXLockService createDTLS() {
    synchronized (TXLockService.class) {
      if (DTLS == null || DTLS.isDestroyed()) {
        DTLS = new TXLockServiceImpl(DLockService.DTLS);
      }
      return DTLS;
    }
  }
  
  /** Destroys DTLS in this process to free up resources */
  public static void destroyServices() {
    synchronized (TXLockService.class) {
      if (DTLS != null) {
        DTLS.destroy();
        DTLS = null;
      }
    }
  }
  
  // -------------------------------------------------------------------------
  //   Instance methods
  // -------------------------------------------------------------------------
  
  /** 
   * Requests batch of try locks as scoped by a region.
   *
   * @param regionLockReqs list of TXRegionLockRequests
   *
   * @param participants set of members participating in tx; each member is
   * identified by a serializable <code>IpAddress</code> from JGroups; the 
   * grantor will use this to recover if the tx originator departs; null or
   * empty is allowed
   *
   * @return a generated serializable object to be used as the tx lock 
   * reference (txLockId)
   *
   * @throws IllegalStateException if service is destroyed
   *
   * @throws IllegalArgumentException if regionLockReqs is null or or if either
   * arguments contain instances of unexpected classes 
   */
  public abstract TXLockId txLock(List regionLockReqs, Set participants)
  throws CommitConflictException;
  
  /** 
   * Releases all locks represented by tx lock reference.
   *
   * @param txLockId the tx lock reference as generated from the call to 
   * <code>txLock</code>
   *
   * @throws IllegalStateException if service is destroyed
   *
   * @throws IllegalArgumentException if argument is null or invalid
   */
  public abstract void release(TXLockId txLockId);

  /** 
   * Updates the set of participants for a given tx lock reference.
   *
   * @param txLockId the tx lock reference as generated from the call to 
   * <code>txLock</code>
   * 
   * @param updatedParticipants the set of new participants generated
   * from the advisor for each <code>Region</code> in the transaction.
   *
   * @throws IllegalStateException if service is destroyed
   *
   * @throws IllegalArgumentException if arguments are null or invalid
   */
  public abstract void updateParticipants(TXLockId txLockId, Set updatedParticipants);

    
  /** 
   * Returns true if this process is the lock grantor for this service. 
   *
   * @throws IllegalStateException if service is destroyed
   */
  public abstract boolean isLockGrantor();
  
  /** 
   * Makes this process explicitly become the lock grantor for this service. 
   *
   * @throws IllegalStateException if service is destroyed
   */
  public abstract void becomeLockGrantor();
  
  /** Returns true if this lock service is destroyed. */
  public abstract boolean isDestroyed();
  
  /** 
   * Destroys this tx lock service and removes the static reference to it. 
   */
  public void destroy() {
    synchronized (TXLockService.class) {
      if (!isDestroyed()) {
        basicDestroy();
        if (this == DTLS) DTLS = null;
      }
    }
  }
  
  // -------------------------------------------------------------------------
  //   Hidden template methods
  // -------------------------------------------------------------------------
  
  /** Perfoms basic destroy of this tx lock service */
  abstract void basicDestroy();

}

