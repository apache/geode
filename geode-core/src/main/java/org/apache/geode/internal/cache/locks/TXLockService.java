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

package org.apache.geode.internal.cache.locks;

import java.util.List;
import java.util.Set;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockService;

/**
 * Provides transaction locking service for coordinating transactions.
 * <p>
 * This is an abstract class defining the public facade for the transaction locking service.
 *
 */
public abstract class TXLockService {

  // -------------------------------------------------------------------------
  // TLS instances
  // -------------------------------------------------------------------------

  /** The distributed transaction lock service */
  @MakeNotStatic
  static TXLockService DTLS = null;

  // -------------------------------------------------------------------------
  // Static methods
  // -------------------------------------------------------------------------

  /** Returns the instance of the distributed TXLockService */
  public static TXLockService getDTLS() {
    return DTLS;
  }

  /** Returns (or creates) the instance of the distributed TXLockService */
  public static TXLockService createDTLS(InternalDistributedSystem system) {
    synchronized (TXLockService.class) {
      if (DTLS == null || DTLS.isDestroyed()) {
        DTLS = new TXLockServiceImpl(DLockService.DTLS, system);
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
  // Instance methods
  // -------------------------------------------------------------------------

  /**
   * Requests batch of try locks as scoped by a region.
   *
   * @param regionLockReqs list of TXRegionLockRequests
   *
   * @param participants set of members participating in tx; each member is identified by a
   *        serializable <code>IpAddress</code> from JGroups; the grantor will use this to recover
   *        if the tx originator departs; null or empty is allowed
   *
   * @return a generated serializable object to be used as the tx lock reference (txLockId)
   *
   * @throws IllegalStateException if service is destroyed
   *
   * @throws IllegalArgumentException if regionLockReqs is null or or if either arguments contain
   *         instances of unexpected classes
   */
  public abstract TXLockId txLock(List regionLockReqs, Set participants)
      throws CommitConflictException;

  /**
   * Releases all locks represented by tx lock reference.
   *
   * @param txLockId the tx lock reference as generated from the call to <code>txLock</code>
   *
   * @throws IllegalStateException if service is destroyed
   *
   * @throws IllegalArgumentException if argument is null or invalid
   */
  public abstract void release(TXLockId txLockId);

  /**
   * Updates the set of participants for a given tx lock reference.
   *
   * @param txLockId the tx lock reference as generated from the call to <code>txLock</code>
   *
   * @param updatedParticipants the set of new participants generated from the advisor for each
   *        <code>Region</code> in the transaction.
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
        if (this == DTLS) {
          DTLS = null;
        }
      }
    }
  }

  // -------------------------------------------------------------------------
  // Hidden template methods
  // -------------------------------------------------------------------------

  /** Perfoms basic destroy of this tx lock service */
  abstract void basicDestroy();

}
