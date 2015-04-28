/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;

import java.util.Map;

import com.gemstone.gemfire.distributed.internal.locks.DLockService;

/**
 * MBean that provides access to information and management functionality for a
 * {@link DLockService}.  Since any number of DLockService objects can be created
 * by a member there may be 0 or more instances of this MBean available.
 * 
 * @author rishim
 * @since 7.0
 * 
 */
public interface LockServiceMXBean {

  /**
   * Returns the name of the lock service.
   */
  public String getName();

  /**
   * Returns whether this is a distributed LockService.
   *
   * @return True is this is a distributed LockService, false otherwise.
   */
  public boolean isDistributed();

  /**
   * Returns the number of members using this LockService.
   */
  public int getMemberCount();

  /**
   * Returns of the name of the member which grants the lock.
   */
  public String fetchGrantorMember();

  /**
   * Returns a list of names of the members using this LockService.
   */
  public String[] getMemberNames();

  /**
   * Returns whether this member is the granter.
   * 
   * @return True if this member is the granter, false otherwise.
   */
  public boolean isLockGrantor();

 
  /**
   * Requests that this member become the granter.
   */
  public void becomeLockGrantor();

  /**
   * Returns a map of the names of the objects being locked on and the names of
   * the threads holding the locks.
   */
  public Map<String, String> listThreadsHoldingLock();

  /**
   * Returns a list of names of the locks held by this member's threads.
   */
  public String[] listHeldLocks();

}
