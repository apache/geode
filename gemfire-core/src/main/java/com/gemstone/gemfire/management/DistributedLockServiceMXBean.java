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

import com.gemstone.gemfire.distributed.DistributedLockService;


/**
 * MBean that provides access to information for a named instance of {@link DistributedLockService}.
 * Since any number of DistributedLockService objects can be created by a member there may be 0 or
 * more instances of this MBean available.
 * 
 * @author rishim
 * @since 7.0
 * 
 */
public interface DistributedLockServiceMXBean {

  /**
   * Returns the name of the LockService.
   */
  public String getName();

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
   * Returns a map of the names of the objects being locked on and the names of
   * the threads holding the locks.
   */
  public Map<String, String> listThreadsHoldingLock();

  /**
   * Returns a list of names of the locks held by this member's threads.
   */
  public String[] listHeldLocks();

}
