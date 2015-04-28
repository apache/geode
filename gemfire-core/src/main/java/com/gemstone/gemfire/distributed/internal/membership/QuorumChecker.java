/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed.internal.membership;

/**
 * A QuorumChecker is created after a forced-disconnect in order
 * to probe the network to see if there is a quorum of members
 * that can be contacted.
 * 
 * @author bschuchardt
 *
 */
public interface QuorumChecker {
  
  /**
   * Check to see if a quorum of the old members are reachable
   * @param timeoutMS time to wait for responses, in milliseconds
   */
  public boolean checkForQuorum(long timeoutMS) throws InterruptedException;
  
  /**
   * suspends the quorum checker for an attempt to connect to the distributed system
   */
  public void suspend();
  
  /**
   * resumes the quorum checker after having invoked suspend();
   */
  public void resume();

  /**
   * Get the membership info from the old system that needs to be passed
   * to the one that is reconnecting.
   */
  public Object getMembershipInfo();
}
