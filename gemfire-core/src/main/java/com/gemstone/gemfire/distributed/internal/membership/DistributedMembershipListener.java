/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership;

import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;

public interface DistributedMembershipListener {

  /** this method is invoked when the processing of a new view is completed */
  public void viewInstalled(NetView view);
  
  /** this is invoked when there has been a loss of quorum and enable-network-partition-detection is not enabled */
  public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remainingMembers);
  
  /**
   * Event indicating that a new member has joined the system.
   * @param m the new member
   * @param stub the stub, if any, representing communication to this member
   */
  public void newMemberConnected(InternalDistributedMember m, 
      com.gemstone.gemfire.internal.tcp.Stub stub);

  /**
   * Event indicating that a member has left the system
   * 
   * @param id the member who has left
   * @param crashed true if the departure was unexpected
   * @param reason a characterization of the departure
   */
  public void memberDeparted(InternalDistributedMember id, boolean crashed, String reason);
  
  /**
   * Event indicating that a member is suspected of having departed but
   * is still in the membership view
   */
  public void memberSuspect(InternalDistributedMember suspect, InternalDistributedMember whoSuspected);

  /**
   * Event indicating a message has been delivered that we need to process.
   * 
   * @param o the message that should be processed.
   */
  public void messageReceived(DistributionMessage o);
  
  /**
   * Indicates whether, during the shutdown sequence, if other members
   * of the distributed system have been notified.
   * 
   * This allows a membership manager to identify potential race conditions during
   * the shutdown process.
   * 
   * @return true if other members of the distributed system have been notified.
   */
  public boolean isShutdownMsgSent();
  
  /**
   * Event indicating that the membership service has failed catastrophically.
   *
   */
  public void membershipFailure(String reason, Throwable t);
  
 /**
   * Support good logging on this listener
   * @return a printable string for this listener
   */
  public String toString();
  
  /**
   * Return the distribution manager for this receiver
   * @return the distribution manager
   */
  public DistributionManager getDM();
  
}
