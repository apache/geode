/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed.internal;

import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * This interface specifies callback methods that are invoked when
 * remote GemFire systems enter and exit the distributed cache.  Note
 * that a <code>MembershipListener</code> can be added from any VM, but
 * the callback methods are always invoked in the GemFire manager VM.
 * Thus, the callback methods should not perform time-consuming
 * operations.
 *
 * @see DistributionManager#addMembershipListener
 */
public interface MembershipListener {
  
  /**
   * This method is invoked when a new member joins the system
   *
   * @param id
   *        The id of the new member that has joined the system
   */
  public void memberJoined(InternalDistributedMember id);
  
  /**
   * This method is invoked after a member has explicitly left
   * the system.  It may not get invoked if a member becomes unreachable
   * due to crash or network problems.
   *
   * @param id
   *        The id of the new member that has joined the system
   * @param crashed 
   *        True if member did not depart in an orderly manner.
   */
  public void memberDeparted(InternalDistributedMember id, boolean crashed);

  /**
   * This method is invoked after the group membership service has
   * suspected that a member is no longer alive, but has not yet been
   * removed from the membership view
   * @param id the suspected member
   * @param whoSuspected the member that initiated suspect processing
   */
  public void memberSuspect(InternalDistributedMember id,
      InternalDistributedMember whoSuspected);
  
  /**
   * This is notification that more than 50% of member weight has been
   * lost in a single view change.  Notification is performed before
   * the view has been installed.
   * @param failures members that have been lost
   * @param remaining members that remain
   */
  public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining);

}
