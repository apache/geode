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
   * @param reason the reason the member was suspected
   */
  public void memberSuspect(InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason);
  
  /**
   * This is notification that more than 50% of member weight has been
   * lost in a single view change.  Notification is performed before
   * the view has been installed.
   * @param failures members that have been lost
   * @param remaining members that remain
   */
  public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining);

}
