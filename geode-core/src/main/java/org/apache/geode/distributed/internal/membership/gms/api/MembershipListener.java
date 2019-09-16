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
package org.apache.geode.distributed.internal.membership.gms.api;

import java.util.List;
import java.util.Set;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipView;

public interface MembershipListener {

  /** this method is invoked when the processing of a new view is completed */
  void viewInstalled(MembershipView view);

  /**
   * this is invoked when there has been a loss of quorum and enable-network-partition-detection is
   * not enabled
   */
  void quorumLost(Set<InternalDistributedMember> failures,
      List<InternalDistributedMember> remainingMembers);

  /**
   * Event indicating that a new member has joined the system.
   *
   * @param m the new member
   */
  void newMemberConnected(DistributedMember m);

  /**
   * Event indicating that a member has left the system
   *
   * @param id the member who has left
   * @param crashed true if the departure was unexpected
   * @param reason a characterization of the departure
   */
  void memberDeparted(InternalDistributedMember id, boolean crashed, String reason);

  /**
   * Event indicating that a member is suspected of having departed but is still in the membership
   * view
   */
  void memberSuspect(InternalDistributedMember suspect, InternalDistributedMember whoSuspected,
      String reason);

  /**
   * Event indicating that the membership service has failed catastrophically.
   *
   */
  void membershipFailure(String reason, Throwable t);

  /**
   * Save the configuration before a force disconnect.
   *
   * This method should probably be merged with memberFailure, but currently the
   * places we save the configuration don't line up with where we call
   * memberFailure.
   */
  void saveConfig();

}
