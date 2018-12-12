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
package org.apache.geode.distributed.internal.membership;

import java.util.List;
import java.util.Set;

import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.direct.DirectChannelListener;

public interface DistributedMembershipListener extends DirectChannelListener {

  /** this method is invoked when the processing of a new view is completed */
  void viewInstalled(NetView view);

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
  void newMemberConnected(InternalDistributedMember m);

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
   * Event indicating a message has been delivered that we need to process.
   *
   * @param o the message that should be processed.
   */
  void messageReceived(DistributionMessage o);

  /**
   * Indicates whether, during the shutdown sequence, if other members of the distributed system
   * have been notified.
   *
   * This allows a membership manager to identify potential race conditions during the shutdown
   * process.
   *
   * @return true if other members of the distributed system have been notified.
   */
  boolean isShutdownMsgSent();

  /**
   * Event indicating that the membership service has failed catastrophically.
   *
   */
  void membershipFailure(String reason, Throwable t);

  /**
   * Support good logging on this listener
   *
   * @return a printable string for this listener
   */
  String toString();

}
