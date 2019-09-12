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

import java.io.NotSerializableException;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipView;

public interface Membership {
  /**
   * Fetch the current view of memberships in th distributed system, as an ordered list.
   *
   * @return list of members
   */
  MembershipView getView();

  /**
   * Return a {@link InternalDistributedMember} representing the current system
   *
   * @return an address corresponding to the current system
   */
  InternalDistributedMember getLocalMember();

  /**
   * @param destinations list of members to send the message to. A list of length 1 with
   *        <em>null</em> as a single element broadcasts to all members of the system.
   * @param content the message to send
   * @return list of members who did not receive the message. If
   *         {@link DistributionMessage#ALL_RECIPIENTS} is given as thelist of recipients, this
   *         return list is null (empty). Otherwise, this list is all of those recipients that did
   *         not receive the message because they departed the distributed system.
   * @throws NotSerializableException If content cannot be serialized
   */
  Set<InternalDistributedMember> send(InternalDistributedMember[] destinations,
      DistributionMessage content)
      throws NotSerializableException;

  /**
   * Returns a serializable map of communications state for use in state stabilization.
   *
   * @param member the member whose message state is to be captured
   * @param includeMulticast whether the state of the mcast messaging should be included
   * @return the current state of the communication channels between this process and the given
   *         distributed member
   * @since GemFire 5.1
   */
  Map<String, Long> getMessageState(DistributedMember member, boolean includeMulticast);

  /**
   * Waits for the given communications to reach the associated state
   *
   * @param member The member whose messaging state we're waiting for
   * @param state The message states to wait for. This should come from getMessageStates
   * @throws InterruptedException Thrown if the thread is interrupted
   * @since GemFire 5.1
   */
  void waitForMessageState(DistributedMember member, Map<String, Long> state)
      throws InterruptedException;

  /**
   * Request the current membership coordinator to remove the given member
   */
  boolean requestMemberRemoval(DistributedMember member, String reason);

  /**
   * like memberExists() this checks to see if the given ID is in the current membership view. If it
   * is in the view though we try to connect to its failure-detection port to see if it's still
   * around. If we can't then suspect processing is initiated on the member with the given reason
   * string.
   *
   * @param mbr the member to verify
   * @param reason why the check is being done (must not be blank/null)
   * @return true if the member checks out
   */
  boolean verifyMember(DistributedMember mbr, String reason);

  /**
   * Returns true if the member is being shunned
   */
  boolean isShunned(DistributedMember m);

  <T> T withViewLock(Function function);
}
