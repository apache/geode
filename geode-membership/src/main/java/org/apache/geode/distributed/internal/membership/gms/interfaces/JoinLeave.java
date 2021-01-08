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
package org.apache.geode.distributed.internal.membership.gms.interfaces;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;

/**
 * The JoinLeave service is responsible for joining and leaving the cluster. It must
 * also fill the role of cluster coordinator and respond to join/leave/remove-member
 * events by sending out new membership views when appropriate.
 */
public interface JoinLeave<ID extends MemberIdentifier> extends Service<ID> {

  /**
   * joins the distributed system.
   *
   * @throws MemberStartupException if there was a problem joining the cluster after membership
   *         configuration has
   *         completed.
   * @throws MembershipConfigurationException if operation either timed out, was stopped or locator
   *         does not exist.
   */
  void join() throws MemberStartupException;

  /**
   * leaves the distributed system. Should be invoked before stop()
   */
  void leave();

  /**
   * force another member out of the system
   */
  void remove(ID m, String reason);

  /**
   * Invoked by the Manager, this notifies the HealthMonitor that a ShutdownMessage has been
   * received from the given member
   */
  void memberShutdown(ID mbr, String reason);

  /**
   * returns the local address
   */
  ID getMemberID();

  /**
   * Get canonical "GMSMember" from current view or prepared view.
   */
  ID getMemberID(ID m);

  /**
   * returns the current membership view
   */
  GMSMembershipView<ID> getView();


  /**
   * returns the last known view prior to close - for reconnecting
   */
  GMSMembershipView<ID> getPreviousView();

  /**
   * check to see if a member is already in the process of leaving or being removed (in the next
   * view)
   */
  boolean isMemberLeaving(ID mbr);

  /**
   * test hook
   */
  void disableDisconnectOnQuorumLossForTesting();
}
