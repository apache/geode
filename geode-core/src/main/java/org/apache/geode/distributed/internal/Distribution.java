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
package org.apache.geode.distributed.internal;

import java.io.NotSerializableException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MembershipView;
import org.apache.geode.distributed.internal.membership.api.QuorumChecker;

public interface Distribution {
  void start();

  MembershipView<InternalDistributedMember> getView();

  InternalDistributedMember getLocalMember();

  Set<InternalDistributedMember> send(List<InternalDistributedMember> destinations,
      DistributionMessage msg) throws NotSerializableException;

  Set<InternalDistributedMember> directChannelSend(List<InternalDistributedMember> destinations,
      DistributionMessage content) throws NotSerializableException;

  Map<String, Long> getMessageState(
      DistributedMember member, boolean includeMulticast);

  void waitForMessageState(InternalDistributedMember member,
      Map<String, Long> state) throws InterruptedException, java.util.concurrent.TimeoutException;

  boolean requestMemberRemoval(InternalDistributedMember member,
      String reason);

  boolean verifyMember(InternalDistributedMember mbr,
      String reason);

  <V> V doWithViewLocked(Supplier<V> function);

  boolean memberExists(InternalDistributedMember m);

  boolean isConnected();

  void beSick();

  void beHealthy();

  void playDead();

  boolean isBeingSick();

  void disconnect(boolean beforeJoined);

  void shutdown();

  void shutdownMessageReceived(InternalDistributedMember id,
      String reason);

  void waitForEventProcessing() throws InterruptedException;

  void startEventProcessing();

  void setShutdown();

  void setReconnectCompleted(boolean reconnectCompleted);

  boolean shutdownInProgress();

  void emergencyClose();

  void addSurpriseMemberForTesting(InternalDistributedMember mbr,
      long birthTime);

  void suspectMembers(Set<InternalDistributedMember> members,
      String reason);

  void suspectMember(InternalDistributedMember member,
      String reason);

  Throwable getShutdownCause();

  boolean addSurpriseMember(InternalDistributedMember mbr);

  void startupMessageFailed(InternalDistributedMember mbr,
      String failureMessage);

  boolean testMulticast();

  boolean isSurpriseMember(InternalDistributedMember m);

  QuorumChecker getQuorumChecker();

  DistributedMember getCoordinator();

  Set<InternalDistributedMember> getMembersNotShuttingDown();

  boolean waitForDeparture(InternalDistributedMember mbr)
      throws TimeoutException, InterruptedException;

  boolean waitForDeparture(InternalDistributedMember mbr, long timeoutMs)
      throws TimeoutException, InterruptedException;

  void forceUDPMessagingForCurrentThread();

  void releaseUDPMessagingForCurrentThread();

  /**
   * When the ClusterDistributionManager initiates normal shutdown it should invoke this
   * method so that services will know how to react.
   */
  void setCloseInProgress();

  Membership<InternalDistributedMember> getMembership();

}
