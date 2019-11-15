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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipTestHook;
import org.apache.geode.distributed.internal.membership.MembershipView;
import org.apache.geode.distributed.internal.membership.QuorumChecker;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.api.Membership;

public interface Distribution {
  void start();

  MembershipView getView();

  InternalDistributedMember getLocalMember();

  Set<InternalDistributedMember> send(InternalDistributedMember[] destinations,
      DistributionMessage msg) throws NotSerializableException;

  Set<InternalDistributedMember> directChannelSend(
      InternalDistributedMember[] destinations,
      DistributionMessage content)
      throws NotSerializableException;

  Map<String, Long> getMessageState(
      DistributedMember member, boolean includeMulticast);

  void waitForMessageState(DistributedMember member,
      Map<String, Long> state) throws InterruptedException;

  boolean requestMemberRemoval(DistributedMember member,
      String reason);

  boolean verifyMember(DistributedMember mbr,
      String reason);

  boolean isShunned(DistributedMember m);

  <V> V doWithViewLocked(
      Function<Membership, V> function);

  boolean memberExists(DistributedMember m);

  boolean isConnected();

  void beSick();

  void playDead();

  boolean isBeingSick();

  void disconnect(boolean beforeJoined);

  void shutdown();

  void shutdownMessageReceived(DistributedMember id,
      String reason);

  void waitForEventProcessing() throws InterruptedException;

  void startEventProcessing();

  void setShutdown();

  void setReconnectCompleted(boolean reconnectCompleted);

  boolean shutdownInProgress();

  void emergencyClose();

  void addSurpriseMemberForTesting(DistributedMember mbr,
      long birthTime);

  void suspectMembers(Set<DistributedMember> members,
      String reason);

  void suspectMember(DistributedMember member,
      String reason);

  Throwable getShutdownCause();

  void registerTestHook(
      MembershipTestHook mth);

  void unregisterTestHook(
      MembershipTestHook mth);

  boolean addSurpriseMember(DistributedMember mbr);

  void startupMessageFailed(DistributedMember mbr,
      String failureMessage);

  boolean testMulticast();

  boolean isSurpriseMember(DistributedMember m);

  QuorumChecker getQuorumChecker();

  void releaseQuorumChecker(
      QuorumChecker checker,
      InternalDistributedSystem distributedSystem);

  DistributedMember getCoordinator();

  Set<InternalDistributedMember> getMembersNotShuttingDown();

  Services getServices();

  // TODO - this method is only used by tests
  @VisibleForTesting
  void forceDisconnect(String reason);

  // TODO - this method is only used by tests
  @VisibleForTesting
  void replacePartialIdentifierInMessage(DistributionMessage message);

  // TODO - this method is only used by tests
  @VisibleForTesting
  boolean isCleanupTimerStarted();

  // TODO - this method is only used by tests
  @VisibleForTesting
  long getSurpriseMemberTimeout();

  // TODO - this method is only used by tests
  @VisibleForTesting
  void installView(GMSMembershipView newView);

  // TODO - this method is only used by tests
  @VisibleForTesting
  int getDirectChannelPort();

  void disableDisconnectOnQuorumLossForTesting();

  boolean waitForDeparture(DistributedMember mbr)
      throws TimeoutException, InterruptedException;

  boolean waitForDeparture(DistributedMember mbr, long timeoutMs)
      throws TimeoutException, InterruptedException;

  void forceUDPMessagingForCurrentThread();

  void releaseUDPMessagingForCurrentThread();
}
