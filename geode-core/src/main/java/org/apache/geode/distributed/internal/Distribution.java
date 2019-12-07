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
import java.util.function.Supplier;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipTestHook;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipView;
import org.apache.geode.distributed.internal.membership.gms.api.QuorumChecker;

public interface Distribution {
  void start();

  MembershipView<InternalDistributedMember> getView();

  InternalDistributedMember getLocalMember();

  Set<InternalDistributedMember> send(InternalDistributedMember[] destinations,
      DistributionMessage msg) throws NotSerializableException;

  Set<InternalDistributedMember> directChannelSend(
      InternalDistributedMember[] destinations,
      DistributionMessage content)
      throws NotSerializableException;

  Map<String, Long> getMessageState(
      DistributedMember member, boolean includeMulticast);

  void waitForMessageState(InternalDistributedMember member,
      Map<String, Long> state) throws InterruptedException;

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

  void registerTestHook(
      MembershipTestHook mth);

  void unregisterTestHook(
      MembershipTestHook mth);

  boolean addSurpriseMember(InternalDistributedMember mbr);

  void startupMessageFailed(InternalDistributedMember mbr,
      String failureMessage);

  boolean testMulticast();

  boolean isSurpriseMember(InternalDistributedMember m);

  QuorumChecker getQuorumChecker();

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
  void installView(GMSMembershipView<InternalDistributedMember> newView);

  // TODO - this method is only used by tests
  @VisibleForTesting
  int getDirectChannelPort();

  void disableDisconnectOnQuorumLossForTesting();

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
}
