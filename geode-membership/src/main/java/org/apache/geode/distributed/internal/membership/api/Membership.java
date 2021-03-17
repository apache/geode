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
package org.apache.geode.distributed.internal.membership.api;

import java.io.NotSerializableException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;

/**
 * Membership encapsulates membership services. Use MembershipBuilder to create a
 * Membership instance. Create and install a MembershipListener implementation using
 * the builder if you want to receive notification of membership events. Create and install
 * a LifecycleListener using the builder if you want to receive notification of lifecycle
 * events (start/disconnect/etc.). Create and install a MessageListener to receive messages
 * sent by other members over UDP communications.
 * <p>
 * Once you've created a Membership instance use the start() method to boot it up and
 * join the cluster.
 */
public interface Membership<ID extends MemberIdentifier> {
  /**
   * Fetch the current view of memberships in th distributed system, as an ordered list.
   *
   * @return list of members
   */
  MembershipView<ID> getView();

  /**
   * Return a {@link ID} representing the current system
   *
   * @return an address corresponding to the current system
   */
  ID getLocalMember();

  /**
   * @param destinations list of members to send the message to. A list of length 1 with
   *        <em>null</em> as a single element broadcasts to all members of the system.
   * @param content the message to send
   * @return list of members who did not receive the message. If
   *         {@link Message#ALL_RECIPIENTS} is given as thelist of recipients, this
   *         return list is null (empty). Otherwise, this list is all of those recipients that did
   *         not receive the message because they departed the distributed system.
   * @throws NotSerializableException If content cannot be serialized
   */
  Set<ID> send(ID[] destinations, Message<ID> content)
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
  Map<String, Long> getMessageState(ID member, boolean includeMulticast,
      Map<String, Long> result);

  /**
   * Waits for the given communications to reach the associated state
   *
   * @param member The member whose messaging state we're waiting for
   * @param state The message states to wait for. This should come from getMessageStates
   * @throws InterruptedException Thrown if the thread is interrupted
   * @since GemFire 5.1
   */
  void waitForMessageState(ID member, Map<String, Long> state)
      throws InterruptedException, TimeoutException;

  /**
   * Request the current membership coordinator to remove the given member
   */
  boolean requestMemberRemoval(ID member, String reason) throws MemberDisconnectedException;

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
  boolean verifyMember(ID mbr, String reason);

  /**
   * Returns true if the member is being shunned
   */
  boolean isShunned(ID m);

  /**
   * execute code with the membership view locked so that it doesn't change
   */
  <V> V doWithViewLocked(Supplier<V> function);

  /**
   * Sanity checking, esp. for elder processing. Does the existing member (still) exist in our view?
   *
   * @param m the member
   * @return true if it still exists
   */
  boolean memberExists(ID m);

  /**
   * Is this manager still connected? If it has not been initialized, this method will return true;
   * otherwise it indicates whether the connection is stil valid
   *
   * @return true if the manager is still connected.
   */
  boolean isConnected();

  /**
   * test method for simulating a sick/dead member
   */
  void beSick();

  /**
   * test method for simulating a sick/dead member
   */
  void playDead();

  /**
   * test method for simulating a sick/dead member
   */
  void beHealthy();

  /**
   * A test hook for healthiness tests
   */
  boolean isBeingSick();

  /**
   * Instructs this manager to shut down
   *
   * @param beforeJoined whether we've joined the cluster or not
   */
  void disconnect(boolean beforeJoined);

  /**
   * Instruct this manager to release resources
   */
  void shutdown();

  /**
   * Forcibly shut down the manager and inform its listeners of the failure
   */
  void uncleanShutdown(String reason, Exception e);

  /**
   * A shutdown message has been received from another member
   */
  void shutdownMessageReceived(ID id, String reason);

  /**
   * Stall the current thread until we are ready to accept view events
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   * @see #startEventProcessing()
   */
  void waitForEventProcessing() throws InterruptedException;

  /**
   * Commence delivering events to my listener.
   *
   * @see #waitForEventProcessing()
   */
  void startEventProcessing();

  /**
   * Indicates to the membership manager that the system is shutting down. Typically speaking, this
   * means that new connection attempts are to be ignored and disconnect failures are to be (more)
   * tolerated.
   *
   */
  void setShutdown();

  /**
   * informs the membership manager that a reconnect has been completed
   */
  void setReconnectCompleted(boolean reconnectCompleted);

  /**
   * Determine whether GCS shutdown has commenced
   *
   * @return true if it is shutting down
   */
  boolean shutdownInProgress();

  /**
   * Returns true if remoteId is an existing member, otherwise waits till timeout. Returns false if
   * remoteId is not confirmed to be a member.
   *
   * @return true if membership is confirmed, else timeout and false
   */
  boolean waitForNewMember(ID remoteId);

  /**
   * Release critical resources, avoiding any possibility of deadlock
   */
  void emergencyClose();

  /**
   * Notifies the manager that a member has contacted us who is not in the current membership view
   *
   */
  void addSurpriseMemberForTesting(ID mbr, long birthTime);

  /**
   * Initiate SUSPECT processing for the given members. This may be done if the members have not
   * been responsive. If they fail SUSPECT processing, they will be removed from membership.
   */
  void suspectMembers(Set<ID> members, String reason);

  /**
   * Initiate SUSPECT processing for the given member. This may be done if the member has not been
   * responsive. If it fails SUSPECT processing, it will be removed from membership.
   */
  void suspectMember(ID member, String reason);

  /**
   * if the manager initiated shutdown, this will return the cause of abnormal termination of
   * membership management in this member
   *
   * @return the exception causing shutdown
   */
  Throwable getShutdownCause();

  boolean addSurpriseMember(ID mbr);

  /**
   * if a StartupMessage is going to reject a new member, this should be used to make sure we don't
   * keep that member on as a "surprise member"
   *
   * @param mbr the failed member
   * @param failureMessage the reason for the failure (e.g., license limitation)
   */
  void startupMessageFailed(ID mbr, String failureMessage);

  /**
   * @return true if multicast is disabled, or if multicast is enabled and seems to be working
   */
  boolean testMulticast();

  /**
   * Returns true if the member is a surprise member.
   *
   * @param m the member in question
   * @return true if the member is a surprise member
   */
  boolean isSurpriseMember(ID m);

  /**
   * After a forced-disconnect this method should be used once before attempting to use
   * quorumCheckForAutoReconnect().
   *
   * @return the quorum checker to be used in reconnecting the system
   */
  QuorumChecker getQuorumChecker();

  /**
   * return the coordinator for the view.
   */
  ID getCoordinator();

  /**
   * return a list of all members excluding those in the process of shutting down
   */
  Set<ID> getMembersNotShuttingDown();

  /**
   * Process a message and pass it on to the {@link MessageListener} that was configured
   * in {@link MembershipBuilder#setMessageListener(MessageListener)}. This method
   * takes care of queueing up the message during startup and filtering out messages
   * from shunned members, before calling the message listener.
   */
  void processMessage(Message<ID> msg) throws MemberShunnedException;

  void checkCancelled() throws MembershipClosedException;

  void waitIfPlayingDead();

  boolean isJoining();

  ID[] getAllMembers(final ID[] arrayType);

  /**
   * TODO - this is very similar to {@link #memberExists(ID)}. However, this
   * is looking at {@link GMSJoinLeave#getView()} rather than {@link Membership#getView()}.
   * Should we remove this in favor of member exists?
   */
  boolean hasMember(ID member);

  void start() throws MemberStartupException;

  void setCloseInProgress();
}
