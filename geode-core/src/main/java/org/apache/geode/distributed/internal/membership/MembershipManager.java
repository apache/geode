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
package com.gemstone.gemfire.distributed.internal.membership;

import java.io.NotSerializableException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;

/**
 * A MembershipManager is responsible for reporting a MemberView, as well as
 * having explicit protocol for leaving or joining the distributed system.<p>
 *
 * Note that it is imperative to send a new manager a postConnect message
 * after instantiation.
 * 
 * 
 */
public interface MembershipManager {

  /** this must be sent to the manager after instantiation to allow
   *  it to perform post-connection chores
   */
  public void postConnect();
  
  /**
   * Fetch the current view of memberships in th distributed system,
   * as an ordered list.
   * 
   * @return list of members
   */
  public NetView getView();
  /**
   * Returns an object that is used to sync access to the view.
   * While this lock is held the view can't change.
   * @since GemFire 5.7
   */
  public ReadWriteLock getViewLock();

  /**
   * Return a {@link InternalDistributedMember} representing the current system
   * 
   * @return an address corresponding to the current system
   */
  public InternalDistributedMember getLocalMember();

  /**
   * Sanity checking, esp. for elder processing.  Does the existing
   * member (still) exist in our view?
   * 
   * @param m the member
   * @return true if it still exists
   */
  public boolean memberExists(DistributedMember m);
  
  /**
   * Is this manager still connected?  If it has not been initialized, this
   * method will return true; otherwise it indicates whether the connection is
   * stil valid
   * @return true if the manager is still connected.
   */
  public boolean isConnected();
  
  
  /**
   * A test hook for healthiness tests
   */
  public boolean isBeingSick();
  
  /**
   * Instruct this manager to release resources
   */
  public void shutdown();
  
  /**
   * Forcibly shut down the manager and inform its listeners of the failure
   */
  public void uncleanShutdown(String reason, Exception e);
  
  /**
   * A shutdown message has been received from another member
   */
  public void shutdownMessageReceived(InternalDistributedMember id, String reason);
  
  /**
   * Stall the current thread until we are ready to
   * accept view events
   * 
   * @throws InterruptedException if the thread is interrupted while waiting
   * @see #startEventProcessing()
   */
  public void waitForEventProcessing() throws InterruptedException;
  
  /**
   * Commence delivering events to my listener.
   * 
   * @see #waitForEventProcessing()
   */
  public void startEventProcessing();
  
  
  /**
   * @param destinations list of members to send the message to.  A list of
   * length 1 with <em>null</em> as a single element broadcasts to all members
   * of the system.
   * @param content the message to send
   * @param stats the statistics object to update
   * @return list of members who did not receive the message.  If 
   * {@link DistributionMessage#ALL_RECIPIENTS} is given as thelist of recipients,
   * this return list is null (empty).  Otherwise, this list is all of those
   * recipients that did not receive the message because they departed the
   * distributed system.
   * @throws NotSerializableException
   *         If content cannot be serialized
   */
  public Set send(
      InternalDistributedMember[] destinations,
      DistributionMessage content,
      DMStats stats)
  throws NotSerializableException;
  
  /**
   * Indicates to the membership manager that the system is shutting down.
   * Typically speaking, this means that new connection attempts are to be
   * ignored and disconnect failures are to be (more) tolerated.
   *
   */
  public void setShutdown();
  

  /**
   * Determine whether GCS shutdown has commenced
   * @return true if it is shutting down
   */
  public boolean shutdownInProgress();

  /**
   * Returns a serializable map of communications state for
   * use in state stabilization.
   * @param member
   *    the member whose message state is to be captured
   * @param includeMulticast
   *    whether the state of the mcast messaging should be included
   * @return the current state of the communication channels between this
   *    process and the given distributed member
   * @since GemFire 5.1
   */
  public Map getMessageState(DistributedMember member, boolean includeMulticast);

  /**
   * Waits for the given communications to reach the associated
   * state
   * @param member
   *    The member whose messaging state we're waiting for
   * @param state
   *    The message states to wait for.  This should come from getMessageStates
   * @throws InterruptedException
   *    Thrown if the thread is interrupted
   * @since GemFire 5.1
   */
  public void waitForMessageState(DistributedMember member, Map state)
    throws InterruptedException;
  
  /**
   * Wait for the given member to not be in the membership view and for all direct-channel
   * receivers for this member to be closed.
   * @param mbr  the member
   * @return for testing purposes this returns true if the serial queue for the member was flushed
   * @throws InterruptedException if interrupted by another thread
   * @throws TimeoutException  if we wait too long for the member to go away
   */
  public boolean waitForDeparture(DistributedMember mbr) throws TimeoutException, InterruptedException;
  
  /**
   * Returns true if remoteId is an existing member, otherwise waits till
   * timeout. Returns false if remoteId is not confirmed to be a member.
   * 
   * @param remoteId
   * @return true if membership is confirmed, else timeout and false
   */
  public boolean waitForNewMember(InternalDistributedMember remoteId);

  /**
   * Release critical resources, avoiding any possibility
   * of deadlock
   * 
   * @see SystemFailure#emergencyClose()
   */
  public void emergencyClose();
  
  /**
   * Request the current membership coordinator to remove the given member
   */
  public boolean requestMemberRemoval(DistributedMember member, String reason);
  
  /**
   * like memberExists() this checks to see if the given ID is in the current
   * membership view.  If it is in the view though we try to connect to its
   * failure-detection port to see if it's still around.  If we can't then
   * suspect processing is initiated on the member with the given reason string.
   * @param mbr the member to verify
   * @param reason why the check is being done (must not be blank/null)
   * @return true if the member checks out
   */
  public boolean verifyMember(DistributedMember mbr, String reason);

  
  /**
   * Initiate SUSPECT processing for the given members.  This may be done if
   * the members have not been responsive.  If they fail SUSPECT processing,
   * they will be removed from membership.
   */
  public void suspectMembers(Set members, String reason);
  
  /**
   * Initiate SUSPECT processing for the given member.  This may be done if
   * the member has not been responsive.  If it fails SUSPECT processing,
   * it will be removed from membership.
   */
  public void suspectMember(DistributedMember member, String reason);
  
  /**
   * if the manager initiated shutdown, this will return the cause
   * of abnormal termination of membership management in this member
   * @return the exception causing shutdown
   */
  public Throwable getShutdownCause();
  
  /**
   * register a test hook for membership events
   * @see MembershipTestHook
   */
  public void registerTestHook(MembershipTestHook mth);
  
  /**
   * remove a test hook previously registered with the manager
   */
  public void unregisterTestHook(MembershipTestHook mth);

  /**
   * If this member is shunned, ensure that a warning is generated at least once.
   * 
   * @param mbr the member that may be shunned
   */
  public void warnShun(DistributedMember mbr);
  
  public boolean addSurpriseMember(DistributedMember mbr);
  
  /** if a StartupMessage is going to reject a new member, this should be used
   * to make sure we don't keep that member on as a "surprise member"
   * @param mbr the failed member
   * @param failureMessage the reason for the failure (e.g., license limitation)
   */
  public void startupMessageFailed(DistributedMember mbr, String failureMessage);

  /**
   * @return true if multicast is disabled, or if multicast is enabled and seems to be working
   */
  public boolean testMulticast();
  
  /**
   * Returns true if the member is a surprise member.
   *
   * @param m the member in question
   * @return true if the member is a surprise member
   */
  public boolean isSurpriseMember(DistributedMember m);
  
  /**
   * Returns true if the member is being shunned
   */
  public boolean isShunned(DistributedMember m);

  /**
   * Forces use of UDP for communications in the current thread.  UDP is
   * connectionless, so no tcp/ip connections will be created or used for
   * messaging until this setting is released with releaseUDPMessagingForCurrentThread.
   */
  public void forceUDPMessagingForCurrentThread();
  
  /**
   * Releases use of UDP for all communications in the current thread,
   * as established by forceUDPMessagingForCurrentThread.
   */
  public void releaseUDPMessagingForCurrentThread();
  
  
  /**
   * After a forced-disconnect this method should be used once before
   * attempting to use quorumCheckForAutoReconnect().
   * @return the quorum checker to be used in reconnecting the system
   */
  public QuorumChecker getQuorumChecker();
  
  
  /**
   * Frees resources used for quorum checks during auto-reconnect
   * polling.  Invoke this method when you're all done using
   * the quorum checker.
   * @param checker the QuorumChecker instance
   */
  public void releaseQuorumChecker(QuorumChecker checker);
  
}
