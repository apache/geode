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

import java.io.NotSerializableException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.Role;
import com.gemstone.gemfire.distributed.internal.locks.ElderState;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.internal.Version;

/**
 * This interface defines the services provided  by any class that
 * is a distribution manager.
 *
 *
 *
 */
public interface DM extends ReplySender {
  
  public boolean shutdownInProgress();
  
  /**
   * Returns the current "cache time" in milliseconds since the epoch.
   * The "cache time" takes into account skew among the local clocks
   * on the various machines involved in the cache.
   */
  public long cacheTimeMillis();
  /**
   * Returns the id of this distribution manager.
   */
  public InternalDistributedMember getDistributionManagerId();

  /**
   * Get a set of all other members (both admin ones and normal).
   * @since GemFire 5.7
   */
  public Set getAllOtherMembers();

  /**
   * Returns the ID in the membership view that is equal to the argument.
   * If the ID is not in the view, the argument is returned.
   */
  public InternalDistributedMember getCanonicalId(DistributedMember id);
  
  /**
   * removes members that have older versions from the given collection, typically
   * a Set from a distribution advisor
   * @since GemFire 8.0
   */
  public void retainMembersWithSameOrNewerVersion(Collection<InternalDistributedMember> members, Version version);

  /**
   * removes members that have the given version or later from the given collection,
   * typically a Set from a distribution advisor
   * @since GemFire 8.0
   */
  public void removeMembersWithSameOrNewerVersion(Collection<InternalDistributedMember> members, Version version);

  /**
   * Returns an unmodifiable set containing the identities of all of
   * the known distribution managers. As of 7.0 this includes locators
   * since they have a cache.
   */
  public Set getDistributionManagerIds();
  /**
   * Returns an unmodifiable set containing the identities of all of
   * the known "normal" distribution managers. 
   * This does not include locators or admin members.
   */
  public Set getNormalDistributionManagerIds();

  /**
   * Returns an unmodifiable set containing the identities of all of
   * the known distribution managers including admin members.
   * @since GemFire 5.7
   */
  public Set getDistributionManagerIdsIncludingAdmin();

  /**
   * Returns a private-memory list containing getDistributionManagerIds()
   * minus our id.
   */
  public Set getOtherDistributionManagerIds();
  /**
   * Returns a private-memory list containing getNormalDistributionManagerIds()
   * minus our id.
   */
  public Set getOtherNormalDistributionManagerIds();

  /**
   * Add a membership listener and return other DistribtionManagerIds
   * as an atomic operation
   */
  public Set addMembershipListenerAndGetDistributionManagerIds(MembershipListener l);

  /**
   * Add a membership listener for all members
   * and return other DistribtionManagerIds as an atomic operation
   * @since GemFire 5.7
   */
  public Set addAllMembershipListenerAndGetAllIds(MembershipListener l);

  /**
   * Returns the identity of this <code>DistributionManager</code>
   */
  public InternalDistributedMember getId();

  /**
   * Return true if no other distribution manager was in this group
   * when he joined.
   *
   * @since GemFire 4.0
   */
  public boolean isAdam();
  /**
   * Returns the identity of the oldest DM in this group.
   * 
   * Note that this method may return null (no valid elders exist).
   * 
   * @return the elder member, possibly null
   * @since GemFire 4.0
   */
  public InternalDistributedMember getElderId();
  /**
   * Return true if this is the oldest DM in this group.
   *
   * @since GemFire 5.0
   */
  public boolean isElder();
  
  /**
   * Return true if this DM is a loner that is not part of
   * a real distributed system.
   */
  public boolean isLoner();
  
  /**
   * Returns the elder state or null if this DM is not the elder.
   * <p>
   * If useTryLock is true, then it will attempt to get a try-lock and throw 
   * IllegalStateException if another thread already holds the try-lock.
   * @param force if true then this DM must become the elder.
   * @param useTryLock if true then a try-lock will be used
   * @throws IllegalStateException if elder try lock fails
   * @since GemFire 4.0
   */
  public ElderState getElderState(boolean force, boolean useTryLock);
  
  /**
   * Returns the id of the underlying distribution channel used for
   * communication.
   *
   * @since GemFire 3.0
   */
  public long getChannelId();

  /**
   * Adds a message to the outgoing queue.  Note that
   * <code>message</code> should not be modified after it has been
   * added to the queue.  After <code>message</code> is distributed,
   * it will be recycled.
   *
   * @return recipients who did not receive the message
   * @throws NotSerializableException
   *         If <code>message</code> cannot be serialized
   * @see #putOutgoing(DistributionMessage)
   */
  public Set putOutgoingUserData(DistributionMessage message) 
      throws NotSerializableException;

  /**
   * Sends a message, guaranteed to be serialized
   * 
   * @see #putOutgoingUserData(DistributionMessage)
   * @param msg
   * @return recipients who did not receive the message
   */
  public Set putOutgoing(DistributionMessage msg);
  
  /**
   * Returns the distributed system to which this distribution manager
   * is connected.
   */
  public InternalDistributedSystem getSystem();

  /**
   * Adds a <code>MembershipListener</code> to this distribution
   * manager.
   */
  public void addMembershipListener(MembershipListener l);

  /**
   * Removes a <code>MembershipListener</code> from this distribution
   * manager.
   *
   * @throws IllegalArgumentException
   *         <code>l</code> was not registered on this distribution
   *         manager
   */
  public void removeMembershipListener(MembershipListener l);

  /**
   * Removes a <code>MembershipListener</code> listening for all members
   * from this distribution manager.
   *
   * @throws IllegalArgumentException
   *         <code>l</code> was not registered on this distribution
   *         manager
   * @since GemFire 5.7
   */
  public void removeAllMembershipListener(MembershipListener l);

  public void addAdminConsole(InternalDistributedMember id);

  public DMStats getStats();

  /**
   * Used to get the DistributionConfig so that Connection can
   * figure out if it is configured for async comms.
   * @since GemFire 4.2.1
   */
  public DistributionConfig getConfig();
  
  /**
   * Makes note of a distribution manager that has shut down.  Invokes
   * the appropriate listeners.
   *
   * @param theId
   *        The id of the distribution manager starting up
   *
   * @see ShutdownMessage#process
   */
  public void handleManagerDeparture(InternalDistributedMember theId, 
      boolean crashed, String reason);

  /**
   * getThreadPool gets this distribution manager's message-processing thread pool */
  public ExecutorService getThreadPool();

  /**
   * Return the high-priority message-processing executor */
  public ExecutorService getHighPriorityThreadPool();
  
  /**
   * Return the waiting message-processing executor 
   */
  public ExecutorService getWaitingThreadPool();
  
  /**
   * Return the special waiting message-processing executor 
   */
  public ExecutorService getPrMetaDataCleanupThreadPool();

  /**
   * gets this distribution manager's message-processing executor
   * for ordered (i.e. serialized) message processing
   */
  // public Executor getSerialExecutor();

  public void close();

  /**
   * Returns the ordered list of current DistributionManagers in
   * oldest-to-youngest order.  Added for DLockGrantor
   */
  public List<InternalDistributedMember> getViewMembers();
  /**
   * Returns the oldest member in the given set of distribution managers.  The
   * current implementation may use n*n/2 comparisons, so use this judiciously
   * 
   * @return the oldest member of the given collection
   * @throws NoSuchElementException when none of the given members is actually
   * a member of the distributed system.
   */
  public DistributedMember getOldestMember(Collection members) throws NoSuchElementException;
/**
 * @return Set of Admin VM nodes
 */
  public Set getAdminMemberSet();
  
  /** Throws ShutdownException if closeInProgress returns true. */
  public void throwIfDistributionStopped();
  
  /** Returns count of members filling the specified role */
  public int getRoleCount(Role role);
  
  /** Returns true if at least one member is filling the specified role */
  public boolean isRolePresent(Role role);

  /** Returns a set of all roles currently in the distributed system. */
  public Set getAllRoles();
  
  /** Returns true if id is a current member of the distributed system */
  public boolean isCurrentMember(InternalDistributedMember id);
  
  /** Remove given member from list of members who are pending a
   * startup reply
   * @param m the member
   * @param departed true if we're removing them due to membership
   */
  public void removeUnfinishedStartup(InternalDistributedMember m,
      boolean departed);
  
  public void setUnfinishedStartups(Collection s);
  
  /**
   * Return the CancelCriterion for this DM.
   * 
   * @return CancelCriterion for this DM
   */
  public CancelCriterion getCancelCriterion();
  
  /**
   * Return the membership manager for this DM
   * @return the membership manager
   */
  public MembershipManager getMembershipManager();
  
  /**
   * Set the root cause for DM failure
   * @param t the underlying failure
   */
  public void setRootCause(Throwable t);
  
  /**
   * Return the underlying root cause for DM failure, possibly null
   * @return the underlying root cause
   */
  public Throwable getRootCause();
  
  /**
   * Return all members that are on the the this host 
   * @return set of {@link InternalDistributedMember} including this VM
   * @since GemFire 5.9
   */
  public Set <InternalDistributedMember> getMembersInThisZone();
  
  /**
   * Acquire a permit to request a GII from another member
   */
  public void acquireGIIPermitUninterruptibly();
  
  /**
   * Release a permit to request a GII from another member.
   */
  public void releaseGIIPermit();

  public int getDistributedSystemId();

  public boolean enforceUniqueZone();

  public Set<InternalDistributedMember> getMembersInSameZone(InternalDistributedMember acceptedMember);

  public boolean areInSameZone(InternalDistributedMember member1,
      InternalDistributedMember member2);
  
  /**
   * Returns true is the two members are on the same equivalent host machine
   * based on overlapping IP addresses collected for all NICs on each member's
   * machine.
   * 
   * @param member1 First member
   * @param member2 Second member
   */
  public boolean areOnEquivalentHost(InternalDistributedMember member1,
      InternalDistributedMember member2);

  public Set<InetAddress> getEquivalents(InetAddress in);

  public Set<DistributedMember> getGroupMembers(String group);

  /**
   * Adds the entry in hostedLocators for a member with one or more
   * hosted locators. The value is a collection of host[port] strings. If a 
   * bind-address was used for a locator then the form is bind-addr[port].
   * <p>
   * This currently only tracks stand-alone/dedicated locators, not embedded
   * locators.
   * @param isSharedConfigurationEnabled flag to determine if the locator has enabled shared configuration
   * 
   * @since GemFire 6.6.3
   */
  public void addHostedLocators(InternalDistributedMember member, Collection<String> locators, boolean isSharedConfigurationEnabled);
  
  
  /**
   * Gets the value in hostedLocators for a member with one or more
   * hosted locators. The value is a collection of host[port] strings. If a 
   * bind-address was used for a locator then the form is bind-addr[port].
   * <p>
   * This currently only tracks stand-alone/dedicated locators, not embedded
   * locators.
   * 
   * @since GemFire 6.6.3
   */
  public Collection<String> getHostedLocators(InternalDistributedMember member);
  
  /**
   * Gets the map of all members hosting locators. The key is the member, and
   * the value is a collection of host[port] strings. If a bind-address was 
   * used for a locator then the form is bind-addr[port].
   * <p>
   * This currently only tracks stand-alone/dedicated locators, not embedded
   * locators.
   * 
   * @since GemFire 6.6.3
   */
  public Map<InternalDistributedMember, Collection<String>> getAllHostedLocators();

  /**
   * Gets the map of all members hosting locators with shared configuration. The key is the member, and
   * the value is a collection of host[port] strings. If a bind-address was 
   * used for a locator then the form is bind-addr[port].
   * <p>
   * This currently only tracks stand-alone/dedicated locators, not embedded
   * locators.
   * 
   * @since GemFire 8.0
   */
  public Map<InternalDistributedMember, Collection<String>> getAllHostedLocatorsWithSharedConfiguration();
  
  /****
   * Determines if the distributed system has the shared configuration service enabled or not. 
   * 
   * @return true if the distributed system was started or had a locator with enable-cluster-configuration = true 
   */
  public boolean isSharedConfigurationServiceEnabledForDS();
    
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
}
