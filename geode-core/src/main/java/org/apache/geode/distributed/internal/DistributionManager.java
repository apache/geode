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

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.apache.geode.CancelCriterion;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.locks.ElderState;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.alerting.AlertingService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

/**
 * This interface defines the services provided by any class that is a distribution manager.
 */
public interface DistributionManager extends ReplySender {

  boolean shutdownInProgress();

  /**
   * Returns the current "cache time" in milliseconds since the epoch. The "cache time" takes into
   * account skew among the local clocks on the various machines involved in the cache.
   */
  long cacheTimeMillis();

  /**
   * Returns the id of this distribution manager.
   */
  InternalDistributedMember getDistributionManagerId();

  /**
   * Get a set of all other members (both admin ones and normal).
   *
   * @since GemFire 5.7
   */
  Set<InternalDistributedMember> getAllOtherMembers();

  /**
   * Returns the ID in the membership view that is equal to the argument. If the ID is not in the
   * view, the argument is returned.
   */
  InternalDistributedMember getCanonicalId(DistributedMember id);

  /**
   * removes members that have older versions from the given collection, typically a Set from a
   * distribution advisor
   *
   * @since GemFire 8.0
   */
  void retainMembersWithSameOrNewerVersion(Collection<InternalDistributedMember> members,
      Version version);

  /**
   * removes members that have the given version or later from the given collection, typically a Set
   * from a distribution advisor
   *
   * @since GemFire 8.0
   */
  void removeMembersWithSameOrNewerVersion(Collection<InternalDistributedMember> members,
      Version version);

  /**
   * Returns an unmodifiable set containing the identities of all of the known distribution
   * managers. As of 7.0 this includes locators since they have a cache.
   */
  Set<InternalDistributedMember> getDistributionManagerIds();

  /**
   * Returns an unmodifiable set containing the identities of all of the known "normal" distribution
   * managers. This does not include locators or admin members.
   */
  Set<InternalDistributedMember> getNormalDistributionManagerIds();

  /**
   * Returns an unmodifiable set containing the identities of all of the known distribution managers
   * including admin members.
   *
   * @since GemFire 5.7
   */
  Set<InternalDistributedMember> getDistributionManagerIdsIncludingAdmin();

  /**
   * Returns a private-memory list containing getDistributionManagerIds() minus our id.
   */
  Set<InternalDistributedMember> getOtherDistributionManagerIds();

  /**
   * Returns a private-memory list containing getNormalDistributionManagerIds() minus our id.
   */
  Set<InternalDistributedMember> getOtherNormalDistributionManagerIds();

  /**
   * Add a membership listener and return other DistributionManagerIds as an atomic operation
   */
  Set<InternalDistributedMember> addMembershipListenerAndGetDistributionManagerIds(
      MembershipListener l);

  /**
   * Add a membership listener for all members and return other DistribtionManagerIds as an atomic
   * operation
   *
   * @since GemFire 5.7
   */
  Set<InternalDistributedMember> addAllMembershipListenerAndGetAllIds(MembershipListener l);

  /**
   * Returns the identity of this <code>DistributionManager</code>
   */
  InternalDistributedMember getId();

  /**
   * Returns the identity of the oldest DM in this group.
   *
   * Note that this method may return null (no valid elders exist).
   *
   * @return the elder member, possibly null
   * @since GemFire 4.0
   */
  InternalDistributedMember getElderId();

  /**
   * Return true if this is the oldest DM in this group.
   *
   * @since GemFire 5.0
   */
  boolean isElder();

  /**
   * Return true if this DM is a loner that is not part of a real distributed system.
   */
  boolean isLoner();

  /**
   * Returns the elder state or null if this DM is not the elder.
   * <p>
   * If useTryLock is true, then it will attempt to get a try-lock and throw IllegalStateException
   * if another thread already holds the try-lock.
   *
   * @param force if true then this DM must become the elder.
   * @throws IllegalStateException if elder try lock fails
   * @since GemFire 4.0
   */
  ElderState getElderState(boolean force);

  /**
   * Returns the membership port of the underlying distribution manager used for communication.
   *
   * @since GemFire 3.0
   */
  long getMembershipPort();

  /**
   * Sends a message
   *
   * @return recipients who did not receive the message
   */
  Set<InternalDistributedMember> putOutgoing(DistributionMessage msg);

  /**
   * Returns the distributed system to which this distribution manager is connected.
   */
  InternalDistributedSystem getSystem();

  /**
   * Adds a <code>MembershipListener</code> to this distribution manager.
   */
  void addMembershipListener(MembershipListener l);

  /**
   * Removes a <code>MembershipListener</code> from this distribution manager.
   *
   * @throws IllegalArgumentException <code>l</code> was not registered on this distribution manager
   */
  void removeMembershipListener(MembershipListener l);

  Collection<MembershipListener> getMembershipListeners();

  /**
   * Removes a <code>MembershipListener</code> listening for all members from this distribution
   * manager.
   *
   * @throws IllegalArgumentException <code>l</code> was not registered on this distribution manager
   * @since GemFire 5.7
   */
  void removeAllMembershipListener(MembershipListener l);

  /**
   * Makes note of a new administration console (admin-only member).
   *
   * @deprecated admin members are deprecated
   */
  void addAdminConsole(InternalDistributedMember id);

  DMStats getStats();

  /**
   * Used to get the DistributionConfig so that Connection can figure out if it is configured for
   * async comms.
   *
   * @since GemFire 4.2.1
   */
  DistributionConfig getConfig();

  /**
   * Makes note of a distribution manager that has shut down. Invokes the appropriate listeners.
   *
   * @param theId The id of the distribution manager starting up
   *
   * @see ShutdownMessage#process
   */
  void handleManagerDeparture(InternalDistributedMember theId, boolean crashed, String reason);

  /**
   * getThreadPool gets this distribution manager's message-processing thread pool
   */
  ExecutorService getThreadPool();

  /**
   * Return the high-priority message-processing executor
   */
  ExecutorService getHighPriorityThreadPool();

  /**
   * Return the waiting message-processing executor
   */
  ExecutorService getWaitingThreadPool();

  /**
   * Return the special waiting message-processing executor
   */
  ExecutorService getPrMetaDataCleanupThreadPool();

  /**
   * Return the executor used for function processing
   */
  Executor getFunctionExecutor();

  void close();

  /**
   * Returns the ordered list of current DistributionManagers in oldest-to-youngest order. Added for
   * DLockGrantor
   */
  List<InternalDistributedMember> getViewMembers();

  /**
   * @return Set of Admin VM nodes
   */
  Set<InternalDistributedMember> getAdminMemberSet();

  /** Throws ShutdownException if closeInProgress returns true. */
  void throwIfDistributionStopped();

  /** Returns count of members filling the specified role */
  int getRoleCount(Role role);

  /** Returns true if at least one member is filling the specified role */
  boolean isRolePresent(Role role);

  /** Returns a set of all roles currently in the distributed system. */
  Set getAllRoles();

  /**
   * Returns true if id is a current member of the distributed system
   *
   */
  boolean isCurrentMember(DistributedMember id);

  /**
   * Remove given member from list of members who are pending a startup reply
   *
   * @param m the member
   * @param departed true if we're removing them due to membership
   */
  void removeUnfinishedStartup(InternalDistributedMember m, boolean departed);

  void setUnfinishedStartups(Collection<InternalDistributedMember> s);

  /**
   * Return the CancelCriterion for this DM.
   *
   * @return CancelCriterion for this DM
   */
  CancelCriterion getCancelCriterion();

  /**
   * Return the membership manager for this DM
   *
   * @return the membership manager
   */
  MembershipManager getMembershipManager();

  /**
   * Set the root cause for DM failure
   *
   * @param t the underlying failure
   */
  void setRootCause(Throwable t);

  /**
   * Return the underlying root cause for DM failure, possibly null
   *
   * @return the underlying root cause
   */
  Throwable getRootCause();

  /**
   * Return all members that are on the the this host
   *
   * @return set of {@link InternalDistributedMember} including this VM
   * @since GemFire 5.9
   */
  Set<InternalDistributedMember> getMembersInThisZone();

  /**
   * Acquire a permit to request a GII from another member
   */
  void acquireGIIPermitUninterruptibly();

  /**
   * Release a permit to request a GII from another member.
   */
  void releaseGIIPermit();

  int getDistributedSystemId();

  boolean enforceUniqueZone();

  Set<InternalDistributedMember> getMembersInSameZone(InternalDistributedMember acceptedMember);

  boolean areInSameZone(InternalDistributedMember member1, InternalDistributedMember member2);

  /**
   * Returns true is the two members are on the same equivalent host machine based on overlapping IP
   * addresses collected for all NICs on each member's machine.
   *
   * @param member1 First member
   * @param member2 Second member
   */
  boolean areOnEquivalentHost(InternalDistributedMember member1, InternalDistributedMember member2);

  Set<InetAddress> getEquivalents(InetAddress in);

  Set<DistributedMember> getGroupMembers(String group);

  /**
   * Adds the entry in hostedLocators for a member with one or more hosted locators. The value is a
   * collection of host[port] strings. If a bind-address was used for a locator then the form is
   * bind-addr[port].
   * <p>
   * This currently only tracks stand-alone/dedicated locators, not embedded locators.
   *
   * @param isSharedConfigurationEnabled flag to determine if the locator has enabled shared
   *        configuration
   *
   * @since GemFire 6.6.3
   */
  void addHostedLocators(InternalDistributedMember member, Collection<String> locators,
      boolean isSharedConfigurationEnabled);


  /**
   * Gets the value in hostedLocators for a member with one or more hosted locators. The value is a
   * collection of host[port] strings. If a bind-address was used for a locator then the form is
   * bind-addr[port].
   * <p>
   * This currently only tracks stand-alone/dedicated locators, not embedded locators.
   *
   * @since GemFire 6.6.3
   */
  Collection<String> getHostedLocators(InternalDistributedMember member);

  /**
   * Gets the map of all members hosting locators. The key is the member, and the value is a
   * collection of host[port] strings. If a bind-address was used for a locator then the form is
   * bind-addr[port].
   *
   *
   * <p>
   * This currently only tracks stand-alone/dedicated locators, not embedded locators.
   *
   * @since GemFire 6.6.3
   */
  Map<InternalDistributedMember, Collection<String>> getAllHostedLocators();

  /**
   * Gets the map of all members hosting locators with shared configuration. The key is the member,
   * and the value is a collection of host[port] strings. If a bind-address was used for a locator
   * then the form is bind-addr[port].
   * <p>
   * This currently only tracks stand-alone/dedicated locators, not embedded locators.
   *
   * @since GemFire 8.0
   */
  Map<InternalDistributedMember, Collection<String>> getAllHostedLocatorsWithSharedConfiguration();

  /**
   * Forces use of UDP for communications in the current thread. UDP is connectionless, so no tcp/ip
   * connections will be created or used for messaging until this setting is released with
   * releaseUDPMessagingForCurrentThread.
   */
  void forceUDPMessagingForCurrentThread();

  /**
   * Releases use of UDP for all communications in the current thread, as established by
   * forceUDPMessagingForCurrentThread.
   */
  void releaseUDPMessagingForCurrentThread();

  /**
   * returns the type of node
   *
   * @see ClusterDistributionManager#NORMAL_DM_TYPE
   * @see ClusterDistributionManager#LONER_DM_TYPE
   * @see ClusterDistributionManager#LOCATOR_DM_TYPE
   * @see ClusterDistributionManager#ADMIN_ONLY_DM_TYPE
   */
  int getDMType();

  /**
   * The returned cache will be null if the cache does not yet exist. Note that the returned cache
   * may be one that is already closed. Callers of GemFireCacheImpl.getInstance() should try to use
   * this method.
   */
  InternalCache getCache();

  /**
   * Returns an existing non-closed cache associated with this DM. Callers of
   * CacheFactory.getAnyInstance(), CacheFactory.getInstance(DistributedSystem) or
   * GemFireCacheImpl.getExisting() should try to use this method.
   *
   * @throws CacheClosedException if a cache has not yet been associated with this DM or it has been
   *         {@link Cache#isClosed closed}.
   */
  InternalCache getExistingCache();

  void setCache(InternalCache instance);

  HealthMonitor getHealthMonitor(InternalDistributedMember owner);

  void removeHealthMonitor(InternalDistributedMember owner, int theId);

  void createHealthMonitor(InternalDistributedMember owner, GemFireHealthConfig cfg);

  boolean exceptionInThreads();

  void clearExceptionInThreads();

  /**
   * returns the ID of a member having the given name, or null if no such member exists
   */
  DistributedMember getMemberWithName(String name);

  /** returns the Threads Monitoring instance */
  public ThreadsMonitoring getThreadMonitoring();

  /**
   * Returns the {@link AlertingService}.
   */
  AlertingService getAlertingService();
}
