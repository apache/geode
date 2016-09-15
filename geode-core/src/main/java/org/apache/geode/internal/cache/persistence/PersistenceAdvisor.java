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
package org.apache.geode.internal.cache.persistence;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.persistence.ConflictingPersistentDataException;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;

/**
 *
 */
public interface PersistenceAdvisor {
  
  /**This should be called before performing a profile exchange for the region,
   * but after the persistent data has been read.
   */
  public void initialize();
  
  /**
   * Try to acquire the distributed lock which members must grab for
   * in the case of a tie. Whoever gets the lock initializes first.
   */
  boolean acquireTieLock();


  /**
   * Determine the state of this member on it's peers, along with
   * the PersistentMemberID of those peers.
   * @return a map from the peers persistentId to the state of this
   * member according to that peer.
   */
  PersistentStateQueryResults getMyStateOnMembers(
      Set<InternalDistributedMember> members) throws ReplyException, InterruptedException;

  /**
   * Retrieve the state of a particular member from storage.
   */
  PersistentMemberState getPersistedStateOfMember(PersistentMemberID id);

  /**
   * Retrieves the stored view of the current members.
   */
  PersistentMembershipView getMembershipView();

  /**
   * Returns the member id of this member only if this member is online.
   */
  PersistentMemberID getPersistentIDIfOnline();

  /**
   * @return the persistent member id of this member even if it isn't online yet
   */
  PersistentMemberID getPersistentID();

  /**
   * Add a listener for changes to the persistent state of peers 
   */
  void addListener(PersistentStateListener listener);

  /**
   * Remove a listener for changes to the persistent state of peers 
   */
  void removeListener(PersistentStateListener listener);

  /**
   * Get the set of peers that are online 
   */
  HashSet<PersistentMemberID> getPersistedOnlineOrEqualMembers();
  
  
  /**Update the membership on this member to reflect changes
   * that have happened since the member was last online. This method
   * should be called before the member is online.
   * @param replicate the replicate to initialize from.
   */
  public void updateMembershipView(InternalDistributedMember replicate, boolean targetReinitializing);
  
  /**
   * Indicate that the current member is online.
   * @param didGII - indicates that the member did a GII (for updating stats)
   * @param atomicCreation - indicates that we are coming online as part of
   * an atomic bucket creation.
   * 
   */
  public void setOnline(boolean didGII, boolean atomicCreation,
      PersistentMemberID newId);
  
  /**
   * Indicate that the current member has started the initialization process.
   * This creates a new persistent ID for this member and notifies other members
   * about it.
   */
  public void setInitializing(PersistentMemberID newId);

  /**
   * Called when a peer is about to come online.
   */
  void prepareNewMember(InternalDistributedMember sender, PersistentMemberID oldId, PersistentMemberID newId);
  
  /**
   * Called when a peer has destroyed the region. This is usually
   * handled by the destroy region code, but if a member crashes
   * during a destroy, it may trigger this removal during recovery.
   */
  void removeMember(PersistentMemberID id);
  
  /**
   * Called when a peer returns that it has closed the cache or region when
   * a region operation was in flight.
   */
  public void markMemberOffline(InternalDistributedMember member, PersistentMemberID id);

  /**
   * If this member was initializing when it crashed or is currently in the
   * process of becoming online, this return the new ID of the member
   */
  PersistentMemberID getInitializingID();
  
  public void close();

  public Set<PersistentMemberID> getPersistedMembers();
  
  /**
   * Check to see if the other members know about the current member
   * .
   * @param replicates
   * @throws ConflictingPersistentDataException if the other members were not part of
   * the same distributed system as the persistent data on in this VM.
   * @return true if we detected that we actually have the same data
   * on disk as another member.
   */
  public boolean checkMyStateOnMembers(Set<InternalDistributedMember> replicates) throws ReplyException, InterruptedException, ConflictingPersistentDataException;

  public void releaseTieLock();
  
  /**
   * Returns the member id of the member who has the latest
   * copy of the persistent region. This may be the local member ID
   * if this member has the latest known copy.
   * 
   * This method will block until the latest member is online.
   * @param recoverFromDisk 
   * @throws ConflictingPersistentDataException if there are active members
   * which are not based on the state that is persisted in this member.
   */
  public CacheDistributionAdvisor.InitialImageAdvice getInitialImageAdvice(
      CacheDistributionAdvisor.InitialImageAdvice previousAdvice, boolean recoverFromDisk);
  
  /**
   * Returns true if this member used to host data.
   */
  public boolean wasHosting();
  
  /* 
   * Persist members to be offline and equal
   * @param Map<InternalDistributedMember, PersistentMemberID>
   *   map of current online members
   */
  public void persistMembersOfflineAndEqual(Map<InternalDistributedMember, PersistentMemberID> map);
  
  /**
   * Generate a new persistent id for this region.
   */
  public PersistentMemberID generatePersistentID();

  public DiskStoreID getDiskStoreID();

  public boolean isOnline();
}
