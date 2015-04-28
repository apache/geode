/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * Holds the results of a persistent state query
 * @author dsmith
 *
 */
class PersistentStateQueryResults {
  public final Map<InternalDistributedMember, PersistentMemberState> stateOnPeers = new HashMap<InternalDistributedMember, PersistentMemberState>();
  public final Map<InternalDistributedMember, PersistentMemberID> initializingIds = new HashMap<InternalDistributedMember, PersistentMemberID>();
  public final Map<InternalDistributedMember, PersistentMemberID> persistentIds = new HashMap<InternalDistributedMember, PersistentMemberID>();
  public final Map<InternalDistributedMember, Set<PersistentMemberID>> onlineMemberMap = new HashMap<InternalDistributedMember, Set<PersistentMemberID>>();
  public final Map<InternalDistributedMember, DiskStoreID> diskStoreIds = new HashMap<InternalDistributedMember, DiskStoreID>();
  
  public synchronized void addResult(PersistentMemberState persistedStateOfPeer,
      InternalDistributedMember sender, PersistentMemberID myId,
      PersistentMemberID myInitializingId, DiskStoreID diskStoreID, HashSet<PersistentMemberID> onlineMembers) {
    stateOnPeers.put(sender, persistedStateOfPeer);
    if(myId != null) {
      persistentIds.put(sender, myId);
    }
    if(myInitializingId != null) {
      initializingIds.put(sender, myInitializingId);
    }
    if(diskStoreID != null) {
      diskStoreIds.put(sender, diskStoreID);
    }
    if(onlineMembers != null) {
      onlineMemberMap.put(sender, onlineMembers);
    }
  }

}
