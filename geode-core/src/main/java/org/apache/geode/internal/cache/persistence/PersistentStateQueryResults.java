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
package org.apache.geode.internal.cache.persistence;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * Holds the results of a persistent state query
 *
 */
class PersistentStateQueryResults {
  private final Map<InternalDistributedMember, PersistentMemberState> stateOnPeers =
      new HashMap<InternalDistributedMember, PersistentMemberState>();
  private final Map<InternalDistributedMember, PersistentMemberID> initializingIds =
      new HashMap<InternalDistributedMember, PersistentMemberID>();
  private final Map<InternalDistributedMember, PersistentMemberID> persistentIds =
      new HashMap<InternalDistributedMember, PersistentMemberID>();
  private final Map<InternalDistributedMember, Set<PersistentMemberID>> onlineMemberMap =
      new HashMap<InternalDistributedMember, Set<PersistentMemberID>>();
  private final Map<InternalDistributedMember, DiskStoreID> diskStoreIds =
      new HashMap<InternalDistributedMember, DiskStoreID>();

  public synchronized void addResult(PersistentMemberState persistedStateOfPeer,
      InternalDistributedMember sender, PersistentMemberID myId,
      PersistentMemberID myInitializingId, DiskStoreID diskStoreID,
      HashSet<PersistentMemberID> onlineMembers) {
    stateOnPeers.put(sender, persistedStateOfPeer);
    if (myId != null) {
      persistentIds.put(sender, myId);
    }
    if (myInitializingId != null) {
      initializingIds.put(sender, myInitializingId);
    }
    if (diskStoreID != null) {
      diskStoreIds.put(sender, diskStoreID);
    }
    if (onlineMembers != null) {
      onlineMemberMap.put(sender, onlineMembers);
    }
  }

  Map<InternalDistributedMember, PersistentMemberState> getStateOnPeers() {
    return stateOnPeers;
  }

  Map<InternalDistributedMember, PersistentMemberID> getInitializingIds() {
    return initializingIds;
  }

  Map<InternalDistributedMember, PersistentMemberID> getPersistentIds() {
    return persistentIds;
  }

  Map<InternalDistributedMember, Set<PersistentMemberID>> getOnlineMemberMap() {
    return onlineMemberMap;
  }

  Map<InternalDistributedMember, DiskStoreID> getDiskStoreIds() {
    return diskStoreIds;
  }
}
