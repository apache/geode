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
package org.apache.geode.internal.cache;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.internal.cache.persistence.PersistentMemberState;
import org.apache.geode.internal.cache.persistence.PersistentMemberView;

public class InMemoryPersistentMemberView implements PersistentMemberView {
  private final Map<PersistentMemberID, PersistentMemberState> members =
      new ConcurrentHashMap<PersistentMemberID, PersistentMemberState>();


  @Override
  public PersistentMemberID generatePersistentID() {
    return null;
  }

  @Override
  public PersistentMemberID getMyPersistentID() {
    return null;
  }

  @Override
  public Set<PersistentMemberID> getOfflineMembers() {
    Set<PersistentMemberID> offlineMembers = new HashSet<PersistentMemberID>();
    for (Map.Entry<PersistentMemberID, PersistentMemberState> entry : members.entrySet()) {
      if (entry.getValue() == PersistentMemberState.OFFLINE) {
        offlineMembers.add(entry.getKey());
      }
    }
    return offlineMembers;
  }

  @Override
  public Set<PersistentMemberID> getOfflineAndEqualMembers() {
    Set<PersistentMemberID> equalMembers = new HashSet<PersistentMemberID>();
    for (Map.Entry<PersistentMemberID, PersistentMemberState> entry : members.entrySet()) {
      if (entry.getValue() == PersistentMemberState.EQUAL) {
        equalMembers.add(entry.getKey());
      }
    }
    return equalMembers;
  }

  @Override
  public Set<PersistentMemberID> getOnlineMembers() {
    Set<PersistentMemberID> onlineMembers = new HashSet<PersistentMemberID>();
    for (Map.Entry<PersistentMemberID, PersistentMemberState> entry : members.entrySet()) {
      if (entry.getValue() == PersistentMemberState.ONLINE) {
        onlineMembers.add(entry.getKey());
      }
    }
    return onlineMembers;
  }

  @Override
  public void memberOffline(PersistentMemberID persistentID) {
    members.put(persistentID, PersistentMemberState.OFFLINE);
  }

  @Override
  public void memberOfflineAndEqual(PersistentMemberID persistentID) {
    members.put(persistentID, PersistentMemberState.EQUAL);
  }

  @Override
  public void memberOnline(PersistentMemberID persistentID) {
    members.put(persistentID, PersistentMemberState.ONLINE);
  }

  @Override
  public void memberRemoved(PersistentMemberID persistentID) {
    members.remove(persistentID);
  }

  @Override
  public void setInitialized() {}

  @Override
  public PersistentMemberID getMyInitializingID() {
    return null;
  }

  @Override
  public void setInitializing(PersistentMemberID newId) {}

  @Override
  public void endDestroy(LocalRegion region) {
    // don't care
  }

  @Override
  public void beginDestroy(LocalRegion region) {
    // don't care
  }

  @Override
  public void beginDestroyDataStorage() {
    // don't care
  }

  @Override
  public void finishPendingDestroy() {}

  @Override
  public boolean wasAboutToDestroy() {
    return false;
  }

  @Override
  public boolean wasAboutToDestroyDataStorage() {
    return false;
  }

  @Override
  public DiskStoreID getDiskStoreID() {
    return null;
  }

  @Override
  public void memberRevoked(PersistentMemberPattern pattern) {
    // do nothing, don't need to persist this information.
  }

  @Override
  public Set<PersistentMemberPattern> getRevokedMembers() {
    return Collections.emptySet();
  }
}
