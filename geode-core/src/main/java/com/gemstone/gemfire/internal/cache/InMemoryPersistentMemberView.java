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
package com.gemstone.gemfire.internal.cache;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import java.util.Collections;
import com.gemstone.gemfire.internal.cache.persistence.DiskStoreID;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberPattern;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberState;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberView;

/**
 *
 */
public class InMemoryPersistentMemberView implements PersistentMemberView {
  private Map<PersistentMemberID, PersistentMemberState> members= new ConcurrentHashMap<PersistentMemberID, PersistentMemberState>();


  public PersistentMemberID generatePersistentID() {
    return null;
  }

  public PersistentMemberID getMyPersistentID() {
    return null;
  }

  public Set<PersistentMemberID> getOfflineMembers() {
    Set<PersistentMemberID> offlineMembers = new HashSet<PersistentMemberID>();
    for(Map.Entry<PersistentMemberID, PersistentMemberState> entry : members.entrySet()) {
      if(entry.getValue() == PersistentMemberState.OFFLINE) {
        offlineMembers.add(entry.getKey());
      }
    }
    return offlineMembers;
  }
  public Set<PersistentMemberID> getOfflineAndEqualMembers() {
    Set<PersistentMemberID> equalMembers = new HashSet<PersistentMemberID>();
    for(Map.Entry<PersistentMemberID, PersistentMemberState> entry : members.entrySet()) {
      if(entry.getValue() == PersistentMemberState.EQUAL) {
        equalMembers.add(entry.getKey());
      }
    }
    return equalMembers;
  }

  public Set<PersistentMemberID> getOnlineMembers() {
    Set<PersistentMemberID> onlineMembers = new HashSet<PersistentMemberID>();
    for(Map.Entry<PersistentMemberID, PersistentMemberState> entry : members.entrySet()) {
      if(entry.getValue() == PersistentMemberState.ONLINE) {
        onlineMembers.add(entry.getKey());
      }
    }
    return onlineMembers;
  }

  public void memberOffline(PersistentMemberID persistentID) {
    members.put(persistentID, PersistentMemberState.OFFLINE);
  }

  public void memberOfflineAndEqual(PersistentMemberID persistentID) {
    members.put(persistentID, PersistentMemberState.EQUAL);
  }

  public void memberOnline(PersistentMemberID persistentID) {
    members.put(persistentID, PersistentMemberState.ONLINE);
  }
  
  public void memberRemoved(PersistentMemberID persistentID) {
    members.remove(persistentID);
  }

  public void setInitialized() {
  }
  
  public PersistentMemberID getMyInitializingID() {
    return null;
  }

  public void setInitializing(PersistentMemberID newId) {
  }

  public void endDestroy(LocalRegion region) {
    //don't care
  }

  public void beginDestroy(LocalRegion region) {
    //don't care
  }
  public void beginDestroyDataStorage() {
    //don't care
  }
  
  public void finishPendingDestroy() {
  }

  public boolean wasAboutToDestroy() {
    return false;
  }
  public boolean wasAboutToDestroyDataStorage() {
    return false;
  }

  public DiskStoreID getDiskStoreID() {
    return null;
  }

  @Override
  public void memberRevoked(PersistentMemberPattern pattern) {
    //do nothing,  don't need to persist this information.
  }

  @Override
  public Set<PersistentMemberPattern> getRevokedMembers() {
    return Collections.emptySet();
  }
}
