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
package com.gemstone.gemfire.internal.cache.persistence;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import com.gemstone.gemfire.internal.cache.LocalRegion;

/**
 * Holds a view of the persistent members for a region. This view may
 * be backed by disk, or it may be only in memory on non-persistent
 * replicas.
 * 
 * @author dsmith
 * 
 */
public interface PersistentMemberView {

  /**
   * @return the set of online members.
   */
  public abstract Set<PersistentMemberID> getOnlineMembers();

  /**
   * @return the set of offline members
   */
  public abstract Set<PersistentMemberID> getOfflineMembers();
  
  /**
   * @return the set of offline and equal members
   */
  public abstract Set<PersistentMemberID> getOfflineAndEqualMembers();

  /**
   * Retrieve the persisted ID of the current member.
   */
  public abstract PersistentMemberID getMyPersistentID();
  
  /**
   * Indicate that this member is now online, and initialize it with the list of
   * offline and online members.
   */
  void setInitialized();

  /**
   * Indicate that a member is offline. This is only called after this member is
   * online.
   */
  public abstract void memberOffline(PersistentMemberID persistentID);
  
  /**
   * Indicate that a member is offline, and has the same data on disk as the
   * current member. This is only called after this member is online.
   */
  public abstract void memberOfflineAndEqual(PersistentMemberID persistentID);

  /**
   * Indicate that a new member is online. This is only called after this member
   * is online.
   */
  public abstract void memberOnline(PersistentMemberID persistentID);

  /**
   * Indicate that the member no longer persists the region and can be removed.
   * This is only called after this member is online.
   * 
   * @param persistentID
   */
  public void memberRemoved(PersistentMemberID persistentID);
  
  public abstract PersistentMemberID generatePersistentID();

  public abstract void setInitializing(PersistentMemberID newId);
  
  /**
   * Indicate that we are about to destroy this region.
   * This method will be called before distribution so
   * that if we crash during distribution, we can recover
   * and complete the destroy.
   * It will remove all data from disk for this region
   * and remove the membership view. It must keep the
   * PersistentID.
   */
  public abstract void beginDestroy(LocalRegion region);
  /**
   * Indicate that we are about to partially destroy this region.
   * It is used to stop hosting a bucket and become a proxy bucket.
   * This method will be called before distribution so
   * that if we crash during distribution, we can recover
   * and complete the partial destroy.
   * It will remove all data from disk for this region.
   * It must keep the PersistentID
   * and the membership view.
   */
  public abstract void beginDestroyDataStorage();
  
  /**
   * This method is called to finish either
   * setAboutToDestroy or setAboutToPartialDestroy.

   * It can be called either during a normal region destroy
   * or during a partial region destroy.
   * AFTER distribution has completed or during initialization
   * IF the region was previously crashed while
   * it was "about to destroy." This method
   * is expected to purge any data on disk, as
   * well as any membership view information and persistent
   * ID for this region.

   * 
   * This method should NOT prevent this 
   * disk region from being initialized later.
   */
  public abstract void endDestroy(LocalRegion region);

  public abstract PersistentMemberID getMyInitializingID();
  
  public abstract boolean wasAboutToDestroy();
  public abstract boolean wasAboutToDestroyDataStorage();

  /**
   * This method is used to complete a destroy that
   * was pending when a member crashed. This method
   * applies the destroy operation, but it leaves the DiskRegion
   * in a usable (empty) state.
   */
  public abstract void finishPendingDestroy();

  public abstract DiskStoreID getDiskStoreID();

  public abstract void memberRevoked(PersistentMemberPattern pattern);

  public abstract Set<PersistentMemberPattern> getRevokedMembers();
}
