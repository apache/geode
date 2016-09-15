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

import org.apache.geode.cache.Region;

/**
 * Used for test hooks to during the
 * persistence process.
 *
 */

public class PersistenceObserverHolder {
  private static PersistenceObserver INSTANCE = new PersistenceObserverAdapter();
  
  public static void setInstance(PersistenceObserver instance) {
    if(instance == null) {
      INSTANCE = new PersistenceObserverAdapter(); 
    }
    INSTANCE = instance;
  }
  
  public static PersistenceObserver getInstance() {
    return INSTANCE;  
  }
  
  private PersistenceObserverHolder() {
    
  }
  
  public static interface PersistenceObserver {
    /**Fired just before we persist that a member is offline. Returning false
     * indicates that we should not persist the change.
     */
    public boolean memberOffline(String regionName, PersistentMemberID persistentID);
    
    /**Fired after we persist that a member is offline.
     */
    public void afterPersistedOffline(String fullPath, PersistentMemberID persistentID);
    
    /**Fired just before we persist that a member is online. Returning false
     * indicates that we should not persist the change.
     */
    public boolean memberOnline(String regionName, PersistentMemberID persistentID);
    
    /**Fired after we persist that a member is online.
     */
    public void afterPersistedOnline(String fullPath, PersistentMemberID persistentID);
    
    /**Fired just before we persist that a member no longer hosts a region. Returning false
     * indicates that we should not persist the change.
     */
    public boolean memberRemoved(String regionName, PersistentMemberID persistentID);
    
    /**Fired after we persist that a member no longer hosts the region.
     */
    public void afterRemovePersisted(String fullPath, PersistentMemberID persistentID);

  }
  
  public static class PersistenceObserverAdapter implements PersistenceObserver {

    public boolean memberOffline(String region, PersistentMemberID persistentID) {
      return true;
    }

    public boolean memberOnline(String region, PersistentMemberID persistentID) {
      return true;
    }

    public boolean memberRemoved(String region, PersistentMemberID persistentID) {
      return true;
    }

    public void afterPersistedOffline(String fullPath,
        PersistentMemberID persistentID) {
    }

    public void afterPersistedOnline(String fullPath,
        PersistentMemberID persistentID) {
    }

    public void afterRemovePersisted(String fullPath,
        PersistentMemberID persistentID) {
    }
  }
}
