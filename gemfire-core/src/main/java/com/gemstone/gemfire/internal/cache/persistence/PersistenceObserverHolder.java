/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

import com.gemstone.gemfire.cache.Region;

/**
 * Used for test hooks to during the
 * persistence process.
 * @author dsmith
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
