/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.persistence;

import java.util.Set;

import com.gemstone.gemfire.GemFireException;

/**
 * Thrown when a paritioned region is configured for disk persistence,
 * and part of the data is stored on members that are known to be offline.
 * 
 * With a partitioned region, the keyspace is segmented into buckets which are
 * assigned to individual members. If all members that are storing data
 * for a particular bucket are offline, any attempt to access or update
 * data in that bucket will throw this exception.
 * 
 * If you see this exception, that means that you need to restart the
 * members that host the missing data.
 * 
 * If you receive this exception when attempting an operation that modifies
 * the region (such as a put), it is possible that the change was actually
 * persisted to disk before the member went offline.
 * 
 * @author dsmith
 * @since 6.5
 *
 */
public class PartitionOfflineException extends GemFireException {

  private static final long serialVersionUID = -6471045959318795870L;
  
  private Set<PersistentID> offlineMembers;

  public PartitionOfflineException(Set<PersistentID> offlineMembers) {
    super();
    this.offlineMembers = offlineMembers;
  }

  public PartitionOfflineException(Set<PersistentID> offlineMembers, String message) {
    super(message);
    this.offlineMembers = offlineMembers;
  }

  public PartitionOfflineException(Set<PersistentID> offlineMembers, String message, Throwable cause) {
    super(message, cause);
    this.offlineMembers = offlineMembers;
  }

  public PartitionOfflineException(Set<PersistentID> offlineMembers, Throwable cause) {
    super(cause);
    this.offlineMembers = offlineMembers;
  }

  /**
   * Retrieve the set of disk directories which are known to hold data for the missing 
   * buckets, but are not online.
   */
  public Set<PersistentID> getOfflineMembers() {
    return (Set<PersistentID>) offlineMembers;
  }

}
