/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;

import java.util.Map;

import com.gemstone.gemfire.cache.persistence.PersistentID;

/**
 * Composite data type used to distribute the status of disk backup
 * operations.
 * 
 * @author rishim
 * @since 7.0
 */
public class DiskBackupStatus {

  /**
   * Map of DistributedMember, Set<PersistentID>
   */
  private Map<String, String[]> backedUpDiskStores;
  
  /**
   * List of offline disk stores
   */
  private String[] offlineDiskStores;

  /**
   * Returns a map of member names/IDs and the {@link PersistentID} of the disk
   * stores that were backed up.
   */
  public Map<String, String[]> getBackedUpDiskStores() {
    return backedUpDiskStores;
  }

  /**
   * Returns a list of directories for the disk stores that were off-line at
   * the time the backup occurred.
   */
  public String[] getOfflineDiskStores() {
    return offlineDiskStores;
  }

  /**
   * Sets the map of member names/IDs and the {@link PersistentID} of the disk
   * stores that were backed up.
   */
  public void setBackedUpDiskStores(Map<String, String[]> backedUpDiskStores) {
    this.backedUpDiskStores = backedUpDiskStores;
  }

  /**
   * Sets the list of directories for the disk stores that were off-line at the
   * time the backup occurred.
   */
  public void setOfflineDiskStores(String[] offlineDiskStores) {
    this.offlineDiskStores = offlineDiskStores;
  }
}
