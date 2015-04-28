/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * The status of a backup operation, returned by
 * {@link AdminDistributedSystem#backupAllMembers(java.io.File,java.io.File)}.
 * 
 * @author dsmith
 * @since 6.5 
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface BackupStatus {
  
  /**
   * Returns a map of disk stores that were successfully backed up.
   * The key is an online distributed member. The value is the set of disk 
   * stores on that distributed member. 
   */
  Map<DistributedMember, Set<PersistentID>> getBackedUpDiskStores();
  
  /**
   * Returns the set of disk stores that were known to be offline at the 
   * time of the backup. These members were not backed up. If this set
   * is not empty the backup may not contain a complete snapshot of 
   * any partitioned regions in the distributed system.
   */
  Set<PersistentID> getOfflineDiskStores();
}
