/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.admin.BackupStatus;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * Holds the result of a backup operation.
 * 
 * @author dsmith
 *
 */
public class BackupStatusImpl implements BackupStatus, Serializable {
  private static final long serialVersionUID = 3704162840296921840L;
  
  private Map<DistributedMember, Set<PersistentID>> backedUpDiskStores;
  private Set<PersistentID> offlineDiskStores;
  
  public BackupStatusImpl(
      Map<DistributedMember, Set<PersistentID>> backedUpDiskStores,
      Set<PersistentID> offlineDiskStores) {
    super();
    this.backedUpDiskStores = backedUpDiskStores;
    this.offlineDiskStores = offlineDiskStores;
  }

  public Map<DistributedMember, Set<PersistentID>> getBackedUpDiskStores() {
    return backedUpDiskStores;
  }

  public Set<PersistentID> getOfflineDiskStores() {
    return offlineDiskStores;
  }

  @Override
  public String toString() {
    return "BackupStatus[backedUpDiskStores=" + backedUpDiskStores + ", offlineDiskStores=" + offlineDiskStores + "]"; 
  }
  
  

}
