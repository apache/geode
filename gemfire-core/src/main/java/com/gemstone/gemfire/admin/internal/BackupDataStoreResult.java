package com.gemstone.gemfire.admin.internal;

import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedMember;

public class BackupDataStoreResult {
  
  private Map<DistributedMember, Set<PersistentID>> existingDataStores;

  private Map<DistributedMember, Set<PersistentID>> successfulMembers;

  public BackupDataStoreResult(
      Map<DistributedMember, Set<PersistentID>> existingDataStores,
      Map<DistributedMember, Set<PersistentID>> successfulMembers) {
    this.existingDataStores = existingDataStores;
    this.successfulMembers = successfulMembers;
  }

  public Map<DistributedMember, Set<PersistentID>> getExistingDataStores() {
    return this.existingDataStores;
  }

  public Map<DistributedMember, Set<PersistentID>> getSuccessfulMembers() {
    return this.successfulMembers;
  }
  
  public String toString() {
    return new StringBuilder()
      .append(getClass().getSimpleName())
      .append("[")
      .append("existingDataStores=")
      .append(this.existingDataStores)
      .append("; successfulMembers=")
      .append(this.successfulMembers)
      .append("]")
      .toString();
  }
}
