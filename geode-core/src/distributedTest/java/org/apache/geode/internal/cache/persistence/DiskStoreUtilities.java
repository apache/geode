package org.apache.geode.internal.cache.persistence;

import java.util.Set;
import java.util.UUID;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.internal.admin.remote.MissingPersistentIDsRequest;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.beans.DistributedSystemBridge;

public class DiskStoreUtilities {

  public static Set<PersistentID> listMissingDiskStores(InternalCache cache) {
    return MissingPersistentIDsRequest.send(cache.getDistributionManager());
  }

  public static boolean revokeMissingDiskStore(InternalCache cache, UUID diskStoreId) {
    return DistributedSystemBridge.revokeMissingDiskStore(cache.getDistributionManager(),
        diskStoreId);
  }

  private DiskStoreUtilities() {
    // static utility method class
  }
}
