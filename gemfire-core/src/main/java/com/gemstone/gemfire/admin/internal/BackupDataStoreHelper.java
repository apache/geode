package com.gemstone.gemfire.admin.internal;

import java.io.File;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.internal.Assert;

public class BackupDataStoreHelper {

  private static String LOCK_SERVICE_NAME = BackupDataStoreHelper.class.getSimpleName();

  private static String LOCK_NAME = LOCK_SERVICE_NAME + "_token";
  
  private static Object LOCK_SYNC = new Object();

  @SuppressWarnings("rawtypes")
  public static BackupDataStoreResult backupAllMembers(
      DM dm, Set recipients, File targetDir, File baselineDir) {
    FlushToDiskRequest.send(dm, recipients);

    boolean abort= true;
    Map<DistributedMember, Set<PersistentID>> successfulMembers;
    Map<DistributedMember, Set<PersistentID>> existingDataStores;
    try {
      existingDataStores = PrepareBackupRequest.send(dm, recipients);
      abort = false;
    } finally {
      successfulMembers = FinishBackupRequest.send(dm, recipients, targetDir, baselineDir, abort);
    }
    return new BackupDataStoreResult(existingDataStores, successfulMembers);
  }
  
  private static DistributedLockService getLockService(DM dm) {
    DistributedLockService dls = DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME);
    if (dls == null) {
      synchronized (LOCK_SYNC) {
        dls = DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME);
        if (dls == null) {
          // Create the DistributedLockService
          dls = DistributedLockService.create(LOCK_SERVICE_NAME, dm.getSystem());
        }
      }
    }
    Assert.assertTrue(dls != null);
    return dls;
  }
  
  public static boolean obtainLock(DM dm) {
    return getLockService(dm).lock(LOCK_NAME, 0, -1);
  }
  
  public static void releaseLock(DM dm) {
    getLockService(dm).unlock(LOCK_NAME);
  }
}
