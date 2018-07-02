package org.apache.geode.internal.cache.backup;

import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.Assert;

public class BackupLockService {

  public static final String LOCK_SERVICE_NAME = BackupLockService.class.getSimpleName();

  private static final String LOCK_NAME = LOCK_SERVICE_NAME + "_token";
  private static final Object LOCK_SYNC = new Object();

  BackupLockService() {
    // nothing
  }

  boolean obtainLock(DistributionManager dm) {
    return getLockService(dm).lock(LOCK_NAME, 0, -1);
  }

  void releaseLock(DistributionManager dm) {
    getLockService(dm).unlock(LOCK_NAME);
  }

  private DistributedLockService getLockService(DistributionManager dm) {
    DistributedLockService dls = DistributedLockService.getServiceNamed(
        LOCK_SERVICE_NAME);
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
}
