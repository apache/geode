/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
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
}
