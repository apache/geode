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
package org.apache.geode.management.internal.beans;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.backup.AbortBackupRequest;
import org.apache.geode.internal.cache.backup.BackupDataStoreHelper;
import org.apache.geode.internal.cache.backup.BackupService;
import org.apache.geode.internal.cache.backup.FinishBackupRequest;
import org.apache.geode.internal.cache.backup.PrepareBackupRequest;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class DistributedSystemBridgeJUnitTest {

  private GemFireCacheImpl cache;
  private BackupService backupService;

  @Before
  public void createCache() throws IOException {
    cache = Fakes.cache();
    PersistentMemberManager memberManager = mock(PersistentMemberManager.class);
    backupService = mock(BackupService.class);
    when(cache.getBackupService()).thenReturn(backupService);
    when(cache.getPersistentMemberManager()).thenReturn(memberManager);
    when(cache.getBackupService()).thenReturn(backupService);

    DLockService dlock = mock(DLockService.class);
    when(dlock.lock(any(), anyLong(), anyLong())).thenReturn(true);

    DLockService.addLockServiceForTests(BackupDataStoreHelper.LOCK_SERVICE_NAME, dlock);
  }

  @After
  public void clearCache() {
    DLockService.removeLockServiceForTests(BackupDataStoreHelper.LOCK_SERVICE_NAME);
  }

  @Test
  public void testSuccessfulBackup() throws Exception {
    DistributionManager dm = cache.getDistributionManager();

    DistributedSystemBridge bridge = new DistributedSystemBridge(null, cache);
    bridge.backupAllMembers("/tmp", null);

    InOrder inOrder = inOrder(dm, backupService);
    inOrder.verify(dm).putOutgoing(isA(PrepareBackupRequest.class));
    inOrder.verify(backupService).prepareBackup(any(), any());
    inOrder.verify(dm).putOutgoing(isA(FinishBackupRequest.class));
    inOrder.verify(backupService).doBackup();
  }

  @Test
  public void testPrepareErrorAbortsBackup() throws Exception {
    DistributionManager dm = cache.getDistributionManager();
    PersistentMemberManager memberManager = mock(PersistentMemberManager.class);
    BackupService backupService = mock(BackupService.class);
    when(cache.getBackupService()).thenReturn(backupService);
    when(cache.getPersistentMemberManager()).thenReturn(memberManager);
    when(cache.getBackupService()).thenReturn(backupService);
    when(dm.putOutgoing(isA(PrepareBackupRequest.class)))
        .thenThrow(new RuntimeException("Fail the prepare"));


    DistributedSystemBridge bridge = new DistributedSystemBridge(null, cache);
    try {
      bridge.backupAllMembers("/tmp", null);
      fail("Should have failed with an exception");
    } catch (RuntimeException expected) {
    }

    verify(dm).putOutgoing(isA(AbortBackupRequest.class));
    verify(backupService).abortBackup();
  }
}
