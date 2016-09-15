/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.beans;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;

import org.apache.geode.admin.internal.BackupDataStoreHelper;
import org.apache.geode.admin.internal.FinishBackupRequest;
import org.apache.geode.admin.internal.PrepareBackupRequest;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.persistence.BackupManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class DistributedSystemBridgeJUnitTest {
  
  private GemFireCacheImpl cache;
  private BackupManager backupManager;

  @Before
  public void createCache() throws IOException {
    cache = Fakes.cache();
    PersistentMemberManager memberManager = mock(PersistentMemberManager.class);
    backupManager = mock(BackupManager.class);
    when(cache.startBackup(any())).thenReturn(backupManager);
    when(cache.getPersistentMemberManager()).thenReturn(memberManager);
    when(cache.getBackupManager()).thenReturn(backupManager);
    
    DLockService dlock = mock(DLockService.class);
    when(dlock.lock(any(), anyLong(), anyLong())).thenReturn(true);
    
    DLockService.addLockServiceForTests(BackupDataStoreHelper.LOCK_SERVICE_NAME, dlock);
    GemFireCacheImpl.setInstanceForTests(cache);
  }
  
  @After
  public void clearCache() {
    GemFireCacheImpl.setInstanceForTests(null);
    DLockService.removeLockServiceForTests(BackupDataStoreHelper.LOCK_SERVICE_NAME);
  }
  
  @Test
  public void testSuccessfulBackup() throws Exception {
    DM dm = cache.getDistributionManager();
    
    DistributedSystemBridge bridge = new DistributedSystemBridge(null);
    bridge.backupAllMembers("/tmp", null);
    
    InOrder inOrder = inOrder(dm, backupManager);
    inOrder.verify(dm).putOutgoing(isA(PrepareBackupRequest.class));
    inOrder.verify(backupManager).prepareBackup();
    inOrder.verify(dm).putOutgoing(isA(FinishBackupRequest.class));
    inOrder.verify(backupManager).finishBackup(any(), any(), eq(false));
  }

  @Test
  public void testPrepareErrorAbortsBackup() throws Exception {
    DM dm = cache.getDistributionManager();
    PersistentMemberManager memberManager = mock(PersistentMemberManager.class);
    BackupManager backupManager = mock(BackupManager.class);
    when(cache.startBackup(any())).thenReturn(backupManager);
    when(cache.getPersistentMemberManager()).thenReturn(memberManager);
    when(cache.getBackupManager()).thenReturn(backupManager);
    when(dm.putOutgoing(isA(PrepareBackupRequest.class))).thenThrow(new RuntimeException("Fail the prepare"));
    
    
    DistributedSystemBridge bridge = new DistributedSystemBridge(null);
    try {
      bridge.backupAllMembers("/tmp", null);
      fail("Should have failed with an exception");
    } catch(RuntimeException expected) {
    }
    
    verify(dm).putOutgoing(isA(FinishBackupRequest.class));
    verify(backupManager).finishBackup(any(), any(), eq(true));
  }
}
