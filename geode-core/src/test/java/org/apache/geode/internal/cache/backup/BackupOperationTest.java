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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.backup.BackupOperation.MissingPersistentMembersProvider;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.management.ManagementException;

public class BackupOperationTest {

  private FlushToDiskFactory flushToDiskFactory;
  private PrepareBackupFactory prepareBackupFactory;
  private AbortBackupFactory abortBackupFactory;
  private FinishBackupFactory finishBackupFactory;
  private DistributionManager dm;
  private InternalCache cache;
  private BackupLockService backupLockService;
  private MissingPersistentMembersProvider missingPersistentMembersProvider;

  private String targetDirPath;
  private String baselineDirPath;

  private BackupOperation backupOperation;

  @Before
  public void setUp() {
    flushToDiskFactory = mock(FlushToDiskFactory.class, RETURNS_DEEP_STUBS);
    prepareBackupFactory = mock(PrepareBackupFactory.class, RETURNS_DEEP_STUBS);
    abortBackupFactory = mock(AbortBackupFactory.class, RETURNS_DEEP_STUBS);
    finishBackupFactory = mock(FinishBackupFactory.class, RETURNS_DEEP_STUBS);
    dm = mock(DistributionManager.class);
    cache = mock(InternalCache.class);
    backupLockService = mock(BackupLockService.class);
    missingPersistentMembersProvider = mock(MissingPersistentMembersProvider.class);

    when(backupLockService.obtainLock(dm)).thenReturn(true);

    targetDirPath = "targetDirPath";
    baselineDirPath = "baselineDirPath";

    backupOperation = new BackupOperation(flushToDiskFactory, prepareBackupFactory,
        abortBackupFactory, finishBackupFactory, dm, cache, backupLockService,
        missingPersistentMembersProvider);
  }

  @Test
  public void hasNoBackedUpDiskStoresIfNoMembers() {
    BackupStatus backupStatus = backupOperation.backupAllMembers(targetDirPath, baselineDirPath);
    assertThat(backupStatus.getBackedUpDiskStores()).hasSize(0);
  }

  @Test
  public void hasNoOfflineDiskStoresIfNoMembers() {
    BackupStatus backupStatus = backupOperation.backupAllMembers(targetDirPath, baselineDirPath);
    assertThat(backupStatus.getOfflineDiskStores()).hasSize(0);
  }

  @Test
  public void flushPrepareFinishOrdering() {
    backupOperation.backupAllMembers(targetDirPath, baselineDirPath);

    InOrder inOrder = inOrder(flushToDiskFactory, prepareBackupFactory, finishBackupFactory);
    inOrder.verify(flushToDiskFactory).createFlushToDiskStep(any(), any(), any(), any(), any());
    inOrder.verify(prepareBackupFactory).createPrepareBackupStep(any(), any(), any(), any(), any(),
        any());
    inOrder.verify(finishBackupFactory).createFinishBackupStep(any(), any(), any(), any(), any());
  }

  @Test
  public void abortIfPrepareFails() {
    PrepareBackupStep prepareBackupStep = mock(PrepareBackupStep.class);
    RuntimeException thrownBySend = new RuntimeException("thrownBySend");

    when(prepareBackupFactory.createPrepareBackupStep(any(), any(), any(), any(), any(), any()))
        .thenReturn(prepareBackupStep);
    when(prepareBackupStep.send()).thenThrow(thrownBySend);

    assertThatThrownBy(() -> backupOperation.backupAllMembers(targetDirPath, baselineDirPath))
        .isSameAs(thrownBySend);

    InOrder inOrder = inOrder(flushToDiskFactory, prepareBackupFactory, abortBackupFactory);
    inOrder.verify(flushToDiskFactory).createFlushToDiskStep(any(), any(), any(), any(), any());
    inOrder.verify(prepareBackupFactory).createPrepareBackupStep(any(), any(), any(), any(), any(),
        any());
    inOrder.verify(abortBackupFactory).createAbortBackupStep(any(), any(), any(), any(), any());

    verifyZeroInteractions(finishBackupFactory);
  }

  @Test
  public void failedToAcquireLockThrows() {
    when(backupLockService.obtainLock(dm)).thenReturn(false);

    assertThatThrownBy(() -> backupOperation.backupAllMembers(targetDirPath, baselineDirPath))
        .isInstanceOf(ManagementException.class);

    verifyZeroInteractions(flushToDiskFactory, prepareBackupFactory, abortBackupFactory,
        finishBackupFactory);
  }
}
