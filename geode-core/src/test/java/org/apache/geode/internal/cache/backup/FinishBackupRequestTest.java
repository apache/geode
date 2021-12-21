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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AdminFailureResponse;
import org.apache.geode.internal.cache.InternalCache;

public class FinishBackupRequestTest {

  private FinishBackupRequest finishBackupRequest;

  private DistributionManager dm;
  private InternalCache cache;
  private BackupService backupService;
  private final int processorId = 79;
  private boolean abort;
  private FinishBackupFactory finishBackupFactory;
  private InternalDistributedMember sender;
  private Set<InternalDistributedMember> recipients;
  private HashSet<PersistentID> persistentIds;
  private FinishBackup finishBackup;

  @Before
  public void setUp() throws Exception {
    // mocks here
    dm = mock(DistributionManager.class);
    cache = mock(InternalCache.class);
    backupService = mock(BackupService.class);
    abort = false;

    when(dm.getCache()).thenReturn(cache);
    when(dm.getDistributionManagerId()).thenReturn(sender);
    when(cache.getBackupService()).thenReturn(backupService);

    sender = mock(InternalDistributedMember.class);

    recipients = new HashSet<>();
    persistentIds = new HashSet<>();

    finishBackup = mock(FinishBackup.class);
    when(finishBackup.run()).thenReturn(persistentIds);

    finishBackupFactory = mock(FinishBackupFactory.class);
    when(finishBackupFactory.createFinishBackup(eq(cache))).thenReturn(finishBackup);
    when(finishBackupFactory.createBackupResponse(eq(sender), eq(persistentIds)))
        .thenReturn(mock(BackupResponse.class));


    finishBackupRequest =
        new FinishBackupRequest(sender, recipients, processorId, finishBackupFactory);
  }

  @Test
  public void usesFactoryToCreateFinishBackup() throws Exception {
    finishBackupRequest.createResponse(dm);

    verify(finishBackupFactory, times(1)).createFinishBackup(eq(cache));
  }

  @Test
  public void usesFactoryToCreateBackupResponse() throws Exception {
    finishBackupRequest.createResponse(dm);

    verify(finishBackupFactory, times(1)).createBackupResponse(eq(sender), eq(persistentIds));
  }

  @Test
  public void returnsBackupResponse() throws Exception {
    assertThat(finishBackupRequest.createResponse(dm)).isInstanceOf(BackupResponse.class);
  }

  @Test
  public void returnsAdminFailureResponseWhenFinishBackupThrowsIOException() throws Exception {
    when(finishBackup.run()).thenThrow(new IOException());

    assertThat(finishBackupRequest.createResponse(dm)).isInstanceOf(AdminFailureResponse.class);
  }
}
