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

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;

public class AbortBackupRequestTest {

  private AbortBackupRequest abortBackupRequest;
  private DistributionManager dm;
  private InternalCache cache;
  private AbortBackupFactory abortBackupFactory;
  private InternalDistributedMember sender;

  @Before
  public void setUp() throws Exception {
    // mocks here
    dm = mock(DistributionManager.class);
    cache = mock(InternalCache.class);
    BackupService backupService = mock(BackupService.class);

    when(dm.getCache()).thenReturn(cache);
    when(dm.getDistributionManagerId()).thenReturn(sender);
    when(cache.getBackupService()).thenReturn(backupService);

    sender = mock(InternalDistributedMember.class);

    Set<InternalDistributedMember> recipients = new HashSet<>();

    AbortBackup abortBackup = mock(AbortBackup.class);

    abortBackupFactory = mock(AbortBackupFactory.class);
    when(abortBackupFactory.createAbortBackup(eq(cache))).thenReturn(abortBackup);
    when(abortBackupFactory.createBackupResponse(eq(sender), eq(new HashSet<>())))
        .thenReturn(mock(BackupResponse.class));

    int processorId = 79;
    abortBackupRequest =
        new AbortBackupRequest(sender, recipients, processorId, abortBackupFactory);
  }

  @Test
  public void usesFactoryToCreateAbortBackupAndResponse() {
    assertThat(abortBackupRequest.createResponse(dm)).isInstanceOf(BackupResponse.class);

    verify(abortBackupFactory, times(1)).createAbortBackup(eq(cache));
    verify(abortBackupFactory, times(1)).createBackupResponse(eq(sender), eq(new HashSet<>()));
  }
}
