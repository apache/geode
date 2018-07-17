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
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AdminFailureResponse;
import org.apache.geode.internal.cache.InternalCache;

public class PrepareBackupRequestTest {

  private PrepareBackupRequest prepareBackupRequest;

  private DistributionManager dm;
  private Set<InternalDistributedMember> recipients;
  private int msgId;
  private PrepareBackupFactory prepareBackupFactory;
  private InternalDistributedMember sender;
  private InternalCache cache;
  private HashSet<PersistentID> persistentIds;
  private PrepareBackup prepareBackup;
  private File targetDir;
  private File baselineDir;
  private Properties backupProperties;

  @Before
  public void setUp() throws Exception {
    dm = mock(DistributionManager.class);
    sender = mock(InternalDistributedMember.class);
    cache = mock(InternalCache.class);
    prepareBackupFactory = mock(PrepareBackupFactory.class);
    prepareBackup = mock(PrepareBackup.class);
    targetDir = mock(File.class);
    baselineDir = mock(File.class);

    msgId = 42;
    recipients = new HashSet<>();
    persistentIds = new HashSet<>();

    backupProperties = new BackupConfigFactory().withTargetDirPath(targetDir.toString())
        .withBaselineDirPath(baselineDir.toString()).createBackupProperties();

    when(dm.getCache()).thenReturn(cache);
    when(dm.getDistributionManagerId()).thenReturn(sender);
    when(prepareBackupFactory.createPrepareBackup(eq(sender), eq(cache), eq(backupProperties)))
        .thenReturn(prepareBackup);
    when(prepareBackupFactory.createBackupResponse(eq(sender), eq(persistentIds)))
        .thenReturn(mock(BackupResponse.class));
    when(prepareBackup.run()).thenReturn(persistentIds);

    prepareBackupRequest =
        new PrepareBackupRequest(sender, recipients, msgId, prepareBackupFactory, backupProperties);
  }

  @Test
  public void usesFactoryToCreatePrepareBackup() throws Exception {
    prepareBackupRequest.createResponse(dm);

    verify(prepareBackupFactory, times(1)).createPrepareBackup(eq(sender), eq(cache),
        eq(backupProperties));
  }

  @Test
  public void usesFactoryToCreateBackupResponse() throws Exception {
    prepareBackupRequest.createResponse(dm);

    verify(prepareBackupFactory, times(1)).createBackupResponse(eq(sender), eq(persistentIds));
  }

  @Test
  public void returnsBackupResponse() throws Exception {
    assertThat(prepareBackupRequest.createResponse(dm)).isInstanceOf(BackupResponse.class);
  }

  @Test
  public void returnsAdminFailureResponseWhenPrepareBackupThrowsIOException() throws Exception {
    when(prepareBackup.run()).thenThrow(new IOException());

    assertThat(prepareBackupRequest.createResponse(dm)).isInstanceOf(AdminFailureResponse.class);
  }

}
