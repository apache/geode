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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class PrepareBackupFactoryTest {

  private PrepareBackupFactory prepareBackupFactory;

  private BackupResultCollector resultCollector;
  private DistributionManager dm;
  private InternalDistributedMember sender;
  private Set<InternalDistributedMember> recipients;
  private InternalDistributedMember member;
  private InternalCache cache;
  private File targetDir;

  @Before
  public void setUp() throws Exception {
    resultCollector = mock(BackupResultCollector.class);
    dm = mock(DistributionManager.class);
    sender = mock(InternalDistributedMember.class);
    member = mock(InternalDistributedMember.class);
    cache = mock(InternalCache.class);
    targetDir = mock(File.class);

    recipients = new HashSet<>();

    when(dm.getSystem()).thenReturn(mock(InternalDistributedSystem.class));
    when(dm.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    prepareBackupFactory = new PrepareBackupFactory();
  }

  @Test
  public void createReplyProcessorReturnsBackupReplyProcessor() throws Exception {
    assertThat(prepareBackupFactory.createReplyProcessor(resultCollector, dm, recipients))
        .isInstanceOf(BackupReplyProcessor.class);
  }

  @Test
  public void createRequestReturnsPrepareBackupRequest() throws Exception {
    assertThat(prepareBackupFactory.createRequest(sender, recipients, 1, targetDir, null))
        .isInstanceOf(PrepareBackupRequest.class);
  }

  @Test
  public void createPrepareBackupReturnsPrepareBackup() throws Exception {
    assertThat(prepareBackupFactory.createPrepareBackup(member, cache, targetDir, null))
        .isInstanceOf(PrepareBackup.class);
  }

  @Test
  public void createBackupResponseReturnsBackupResponse() {
    assertThat(prepareBackupFactory.createBackupResponse(member, null))
        .isInstanceOf(BackupResponse.class);
  }
}
