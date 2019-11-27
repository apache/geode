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

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.ClusterMessage;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class BackupReplyProcessorTest {

  private BackupReplyProcessor backupReplyProcessor;

  private BackupResultCollector resultCollector;
  private DistributionManager dm;
  private InternalDistributedSystem system;
  private InternalDistributedMember sender;

  private Set<InternalDistributedMember> recipients;
  private Set<PersistentID> persistentIds;

  private BackupResponse backupResponse;
  private ClusterMessage nonBackupResponse;

  @Before
  public void setUp() throws Exception {
    resultCollector = mock(BackupResultCollector.class);
    dm = mock(DistributionManager.class);
    system = mock(InternalDistributedSystem.class);
    backupResponse = mock(BackupResponse.class);
    nonBackupResponse = mock(ClusterMessage.class);
    sender = mock(InternalDistributedMember.class);

    recipients = new HashSet<>();
    persistentIds = new HashSet<>();

    when(dm.getSystem()).thenReturn(system);
    when(dm.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(backupResponse.getSender()).thenReturn(sender);
    when(backupResponse.getPersistentIds()).thenReturn(persistentIds);
    when(nonBackupResponse.getSender()).thenReturn(sender);

    backupReplyProcessor = new BackupReplyProcessor(resultCollector, dm, recipients);
  }

  @Test
  public void stopBecauseOfExceptionsReturnsFalse() throws Exception {
    assertThat(backupReplyProcessor.stopBecauseOfExceptions()).isFalse();
  }

  @Test
  public void processBackupResponseAddsSenderToResults() throws Exception {
    backupReplyProcessor.process(backupResponse, false);

    verify(resultCollector, times(1)).addToResults(eq(sender), eq(persistentIds));
  }

  @Test
  public void processNonBackupResponseDoesNotAddSenderToResults() throws Exception {
    backupReplyProcessor.process(nonBackupResponse, false);

    verify(resultCollector, times(0)).addToResults(eq(sender), eq(persistentIds));
  }
}
