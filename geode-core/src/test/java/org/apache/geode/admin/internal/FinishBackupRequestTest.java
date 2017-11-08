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
package org.apache.geode.admin.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;

import org.apache.geode.admin.internal.FinishBackupRequest.FinishBackupReplyProcessor;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AdminFailureResponse;
import org.apache.geode.internal.admin.remote.AdminResponse;
import org.apache.geode.internal.cache.BackupManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class FinishBackupRequestTest {

  private FinishBackupRequest finishBackupRequest;

  private FinishBackupReplyProcessor replyProcessor;
  private DM dm;
  private InternalCache cache;
  private BackupManager backupManager;
  private File targetDir;
  private File baselineDir;

  private InternalDistributedMember localMember;
  private InternalDistributedMember member1;
  private InternalDistributedMember member2;

  private Set<InternalDistributedMember> recipients;

  @Before
  public void setUp() throws Exception {
    // mocks here
    replyProcessor = mock(FinishBackupReplyProcessor.class);
    dm = mock(DM.class);
    cache = mock(InternalCache.class);
    backupManager = mock(BackupManager.class);
    targetDir = mock(File.class);
    baselineDir = mock(File.class);

    when(dm.getCache()).thenReturn(cache);
    when(dm.getDistributionManagerId()).thenReturn(localMember);
    when(cache.getBackupManager()).thenReturn(backupManager);
    when(replyProcessor.getResults()).thenReturn(Collections.emptyMap());

    localMember = mock(InternalDistributedMember.class);
    member1 = mock(InternalDistributedMember.class);
    member2 = mock(InternalDistributedMember.class);

    recipients = new HashSet<>();
    recipients.add(member1);
    recipients.add(member2);

    finishBackupRequest =
        new FinishBackupRequest(dm, recipients, replyProcessor, targetDir, baselineDir, false);
  }

  @Test
  public void getRecipientsReturnsRecipientMembers() throws Exception {
    assertThat(finishBackupRequest.getRecipients()).hasSize(2).contains(member1, member2);
  }

  @Test
  public void getRecipientsDoesNotIncludeNull() throws Exception {
    InternalDistributedMember nullMember = null;

    assertThat(finishBackupRequest.getRecipients()).doesNotContain(nullMember);
  }

  @Test
  public void sendShouldUseDMToSendMessage() throws Exception {
    finishBackupRequest.send();

    verify(dm, times(1)).putOutgoing(finishBackupRequest);
  }

  @Test
  public void sendShouldWaitForRepliesFromRecipients() throws Exception {
    finishBackupRequest.send();

    verify(replyProcessor, times(1)).waitForReplies();
  }

  @Test
  public void sendShouldReturnResultsContainingRecipientsAndLocalMember() throws Exception {
    Set<PersistentID> localMember_PersistentIdSet = new HashSet<>();
    localMember_PersistentIdSet.add(mock(PersistentID.class));
    Set<PersistentID> member1_PersistentIdSet = new HashSet<>();
    member1_PersistentIdSet.add(mock(PersistentID.class));
    Set<PersistentID> member2_PersistentIdSet = new HashSet<>();
    member2_PersistentIdSet.add(mock(PersistentID.class));
    member2_PersistentIdSet.add(mock(PersistentID.class));
    Map<DistributedMember, Set<PersistentID>> expectedResults = new HashMap<>();
    expectedResults.put(localMember, localMember_PersistentIdSet);
    expectedResults.put(member1, member1_PersistentIdSet);
    expectedResults.put(member2, member2_PersistentIdSet);
    when(replyProcessor.getResults()).thenReturn(expectedResults);

    Map<DistributedMember, Set<PersistentID>> results = finishBackupRequest.send();

    assertThat(results).isEqualTo(expectedResults);
  }

  @Test
  public void sendShouldInvokeProcessLocally() throws Exception {
    finishBackupRequest.send();

    verify(replyProcessor, times(1)).process(any(AdminResponse.class), eq(false));
  }

  @Test
  public void sendShouldInvokeDoBackupLocally() throws Exception {
    finishBackupRequest.send();

    verify(backupManager, times(1)).doBackup(targetDir, baselineDir, false);
  }

  /**
   * Confirms fix for GEODE-3940: Backup can hang while trying to get a lock
   */
  @Test
  public void sendShouldDoBackupInLocalMemberBeforeWaitingForReplies() throws Exception {
    InOrder inOrder = inOrder(backupManager, replyProcessor);

    finishBackupRequest.send();

    // assert that doBackup which releases BackupLock is invoked before invoking waitForReplies
    inOrder.verify(backupManager, times(1)).doBackup(targetDir, baselineDir, false);
    inOrder.verify(replyProcessor, times(1)).waitForReplies();
  }

  @Test
  public void repliesWithFinishBackupResponse() throws Exception {
    finishBackupRequest.send();

    verify(replyProcessor, times(1)).process(any(FinishBackupResponse.class), eq(false));
  }

  @Test
  public void repliesWithAdminFailureResponseWhenDoBackupThrowsIOException() throws Exception {
    when(backupManager.doBackup(targetDir, baselineDir, false)).thenThrow(new IOException());

    finishBackupRequest.send();

    verify(replyProcessor, times(1)).process(any(AdminFailureResponse.class), eq(false));
  }

  @Test
  public void sendShouldCompleteIfWaitForRepliesThrowsReplyExceptionCausedByCacheClosedException()
      throws Exception {
    doThrow(new ReplyException(new CacheClosedException())).when(replyProcessor).waitForReplies();

    finishBackupRequest.send();
  }

  @Test
  public void sendShouldThrowIfWaitForRepliesThrowsReplyExceptionNotCausedByCacheClosedException()
      throws Exception {
    doThrow(new ReplyException(new NullPointerException())).when(replyProcessor).waitForReplies();

    assertThatThrownBy(() -> finishBackupRequest.send()).isInstanceOf(ReplyException.class)
        .hasCauseInstanceOf(NullPointerException.class);
  }

  @Test
  public void sendCompletesWhenWaitForRepliesThrowsInterruptedException() throws Exception {
    doThrow(new InterruptedException()).when(replyProcessor).waitForReplies();

    finishBackupRequest.send();
  }

}
