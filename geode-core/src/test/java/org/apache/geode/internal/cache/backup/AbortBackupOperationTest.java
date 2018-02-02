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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AbortBackupOperationTest {

  private DistributionManager dm;

  private InternalDistributedMember sender;
  private InternalDistributedMember member1;
  private InternalDistributedMember member2;

  private BackupReplyProcessor backupReplyProcessor;
  private AbortBackupRequest abortBackupRequest;
  private AbortBackup abortBackup;

  private AbortBackupOperation abortBackupOperation;

  @Before
  public void setUp() throws Exception {
    dm = mock(DistributionManager.class);
    InternalCache cache = mock(InternalCache.class);

    backupReplyProcessor = mock(BackupReplyProcessor.class);
    abortBackupRequest = mock(AbortBackupRequest.class);
    abortBackup = mock(AbortBackup.class);

    AbortBackupFactory abortBackupFactory = mock(AbortBackupFactory.class);

    sender = mock(InternalDistributedMember.class, "sender");
    member1 = mock(InternalDistributedMember.class, "member1");
    member2 = mock(InternalDistributedMember.class, "member2");
    Set<InternalDistributedMember> recipients = new HashSet<>();

    abortBackupOperation =
        new AbortBackupOperation(dm, sender, cache, recipients, abortBackupFactory);

    when(backupReplyProcessor.getProcessorId()).thenReturn(42);

    when(abortBackupFactory.createReplyProcessor(eq(abortBackupOperation), eq(dm), eq(recipients)))
        .thenReturn(backupReplyProcessor);
    when(abortBackupFactory.createRequest(eq(sender), eq(recipients), eq(42)))
        .thenReturn(abortBackupRequest);
    when(abortBackupFactory.createAbortBackup(eq(cache))).thenReturn(abortBackup);
  }

  @Test
  public void sendShouldSendAbortBackupMessage() {
    abortBackupOperation.send();

    verify(dm, times(1)).putOutgoing(abortBackupRequest);
  }

  @Test
  public void sendReturnsResultsForLocalMember() {
    assertThat(abortBackupOperation.send()).containsOnlyKeys(sender);
  }

  @Test
  public void sendReturnsResultsForAllMembers() throws Exception {
    MemberWithPersistentIds[] ids =
        new MemberWithPersistentIds[] {new MemberWithPersistentIds(member1, new HashSet<>()),
            new MemberWithPersistentIds(member2, new HashSet<>())};

    doAnswer(invokeAddToResults(ids)).when(backupReplyProcessor).waitForReplies();

    assertThat(abortBackupOperation.send()).containsOnlyKeys(member1, member2, sender);
  }

  @Test
  public void getResultsShouldReturnEmptyMapByDefault() {
    assertThat(abortBackupOperation.getResults()).isEmpty();
  }

  @Test
  public void addToResultsWithNullShouldBeNoop() {
    abortBackupOperation.addToResults(member1, null);
    assertThat(abortBackupOperation.getResults()).isEmpty();
  }

  @Test
  public void addToResultsShouldShowUpInGetResults() {
    abortBackupOperation.addToResults(member1, new HashSet<>());
    assertThat(abortBackupOperation.getResults()).containsOnlyKeys(member1);
  }

  @Test
  public void sendShouldHandleCancelExceptionFromWaitForReplies() throws Exception {
    ReplyException replyException =
        new ReplyException("expected exception", new CacheClosedException("expected exception"));
    doThrow(replyException).when(backupReplyProcessor).waitForReplies();
    abortBackupOperation.send();
  }

  @Test
  public void sendShouldHandleInterruptedExceptionFromWaitForReplies() throws Exception {
    doThrow(new InterruptedException("expected exception")).when(backupReplyProcessor)
        .waitForReplies();
    abortBackupOperation.send();
  }

  @Test(expected = ReplyException.class)
  public void sendShouldThrowReplyExceptionWithNoCauseFromWaitForReplies() throws Exception {
    doThrow(new ReplyException("expected exception")).when(backupReplyProcessor).waitForReplies();
    abortBackupOperation.send();
  }

  @Test(expected = ReplyException.class)
  public void sendShouldThrowReplyExceptionWithCauseThatIsNotACancelFromWaitForReplies()
      throws Exception {
    doThrow(new ReplyException("expected exception", new RuntimeException("expected")))
        .when(backupReplyProcessor).waitForReplies();
    abortBackupOperation.send();
  }

  @Test
  public void sendShouldAbortBackupInLocalMemberBeforeWaitingForReplies() throws Exception {
    InOrder inOrder = inOrder(abortBackup, backupReplyProcessor);
    abortBackupOperation.send();

    inOrder.verify(abortBackup, times(1)).run();
    inOrder.verify(backupReplyProcessor, times(1)).waitForReplies();
  }

  private Answer<Object> invokeAddToResults(MemberWithPersistentIds... memberWithPersistentIds) {
    return invocation -> {
      for (MemberWithPersistentIds ids : memberWithPersistentIds) {
        abortBackupOperation.addToResults(ids.member, ids.persistentIds);
      }
      return null;
    };
  }

  private static class MemberWithPersistentIds {
    InternalDistributedMember member;
    Set<PersistentID> persistentIds;

    MemberWithPersistentIds(InternalDistributedMember member, HashSet<PersistentID> persistentIds) {
      this.member = member;
      this.persistentIds = persistentIds;
    }
  }
}
