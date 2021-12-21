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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;

public class FinishBackupStepTest {

  private DistributionManager dm;
  private InternalCache cache;
  private Set<InternalDistributedMember> recipients;

  private InternalDistributedMember sender;
  private InternalDistributedMember member1;
  private InternalDistributedMember member2;

  private final boolean abort = false;

  private FinishBackupFactory finishBackupFactory;
  private BackupReplyProcessor finishBackupReplyProcessor;
  private FinishBackupRequest finishBackupRequest;
  private FinishBackup finishBackup;

  private FinishBackupStep finishBackupStep;

  @Before
  public void setUp() throws Exception {
    dm = mock(DistributionManager.class);
    cache = mock(InternalCache.class);

    finishBackupReplyProcessor = mock(BackupReplyProcessor.class);
    finishBackupRequest = mock(FinishBackupRequest.class);
    finishBackup = mock(FinishBackup.class);

    finishBackupFactory = mock(FinishBackupFactory.class);

    sender = mock(InternalDistributedMember.class, "sender");
    member1 = mock(InternalDistributedMember.class, "member1");
    member2 = mock(InternalDistributedMember.class, "member2");
    recipients = new HashSet<>();

    finishBackupStep =
        new FinishBackupStep(dm, sender, cache, recipients, finishBackupFactory);

    when(finishBackupReplyProcessor.getProcessorId()).thenReturn(42);

    when(
        finishBackupFactory.createReplyProcessor(eq(finishBackupStep), eq(dm), eq(recipients)))
            .thenReturn(finishBackupReplyProcessor);
    when(finishBackupFactory.createRequest(eq(sender), eq(recipients), eq(42)))
        .thenReturn(finishBackupRequest);
    when(finishBackupFactory.createFinishBackup(eq(cache))).thenReturn(finishBackup);
  }

  @Test
  public void sendShouldSendfinishBackupMessage() throws Exception {
    finishBackupStep.send();

    verify(dm, times(1)).putOutgoing(finishBackupRequest);
  }

  @Test
  public void sendReturnsResultsForRemoteRecipient() throws Exception {
    HashSet<PersistentID> persistentIdsForMember1 = new HashSet<>();
    persistentIdsForMember1.add(mock(PersistentID.class));
    doAnswer(invokeAddToResults(new MemberWithPersistentIds(member1, persistentIdsForMember1)))
        .when(finishBackupReplyProcessor).waitForReplies();

    assertThat(finishBackupStep.send()).containsOnlyKeys(member1)
        .containsValues(persistentIdsForMember1);
  }

  @Test
  public void sendReturnsResultsForLocalMember() throws Exception {
    HashSet<PersistentID> persistentIdsForSender = new HashSet<>();
    persistentIdsForSender.add(mock(PersistentID.class));
    when(finishBackup.run()).thenReturn(persistentIdsForSender);

    assertThat(finishBackupStep.send()).containsOnlyKeys(sender)
        .containsValue(persistentIdsForSender);
  }

  @Test
  public void sendReturnsResultsForAllMembers() throws Exception {
    HashSet<PersistentID> persistentIdsForMember1 = new HashSet<>();
    persistentIdsForMember1.add(mock(PersistentID.class));

    HashSet<PersistentID> persistentIdsForMember2 = new HashSet<>();
    persistentIdsForMember2.add(mock(PersistentID.class));

    MemberWithPersistentIds[] ids = new MemberWithPersistentIds[] {
        new MemberWithPersistentIds(member1, persistentIdsForMember1),
        new MemberWithPersistentIds(member2, persistentIdsForMember2)};

    doAnswer(invokeAddToResults(ids)).when(finishBackupReplyProcessor).waitForReplies();

    HashSet<PersistentID> persistentIdsForSender = new HashSet<>();
    persistentIdsForSender.add(mock(PersistentID.class));
    when(finishBackup.run()).thenReturn(persistentIdsForSender);

    assertThat(finishBackupStep.send()).containsOnlyKeys(member1, member2, sender)
        .containsValues(persistentIdsForSender, persistentIdsForMember1, persistentIdsForMember2);
  }

  @Test
  public void getResultsShouldReturnEmptyMapByDefault() throws Exception {
    assertThat(finishBackupStep.getResults()).isEmpty();
  }

  @Test
  public void addToResultsWithNullShouldBeNoop() throws Exception {
    finishBackupStep.addToResults(member1, null);
    assertThat(finishBackupStep.getResults()).isEmpty();
  }

  @Test
  public void addToResultsWithEmptySetShouldBeNoop() throws Exception {
    finishBackupStep.addToResults(member1, new HashSet<>());
    assertThat(finishBackupStep.getResults()).isEmpty();
  }

  @Test
  public void addToResultsShouldShowUpInGetResults() throws Exception {
    HashSet<PersistentID> persistentIdsForMember1 = new HashSet<>();
    persistentIdsForMember1.add(mock(PersistentID.class));
    finishBackupStep.addToResults(member1, persistentIdsForMember1);
    assertThat(finishBackupStep.getResults()).containsOnlyKeys(member1)
        .containsValue(persistentIdsForMember1);
  }

  @Test
  public void sendShouldHandleIOExceptionThrownFromRun() throws Exception {
    when(finishBackup.run()).thenThrow(new IOException("expected exception"));
    finishBackupStep.send();
  }

  @Test(expected = RuntimeException.class)
  public void sendShouldThrowNonIOExceptionThrownFromRun() throws Exception {
    when(finishBackup.run()).thenThrow(new RuntimeException("expected exception"));
    finishBackupStep.send();
  }

  @Test
  public void sendShouldHandleCancelExceptionFromWaitForReplies() throws Exception {
    ReplyException replyException =
        new ReplyException("expected exception", new CacheClosedException("expected exception"));
    doThrow(replyException).when(finishBackupReplyProcessor).waitForReplies();
    finishBackupStep.send();
  }

  @Test
  public void sendShouldHandleInterruptedExceptionFromWaitForReplies() throws Exception {
    doThrow(new InterruptedException("expected exception")).when(finishBackupReplyProcessor)
        .waitForReplies();
    finishBackupStep.send();
  }

  @Test(expected = ReplyException.class)
  public void sendShouldThrowReplyExceptionWithNoCauseFromWaitForReplies() throws Exception {
    doThrow(new ReplyException("expected exception")).when(finishBackupReplyProcessor)
        .waitForReplies();
    finishBackupStep.send();
  }

  @Test(expected = ReplyException.class)
  public void sendShouldThrowReplyExceptionWithCauseThatIsNotACancelFromWaitForReplies()
      throws Exception {
    doThrow(new ReplyException("expected exception", new RuntimeException("expected")))
        .when(finishBackupReplyProcessor).waitForReplies();
    finishBackupStep.send();
  }

  @Test
  public void sendShouldfinishForBackupInLocalMemberBeforeWaitingForReplies() throws Exception {
    InOrder inOrder = inOrder(finishBackup, finishBackupReplyProcessor);
    finishBackupStep.send();

    inOrder.verify(finishBackup, times(1)).run();
    inOrder.verify(finishBackupReplyProcessor, times(1)).waitForReplies();
  }

  private Answer<Object> invokeAddToResults(MemberWithPersistentIds... memberWithPersistentIds) {
    return invocation -> {
      for (MemberWithPersistentIds ids : memberWithPersistentIds) {
        finishBackupStep.addToResults(ids.member, ids.persistentIds);
      }
      return null;
    };
  }

  private static class MemberWithPersistentIds {
    InternalDistributedMember member;
    HashSet<PersistentID> persistentIds;

    MemberWithPersistentIds(InternalDistributedMember member, HashSet<PersistentID> persistentIds) {
      this.member = member;
      this.persistentIds = persistentIds;
    }
  }
}
