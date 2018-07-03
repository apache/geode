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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
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

public class PrepareBackupStepTest {

  private DistributionManager dm;
  private InternalCache cache;
  private Set<InternalDistributedMember> recipients;

  private InternalDistributedMember sender;
  private InternalDistributedMember member1;
  private InternalDistributedMember member2;

  private PrepareBackupFactory prepareBackupFactory;
  private BackupReplyProcessor prepareBackupReplyProcessor;
  private PrepareBackupRequest prepareBackupRequest;
  private PrepareBackup prepareBackup;

  private PrepareBackupStep prepareBackupStep;

  @Before
  public void setUp() throws Exception {
    dm = mock(DistributionManager.class);
    cache = mock(InternalCache.class);

    prepareBackupReplyProcessor = mock(BackupReplyProcessor.class);
    prepareBackupRequest = mock(PrepareBackupRequest.class);
    prepareBackup = mock(PrepareBackup.class);

    prepareBackupFactory = mock(PrepareBackupFactory.class);

    File targetDir = mock(File.class);
    File baselineDir = mock(File.class);

    sender = mock(InternalDistributedMember.class, "sender");
    member1 = mock(InternalDistributedMember.class, "member1");
    member2 = mock(InternalDistributedMember.class, "member2");
    recipients = new HashSet<>();

    Properties backupProperties = new BackupConfigFactory().withTargetDirPath(targetDir.toString())
        .withBaselineDirPath(baselineDir.toString()).createBackupProperties();

    prepareBackupStep = new PrepareBackupStep(dm, sender, cache, recipients,
        prepareBackupFactory, backupProperties);

    when(prepareBackupReplyProcessor.getProcessorId()).thenReturn(42);

    when(prepareBackupFactory.createReplyProcessor(eq(prepareBackupStep), eq(dm),
        eq(recipients))).thenReturn(prepareBackupReplyProcessor);
    when(prepareBackupFactory.createRequest(eq(sender), eq(recipients), eq(42),
        eq(backupProperties))).thenReturn(prepareBackupRequest);
    when(prepareBackupFactory.createPrepareBackup(eq(sender), eq(cache), eq(backupProperties)))
        .thenReturn(prepareBackup);
  }

  @Test
  public void sendShouldSendPrepareBackupMessage() throws Exception {
    prepareBackupStep.send();

    verify(dm, times(1)).putOutgoing(prepareBackupRequest);
  }

  @Test
  public void sendReturnsResultsForRemoteRecipient() throws Exception {
    HashSet<PersistentID> persistentIdsForMember1 = new HashSet<>();
    persistentIdsForMember1.add(mock(PersistentID.class));
    doAnswer(invokeAddToResults(new MemberWithPersistentIds(member1, persistentIdsForMember1)))
        .when(prepareBackupReplyProcessor).waitForReplies();

    assertThat(prepareBackupStep.send()).containsOnlyKeys(member1)
        .containsValues(persistentIdsForMember1);
  }

  @Test
  public void sendReturnsResultsForLocalMember() throws Exception {
    HashSet<PersistentID> persistentIdsForSender = new HashSet<>();
    persistentIdsForSender.add(mock(PersistentID.class));
    when(prepareBackup.run()).thenReturn(persistentIdsForSender);

    assertThat(prepareBackupStep.send()).containsOnlyKeys(sender)
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

    doAnswer(invokeAddToResults(ids)).when(prepareBackupReplyProcessor).waitForReplies();

    // prepareBackupStep.addToResults(ids[0].member, ids[0].persistentIds);
    // prepareBackupStep.addToResults(ids[1].member, ids[1].persistentIds);

    HashSet<PersistentID> persistentIdsForSender = new HashSet<>();
    persistentIdsForSender.add(mock(PersistentID.class));
    when(prepareBackup.run()).thenReturn(persistentIdsForSender);

    assertThat(prepareBackupStep.send()).containsOnlyKeys(member1, member2, sender)
        .containsValues(persistentIdsForSender, persistentIdsForMember1, persistentIdsForMember2);
  }

  @Test
  public void getResultsShouldReturnEmptyMapByDefault() throws Exception {
    assertThat(prepareBackupStep.getResults()).isEmpty();
  }

  @Test
  public void addToResultsWithNullShouldBeNoop() throws Exception {
    prepareBackupStep.addToResults(member1, null);
    assertThat(prepareBackupStep.getResults()).isEmpty();
  }

  @Test
  public void addToResultsWithEmptySetShouldBeNoop() throws Exception {
    prepareBackupStep.addToResults(member1, new HashSet<>());
    assertThat(prepareBackupStep.getResults()).isEmpty();
  }

  @Test
  public void addToResultsShouldShowUpInGetResults() throws Exception {
    HashSet<PersistentID> persistentIdsForMember1 = new HashSet<>();
    persistentIdsForMember1.add(mock(PersistentID.class));
    prepareBackupStep.addToResults(member1, persistentIdsForMember1);
    assertThat(prepareBackupStep.getResults()).containsOnlyKeys(member1)
        .containsValue(persistentIdsForMember1);
  }

  @Test
  public void sendShouldHandleIOExceptionThrownFromRun() throws Exception {
    when(prepareBackup.run()).thenThrow(new IOException("expected exception"));
    prepareBackupStep.send();
  }

  @Test(expected = RuntimeException.class)
  public void sendShouldThrowNonIOExceptionThrownFromRun() throws Exception {
    when(prepareBackup.run()).thenThrow(new RuntimeException("expected exception"));
    prepareBackupStep.send();
  }

  @Test
  public void sendShouldHandleCancelExceptionFromWaitForReplies() throws Exception {
    ReplyException replyException =
        new ReplyException("expected exception", new CacheClosedException("expected exception"));
    doThrow(replyException).when(prepareBackupReplyProcessor).waitForReplies();
    prepareBackupStep.send();
  }

  @Test
  public void sendShouldHandleInterruptedExceptionFromWaitForReplies() throws Exception {
    doThrow(new InterruptedException("expected exception")).when(prepareBackupReplyProcessor)
        .waitForReplies();
    prepareBackupStep.send();
  }

  @Test(expected = ReplyException.class)
  public void sendShouldThrowReplyExceptionWithNoCauseFromWaitForReplies() throws Exception {
    doThrow(new ReplyException("expected exception")).when(prepareBackupReplyProcessor)
        .waitForReplies();
    prepareBackupStep.send();
  }

  @Test(expected = ReplyException.class)
  public void sendShouldThrowReplyExceptionWithCauseThatIsNotACancelFromWaitForReplies()
      throws Exception {
    doThrow(new ReplyException("expected exception", new RuntimeException("expected")))
        .when(prepareBackupReplyProcessor).waitForReplies();
    prepareBackupStep.send();
  }

  @Test
  public void sendShouldPrepareForBackupInLocalMemberBeforeWaitingForReplies() throws Exception {
    InOrder inOrder = inOrder(prepareBackup, prepareBackupReplyProcessor);
    prepareBackupStep.send();

    inOrder.verify(prepareBackup, times(1)).run();
    inOrder.verify(prepareBackupReplyProcessor, times(1)).waitForReplies();
  }

  private Answer<Object> invokeAddToResults(MemberWithPersistentIds... memberWithPersistentIds) {
    return invocation -> {
      for (MemberWithPersistentIds ids : memberWithPersistentIds) {
        prepareBackupStep.addToResults(ids.member, ids.persistentIds);
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
