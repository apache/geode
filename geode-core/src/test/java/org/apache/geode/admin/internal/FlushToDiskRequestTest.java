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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;

import org.apache.geode.admin.internal.FlushToDiskRequest.FlushToDiskProcessor;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AdminResponse;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class FlushToDiskRequestTest {

  private FlushToDiskRequest flushToDiskRequest;

  private FlushToDiskProcessor replyProcessor;
  private DM dm;
  private InternalCache cache;

  private DiskStore diskStore1;
  private DiskStore diskStore2;
  private Collection<DiskStore> diskStoreCollection;

  private InternalDistributedMember localMember;
  private InternalDistributedMember member1;
  private InternalDistributedMember member2;

  private Set<InternalDistributedMember> recipients;

  @Before
  public void setUp() throws Exception {
    // mocks here
    replyProcessor = mock(FlushToDiskProcessor.class);
    dm = mock(DM.class);
    cache = mock(InternalCache.class);
    diskStore1 = mock(DiskStore.class);
    diskStore2 = mock(DiskStore.class);

    diskStoreCollection = new ArrayList<>();
    diskStoreCollection.add(diskStore1);
    diskStoreCollection.add(diskStore2);

    when(dm.getCache()).thenReturn(cache);
    when(dm.getDistributionManagerId()).thenReturn(localMember);
    when(cache.listDiskStoresIncludingRegionOwned()).thenReturn(diskStoreCollection);

    localMember = mock(InternalDistributedMember.class);
    member1 = mock(InternalDistributedMember.class);
    member2 = mock(InternalDistributedMember.class);

    recipients = new HashSet<>();
    recipients.add(member1);
    recipients.add(member2);

    flushToDiskRequest = new FlushToDiskRequest(dm, recipients, replyProcessor);
  }

  @Test
  public void getRecipientsReturnsRecipientMembers() throws Exception {
    assertThat(flushToDiskRequest.getRecipients()).hasSize(2).contains(member1, member2);
  }

  @Test
  public void getRecipientsDoesNotIncludeNull() throws Exception {
    InternalDistributedMember nullMember = null;

    assertThat(flushToDiskRequest.getRecipients()).doesNotContain(nullMember);
  }

  @Test
  public void sendShouldUseDMToSendMessage() throws Exception {
    flushToDiskRequest.send();

    verify(dm, times(1)).putOutgoing(flushToDiskRequest);
  }

  @Test
  public void sendShouldWaitForRepliesFromRecipients() throws Exception {
    flushToDiskRequest.send();

    verify(replyProcessor, times(1)).waitForReplies();
  }

  @Test
  public void sendShouldInvokeProcessLocally() throws Exception {
    flushToDiskRequest.send();

    verify(replyProcessor, times(1)).process(any(AdminResponse.class));
  }

  @Test
  public void sendShouldFlushDiskStores() throws Exception {
    flushToDiskRequest.send();

    verify(diskStore1, times(1)).flush();
    verify(diskStore2, times(1)).flush();
  }

  @Test
  public void sendShouldFlushDiskStoresInLocalMemberBeforeWaitingForReplies() throws Exception {
    InOrder inOrder = inOrder(diskStore1, diskStore2, replyProcessor);

    flushToDiskRequest.send();

    // assert that prepareForBackup is invoked before invoking waitForReplies
    inOrder.verify(diskStore1, times(1)).flush();
    inOrder.verify(diskStore2, times(1)).flush();
    inOrder.verify(replyProcessor, times(1)).waitForReplies();
  }

  @Test
  public void repliesWithFinishBackupResponse() throws Exception {
    flushToDiskRequest.send();

    verify(replyProcessor, times(1)).process(any(FlushToDiskResponse.class));
  }

  @Test
  public void sendShouldCompleteIfWaitForRepliesThrowsReplyExceptionCausedByCacheClosedException()
      throws Exception {
    doThrow(new ReplyException(new CacheClosedException())).when(replyProcessor).waitForReplies();

    flushToDiskRequest.send();
  }

  @Test
  public void sendShouldThrowIfWaitForRepliesThrowsReplyExceptionNotCausedByCancelException()
      throws Exception {
    doThrow(new ReplyException(new NullPointerException())).when(replyProcessor).waitForReplies();

    assertThatThrownBy(() -> flushToDiskRequest.send()).isInstanceOf(ReplyException.class)
        .hasCauseInstanceOf(NullPointerException.class);
  }

  @Test
  public void sendCompletesWhenWaitForRepliesThrowsInterruptedException() throws Exception {
    doThrow(new InterruptedException()).when(replyProcessor).waitForReplies();

    flushToDiskRequest.send();
  }

}
