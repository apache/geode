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
package org.apache.geode.internal.cache;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class TXCommitMessageTest {

  @Test
  public void commitProcessQueryMessageIsSentIfHostDeparted() {
    DistributionManager manager = mock(DistributionManager.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    DistributionManager dm = mock(DistributionManager.class);
    TXCommitMessage.CommitProcessQueryReplyProcessor processor = mock(
        TXCommitMessage.CommitProcessQueryReplyProcessor.class);
    TXCommitMessage.CommitProcessQueryMessage queryMessage =
        mock(TXCommitMessage.CommitProcessQueryMessage.class);
    HashSet farSiders = mock(HashSet.class);
    TXCommitMessage message = spy(new TXCommitMessage());
    doReturn(dm).when(message).getDistributionManager();
    when(dm.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    doReturn(member).when(message).getSender();
    doReturn(false).when(message).isProcessing();
    doReturn(processor).when(message).createReplyProcessor();
    doReturn(farSiders).when(message).getFarSiders();
    doReturn(queryMessage).when(message).createQueryMessage(processor);
    when(farSiders.isEmpty()).thenReturn(false);

    message.memberDeparted(manager, member, false);

    verify(dm, timeout(60000)).putOutgoing(queryMessage);
    verify(processor, timeout(60000)).waitForRepliesUninterruptibly();
  }

  @Test
  public void firePendingCallbacksSetsChangeAppliedToCacheInEventLocalFilterInfo() {
    TXCommitMessage message = spy(new TXCommitMessage());
    FilterRoutingInfo.FilterInfo filterInfo1 = mock(FilterRoutingInfo.FilterInfo.class);
    FilterRoutingInfo.FilterInfo filterInfo2 = mock(FilterRoutingInfo.FilterInfo.class);
    EntryEventImpl event1 = mock(EntryEventImpl.class, RETURNS_DEEP_STUBS);
    EntryEventImpl event2 = mock(EntryEventImpl.class, RETURNS_DEEP_STUBS);
    List<EntryEventImpl> callbacks = new ArrayList<>();
    callbacks.add(event1);
    callbacks.add(event2);
    doReturn(event2).when(message).getLastTransactionEvent(callbacks);
    when(event1.getLocalFilterInfo()).thenReturn(filterInfo1);
    when(event2.getLocalFilterInfo()).thenReturn(filterInfo2);

    message.firePendingCallbacks(callbacks);

    verify(filterInfo1).setChangeAppliedToCache(true);
    verify(filterInfo2).setChangeAppliedToCache(true);
  }

  @Test
  public void firePendingCallbacksSendsAFTER_CREATECallbackIfUpdateEntryEventHasNullNewValue() {
    TXCommitMessage message = spy(new TXCommitMessage());
    LocalRegion region = mock(LocalRegion.class, RETURNS_DEEP_STUBS);
    EntryEventImpl updateEvent = mock(EntryEventImpl.class, RETURNS_DEEP_STUBS);
    EntryEventImpl lastTxEvent = mock(EntryEventImpl.class, RETURNS_DEEP_STUBS);

    List<EntryEventImpl> callbacks = new ArrayList<>();
    callbacks.add(updateEvent);
    callbacks.add(lastTxEvent);

    when(updateEvent.getLocalFilterInfo()).thenReturn(null);
    when(updateEvent.getNewValue()).thenReturn(null);
    when(updateEvent.getRegion()).thenReturn(region);

    doReturn(lastTxEvent).when(message).getLastTransactionEvent(callbacks);

    message.firePendingCallbacks(callbacks);

    verify(region, only()).invokeTXCallbacks(EnumListenerEvent.AFTER_CREATE, updateEvent, true,
        false);
  }
}
