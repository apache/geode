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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;

public class QueueSynchronizationProcessorTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final ClusterDistributionManager manager =
      mock(ClusterDistributionManager.class, RETURNS_DEEP_STUBS);
  private final InternalDistributedMember primary = mock(InternalDistributedMember.class);
  private final InternalCache cache = mock(InternalCache.class);
  private QueueSynchronizationProcessor.QueueSynchronizationMessage message;
  private QueueSynchronizationProcessor.QueueSynchronizationReplyMessage replyMessage;
  private final List<EventID> eventIDs = new LinkedList<>();
  private final EventID id1 = mock(EventID.class);
  private final EventID id2 = mock(EventID.class);
  private final int processorId = 11;
  private final List<EventID> dispatched = new LinkedList<>();

  @Before
  public void setup() {
    when((manager.getCache())).thenReturn(cache);
    eventIDs.add(id1);
    eventIDs.add(id2);
  }

  @Test
  public void processMessageSetsReply() {
    QueueSynchronizationProcessor processor =
        spy(new QueueSynchronizationProcessor(manager, primary));
    replyMessage = mock(QueueSynchronizationProcessor.QueueSynchronizationReplyMessage.class);

    processor.process(replyMessage);

    assertThat(processor.reply).isEqualTo(replyMessage);
  }

  @Test
  public void processQueueSynchronizationMessageSendsReply() {
    message = spy(new QueueSynchronizationProcessor.QueueSynchronizationMessage());
    setupMessage();

    message.process(manager);

    verify(replyMessage).setEventIds(dispatched);
    verify(replyMessage).setSuccess();
    verifyReplyMessageSent();
  }

  private void setupMessage() {
    message.setEventIdList(eventIDs);
    message.setProcessorId(processorId);
    replyMessage = mock(QueueSynchronizationProcessor.QueueSynchronizationReplyMessage.class);
    dispatched.add(id2);
    doReturn(replyMessage).when(message).createQueueSynchronizationReplyMessage();
    doReturn(dispatched).when(message).getDispatchedEvents(cache);
    doReturn(primary).when(message).getSender();
  }

  private void verifyReplyMessageSent() {
    verify(replyMessage).setProcessorId(processorId);
    verify(replyMessage).setRecipient(primary);
    verify(manager).putOutgoing(replyMessage);
  }

  @Test
  public void processQueueSynchronizationMessageCanSendReplyWithException() {
    expectedException.expect(RuntimeException.class);
    message = spy(new QueueSynchronizationProcessor.QueueSynchronizationMessage());
    setupMessage();
    RuntimeException runtimeException = new RuntimeException();
    doThrow(runtimeException).when(message).getDispatchedEvents(cache);

    message.process(manager);

    verify(replyMessage).setException(any());
    verifyReplyMessageSent();
  }

  @Test
  public void processQueueSynchronizationMessageSendsFailedMessageIfDispatchedEventsAreNull() {
    message = spy(new QueueSynchronizationProcessor.QueueSynchronizationMessage());
    setupMessage();
    doReturn(null).when(message).getDispatchedEvents(cache);

    message.process(manager);

    verify(replyMessage, never()).setSuccess();
    verifyReplyMessageSent();
  }

  @Test
  public void getDispatchedEventsReturnsNullIfQueueIsNull() {
    message = spy(new QueueSynchronizationProcessor.QueueSynchronizationMessage());
    String regionName = "queueName";
    message.setRegionName(regionName);
    when(cache.getRegion(regionName)).thenReturn(null);

    assertThat(message.getDispatchedEvents(cache)).isNull();
  }

  @Test
  public void getDispatchedEventsReturns() {
    message = spy(new QueueSynchronizationProcessor.QueueSynchronizationMessage());
    String regionName = "queueName";
    message.setRegionName(regionName);
    message.setEventIdList(eventIDs);
    HARegion region = mock(HARegion.class);
    HARegionQueue queue = mock(HARegionQueue.class);
    when(cache.getRegion(regionName)).thenReturn(uncheckedCast(region));
    when(region.getOwner()).thenReturn(queue);
    when(queue.getDispatchedEvents(eventIDs)).thenReturn(dispatched);

    assertThat(message.getDispatchedEvents(cache)).isEqualTo(dispatched);
  }
}
