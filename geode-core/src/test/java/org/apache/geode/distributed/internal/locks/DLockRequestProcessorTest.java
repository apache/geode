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

package org.apache.geode.distributed.internal.locks;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class DLockRequestProcessorTest {
  private DistributionManager distributionManager;
  private InternalDistributedMember member;
  private DLockGrantor grantor;
  private DLockRequestProcessor.DLockRequestMessage message;

  @Before
  public void setup() {
    distributionManager = mock(DistributionManager.class, RETURNS_DEEP_STUBS);
    member = mock(InternalDistributedMember.class);
    grantor = mock(DLockGrantor.class);
    when(distributionManager.getDistributionManagerId()).thenReturn(member);

    message = spy(new DLockRequestProcessor.DLockRequestMessage());
    message.receivingDM = distributionManager;
    message.response = mock(DLockRequestProcessor.DLockResponseMessage.class);
    message.grantor = grantor;
    message.objectName = new Object();
    message.lockId = 1;
  }

  @Test
  public void releasesLockIfReplyProcessorIsGoneWhenRequestedLocally() throws Exception {
    doReturn(member).when(message).getSender();

    message.sendResponse();

    verify(message, never()).executeGrantToRemote(message.response);
    verify(message).endGrantWaitStatistic();
    verify(grantor).releaseIfLocked(message.objectName, member, message.lockId);
  }

  @Test
  public void sendResponseProcessesLocallyIfRequestedLocally() {
    doReturn(member).when(message).getSender();
    ReplyProcessor21 processor = mock(ReplyProcessor21.class);
    doReturn(processor).when(message).getReplyProcessor();

    message.sendResponse();

    verify(processor).process(message.response);
    verify(message, never()).executeGrantToRemote(message.response);
    verify(message).endGrantWaitStatistic();
  }

  @Test
  public void sendResponseInvokesExecuteGrantToRemoteIfRequestedRemotely() {
    doReturn(mock(InternalDistributedMember.class)).when(message).getSender();

    message.sendResponse();

    verify(message).executeGrantToRemote(message.response);
  }

  @Test
  public void executeGrantToRemoteUsingExecutorsIfNotExecutedByWaitingPoolThread() {
    message.executeGrantToRemote(message.response);

    verify(distributionManager).getExecutors();
  }

  @Test
  public void grantToRemoteSendsOutgoingMessage() {
    message.grantToRemote(message.response);

    verify(distributionManager).putOutgoing(message.response);
    verify(message).endGrantWaitStatistic();
  }
}
