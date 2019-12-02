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

package org.apache.geode.distributed.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class StartupMessageJUnitTest {

  @Test
  public void processShouldSendAReplyMessageWhenExceptionThrown() {
    ClusterDistributionManager distributionManager = mock(ClusterDistributionManager.class);

    InternalDistributedMember id = mock(InternalDistributedMember.class);
    when(distributionManager.getId()).thenReturn(id);

    // This method will throw an exception in the middle of StartupMessage.process
    when(distributionManager.getTransport()).thenThrow(new IllegalStateException("FAIL"));

    StartupMessage startupMessage = new StartupMessage();
    startupMessage.setSender(id);
    startupMessage.process(distributionManager);

    // We expect process to send a ReplyMessage with an exception
    // So that the sender is not blocked
    ArgumentCaptor<DistributionMessage> responseCaptor =
        ArgumentCaptor.forClass(DistributionMessage.class);
    verify(distributionManager).putOutgoing(responseCaptor.capture());

    DistributionMessage response = responseCaptor.getValue();
    assertThat(response).isInstanceOf(ReplyMessage.class);
    assertThat(((ReplyMessage) response).getException())
        .isInstanceOf(ReplyException.class)
        .hasCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  public void processShouldSendAReplyMessageWhenErrorThrownForNoResponse() {
    ClusterDistributionManager distributionManager = mock(ClusterDistributionManager.class);

    InternalDistributedMember id = mock(InternalDistributedMember.class);
    when(distributionManager.getId()).thenReturn(id);

    // This method will throw an error in the middle of StartupMessage.process
    when(distributionManager.getTransport()).thenThrow(new Error("No Response sent"));

    StartupMessage startupMessage = new StartupMessage();
    startupMessage.setSender(id);
    assertThatThrownBy(() -> startupMessage.process(distributionManager))
        .isInstanceOf(Error.class).hasMessage("No Response sent");

    // We expect process to send a ReplyMessage with an exception
    // So that the sender is not blocked
    ArgumentCaptor<DistributionMessage> responseCaptor =
        ArgumentCaptor.forClass(DistributionMessage.class);
    verify(distributionManager).putOutgoing(responseCaptor.capture());

    DistributionMessage response = responseCaptor.getValue();
    assertThat(response).isInstanceOf(ReplyMessage.class);
    assertThat(((ReplyMessage) response).getException())
        .isInstanceOf(ReplyException.class)
        .hasCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  public void startupMessageGetProcessorTypeIsWaitingPool() {
    ClusterDistributionManager distributionManager = mock(ClusterDistributionManager.class);

    InternalDistributedMember id = mock(InternalDistributedMember.class);
    when(distributionManager.getId()).thenReturn(id);

    StartupMessage startupMessage = new StartupMessage();
    startupMessage.setSender(id);
    startupMessage.process(distributionManager);

    assertThat(
        startupMessage.getProcessorType() == OperationExecutors.WAITING_POOL_EXECUTOR);
  }

  @Test
  public void startupResponseMessageGetProcessorTypeIsWaitingPool() {
    ClusterDistributionManager distributionManager = mock(ClusterDistributionManager.class);

    InternalDistributedMember id = mock(InternalDistributedMember.class);
    when(distributionManager.getId()).thenReturn(id);

    StartupResponseMessage startupResponseMessage = new StartupResponseMessage();
    startupResponseMessage.setSender(id);
    startupResponseMessage.process(distributionManager);

    assertThat(
        startupResponseMessage
            .getProcessorType() == OperationExecutors.WAITING_POOL_EXECUTOR);
  }
}
