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

import static org.apache.geode.distributed.internal.DistributionImpl.EMPTY_MEMBER_ARRAY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jgroups.util.UUID;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.SystemConnectException;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.direct.DirectChannel;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MembershipClosedException;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.gms.GMSMembership;
import org.apache.geode.internal.admin.remote.AlertListenerMessage;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.tcp.ConnectExceptions;

public class DistributionTest {


  private DirectChannel dc;
  private InternalDistributedMember[] mockMembers;
  private DistributionImpl distribution;
  private ClusterDistributionManager clusterDistributionManager;
  private RemoteTransportConfig remoteTransportConfig;
  private InternalDistributedSystem internalDistributedSystem;
  private Membership membership;
  private Properties distProperties;

  /**
   * Some tests require a DirectChannel mock
   */
  @Before
  public void setUpDirectChannelMock() throws Exception {

    internalDistributedSystem = mock(InternalDistributedSystem.class);
    clusterDistributionManager = mock(ClusterDistributionManager.class);
    remoteTransportConfig = mock(RemoteTransportConfig.class);
    DistributionConfig distributionConfig = mock(DistributionConfig.class);
    when(distributionConfig.getAckWaitThreshold()).thenReturn(1);
    when(distributionConfig.getAckSevereAlertThreshold()).thenReturn(10);
    when(internalDistributedSystem.getConfig()).thenReturn(distributionConfig);

    membership = mock(Membership.class);
    distribution = new DistributionImpl(clusterDistributionManager,
        remoteTransportConfig, internalDistributedSystem, membership);

    Random r = new Random();
    mockMembers = new InternalDistributedMember[5];
    for (int i = 0; i < mockMembers.length; i++) {
      mockMembers[i] = new InternalDistributedMember("localhost", 8888 + i);
      UUID uuid = new UUID(r.nextLong(), r.nextLong());
      mockMembers[i].setUUID(uuid);
    }

    dc = mock(DirectChannel.class);

    distribution.setDirectChannel(dc);
    when(dc.send(any(GMSMembership.class), any(mockMembers.getClass()),
        any(DistributionMessage.class), anyInt(), anyInt())).thenReturn(100);

  }

  @Test
  public void testDirectChannelSend() throws Exception {
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    List<InternalDistributedMember> recipients = Arrays.asList(mockMembers[2], mockMembers[3]);
    m.setRecipients(recipients);
    Set<InternalDistributedMember> failures = distribution
        .directChannelSend(recipients, m);
    assertTrue(failures == null);
    verify(dc).send(any(), any(),
        any(), anyLong(), anyLong());
  }

  @Test
  public void testDirectChannelSendFailureToOneRecipient() throws Exception {
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    List<InternalDistributedMember> recipients = Arrays.asList(mockMembers[2], mockMembers[3]);
    m.setRecipients(recipients);
    Set<InternalDistributedMember> failures = distribution
        .directChannelSend(recipients, m);
    ConnectExceptions exception = new ConnectExceptions();
    exception.addFailure(recipients.get(0), new Exception("testing"));
    when(dc.send(any(), any(mockMembers.getClass()),
        any(DistributionMessage.class), anyLong(), anyLong())).thenThrow(exception);
    failures = distribution.directChannelSend(recipients, m);
    assertTrue(failures != null);
    assertEquals(1, failures.size());
    assertEquals(recipients.get(0), failures.iterator().next());
  }

  @Test
  public void testDirectChannelSendFailureToAll() throws Exception {
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    List<InternalDistributedMember> recipients = Arrays.asList(mockMembers[2], mockMembers[3]);
    m.setRecipients(recipients);
    Set<InternalDistributedMember> failures = distribution
        .directChannelSend(recipients, m);
    when(dc.send(any(), any(mockMembers.getClass()),
        any(DistributionMessage.class), anyInt(), anyInt())).thenReturn(0);
    doThrow(MembershipClosedException.class).when(membership).checkCancelled();

    try {
      distribution.directChannelSend(recipients, m);
      fail("expected directChannelSend to throw an exception");
    } catch (DistributedSystemDisconnectedException expected) {
    }
  }

  @Test
  public void testDirectChannelSendAllRecipients() throws Exception {
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    when(membership.getAllMembers(EMPTY_MEMBER_ARRAY)).thenReturn(mockMembers);
    m.setRecipient(DistributionMessage.ALL_RECIPIENTS);
    assertTrue(m.forAll());
    Set<InternalDistributedMember> failures = distribution
        .directChannelSend(null, m);
    assertTrue(failures == null);
    verify(dc).send(any(), isA(mockMembers.getClass()),
        isA(DistributionMessage.class), anyLong(), anyLong());
  }

  @Test
  public void testDirectChannelSendFailureDueToForcedDisconnect() throws Exception {
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    when(membership.shutdownInProgress()).thenReturn(true);
    List<InternalDistributedMember> recipients = Arrays.asList(mockMembers[2], mockMembers[3]);
    m.setRecipients(recipients);
    Set<InternalDistributedMember> failures = distribution
        .directChannelSend(recipients, m);
    distribution.setShutdown();
    ConnectExceptions exception = new ConnectExceptions();
    exception.addFailure(recipients.get(0), new Exception("testing"));
    when(dc.send(any(), any(mockMembers.getClass()),
        any(DistributionMessage.class), anyLong(), anyLong())).thenThrow(exception);
    Assertions.assertThatThrownBy(() -> {
      distribution.directChannelSend(recipients, m);
    }).isInstanceOf(DistributedSystemDisconnectedException.class);
  }

  @Test
  public void testSendAdminMessageFailsDuringShutdown() throws Exception {
    AlertListenerMessage m = AlertListenerMessage.create(mockMembers[0], 1,
        Instant.now(), "thread", "", 1L, "", "");
    when(membership.shutdownInProgress()).thenReturn(true);
    Set<InternalDistributedMember> failures =
        distribution.send(Collections.singletonList(mockMembers[0]), m);
    verify(membership, never()).send(any(), any());
    assertEquals(1, failures.size());
    assertEquals(mockMembers[0], failures.iterator().next());
  }

  @Test
  public void testSendToNullListIsRejected() throws Exception {
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    m.setRecipient(mockMembers[0]);
    distribution.send(null, m);
    verify(membership, never()).send(any(), any());
  }

  @Test
  public void testSendToEmptyListIsRejected() throws Exception {
    List<InternalDistributedMember> emptyList = Collections.emptyList();
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    m.setRecipient(mockMembers[0]);
    distribution.send(emptyList, m);
    verify(membership, never()).send(any(), any());
  }

  @Test
  public void testExceptionNestedOnStartConfigError() throws Exception {
    Throwable exception = new MembershipConfigurationException("Test exception");
    doThrow(exception).when(membership).start();

    assertThatThrownBy(() -> distribution.start())
        .isInstanceOf(GemFireConfigException.class)
        .hasCause(exception);
  }

  @Test
  public void testExceptionNestedOnStartStartupError() throws Exception {
    Throwable exception = new MemberStartupException("Test exception");
    doThrow(exception).when(membership).start();

    assertThatThrownBy(() -> distribution.start())
        .isInstanceOf(SystemConnectException.class)
        .hasCause(exception);
  }
}
