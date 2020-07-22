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
package org.apache.geode.distributed.internal.membership.gms.messenger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.util.Digest;
import org.jgroups.util.UUID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactoryImpl;
import org.apache.geode.distributed.internal.membership.api.MembershipClosedException;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.Message;
import org.apache.geode.distributed.internal.membership.gms.DefaultMembershipStatistics;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.MemberIdentifierImpl;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.Services.Stopper;
import org.apache.geode.distributed.internal.membership.gms.TestMessage;
import org.apache.geode.distributed.internal.membership.gms.interfaces.HealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.distributed.internal.membership.gms.interfaces.MessageHandler;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.distributed.internal.membership.gms.messages.HeartbeatMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.InstallViewMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinResponseMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messenger.JGroupsMessenger.JGroupsReceiver;
import org.apache.geode.distributed.internal.membership.gms.util.MemberIdentifierUtil;
import org.apache.geode.internal.serialization.BufferDataOutputStream;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.serialization.internal.DSFIDSerializerImpl;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class JGroupsMessengerJUnitTest {

  private static final String AES_128 = "AES:128";

  private Services services;
  private JGroupsMessenger messenger;
  private JoinLeave joinLeave;
  private Manager manager;
  private Stopper stopper;
  private HealthMonitor healthMonitor;
  private InterceptUDP interceptor;
  private MembershipConfig membershipConfig;

  private void initMocks(boolean enableMcast) throws Exception {
    initMocks(enableMcast, false);
  }

  /**
   * Create stub and mock objects
   */
  private void initMocks(boolean enableMcast, boolean secure) throws Exception {
    if (messenger != null) {
      messenger.stop();
      messenger = null;
    }

    stopper = mock(Stopper.class);
    when(stopper.isCancelInProgress()).thenReturn(false);

    manager = mock(Manager.class);
    when(manager.isMulticastAllowed()).thenReturn(enableMcast);

    healthMonitor = mock(HealthMonitor.class);

    joinLeave = mock(JoinLeave.class);


    membershipConfig = new TestMembershipConfig() {
      @Override
      public String getLocators() {
        return "localhost[10344]";
      }

      @Override
      public long getAckWaitThreshold() {
        return 1;
      }

      @Override
      public int getMcastTtl() {
        return 0;
      }

      @Override
      public boolean getDisableTcp() {
        return true;
      }

      @Override
      public int getMcastPort() {
        if (enableMcast) {
          return 1234;
        } else {
          return 0;
        }
      }

      @Override
      public String getSecurityUDPDHAlgo() {
        if (secure) {
          return AES_128;
        } else {
          return "";
        }
      }
    };

    services = mock(Services.class);
    when(services.getConfig()).thenReturn(membershipConfig);
    when(services.getCancelCriterion()).thenReturn(stopper);
    when(services.getHealthMonitor()).thenReturn(healthMonitor);
    when(services.getManager()).thenReturn(manager);
    when(services.getJoinLeave()).thenReturn(joinLeave);
    DSFIDSerializer serializer = new DSFIDSerializerImpl();
    Services.registerSerializables(serializer);
    when(services.getSerializer()).thenReturn(serializer);
    Version current = Version.CURRENT; // force Version static initialization to set
                                       // Version

    when(services.getStatistics()).thenReturn(new DefaultMembershipStatistics());

    messenger = new JGroupsMessenger<>();
    messenger.init(services);

    // if I do this earlier then test this return messenger as null
    when(services.getMessenger()).thenReturn(messenger);
    when(services.getMemberFactory())
        .thenReturn(new MemberIdentifierFactoryImpl());

    String jgroupsConfig = messenger.jgStackConfig;
    int startIdx = jgroupsConfig.indexOf("<org");
    int insertIdx = jgroupsConfig.indexOf('>', startIdx + 4) + 1;
    jgroupsConfig = jgroupsConfig.substring(0, insertIdx) + "<" + InterceptUDP.class.getName()
        + "/>" + jgroupsConfig.substring(insertIdx);
    messenger.jgStackConfig = jgroupsConfig;
    // System.out.println("jgroups config: " + jgroupsConfig);

    messenger.start();
    messenger.started();

    interceptor =
        (InterceptUDP) messenger.myChannel.getProtocolStack().getTransport().getUpProtocol();

  }

  @After
  public void stopMessenger() {
    if (messenger != null && messenger.myChannel != null) {
      messenger.stop();
    }
  }

  @Test
  public void ioExceptionInitiatesSuspectProcessing() throws Exception {
    // see GEODE-634
    initMocks(false);
    GMSMembershipView<MemberIdentifier> v = createView();
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);
    messenger.handleJGroupsIOException(new IOException("je m'en fiche"),
        new JGAddress(v.getMembers().get(1)));
    verify(healthMonitor).checkIfAvailable(isA(MemberIdentifier.class), isA(String.class),
        isA(Boolean.class));
  }

  @Test
  public void ioExceptionDuringShutdownAvoidsSuspectProcessing() throws Exception {
    // see GEODE-634
    initMocks(false);
    GMSMembershipView<MemberIdentifier> v = createView();
    when(joinLeave.getView()).thenReturn(v);
    when(manager.shutdownInProgress()).thenReturn(true);
    messenger.installView(v);
    messenger.handleJGroupsIOException(new IOException("fichez-moi le camp"),
        new JGAddress(v.getMembers().get(1)));
    verify(healthMonitor, never()).checkIfAvailable(isA(MemberIdentifier.class),
        isA(String.class), isA(Boolean.class));
  }

  private GMSMembershipView createView() {
    MemberIdentifier sender = messenger.getMemberID();
    List<MemberIdentifier> mbrs = new ArrayList<>();
    mbrs.add(sender);
    mbrs.add(createAddress(100));
    mbrs.add(createAddress(101));
    GMSMembershipView v = new GMSMembershipView(sender, 1, mbrs);
    return v;
  }

  @Test
  public void normalMessagesUseFlowControl() throws Exception {
    initMocks(false);
    org.jgroups.Message jgmsg = new org.jgroups.Message();
    Message dmsg = mock(Message.class);
    when(dmsg.isHighPriority()).thenReturn(false);
    messenger.setMessageFlags(dmsg, jgmsg);
    assertFalse("expected flow-control to be used: " + jgmsg,
        jgmsg.isFlagSet(org.jgroups.Message.Flag.NO_FC));
  }

  @Test
  public void highPriorityMessagesBypassFlowControl() throws Exception {
    initMocks(false);
    org.jgroups.Message jgmsg = new org.jgroups.Message();
    Message dmsg = mock(Message.class);
    when(dmsg.isHighPriority()).thenReturn(true);
    messenger.setMessageFlags(dmsg, jgmsg);
    assertTrue("expected flow-control to not be used: " + jgmsg,
        jgmsg.isFlagSet(org.jgroups.Message.Flag.NO_FC));
  }

  @Test
  public void testMemberWeightIsSerialized() throws Exception {
    initMocks(false);
    BufferDataOutputStream out =
        new BufferDataOutputStream(500, Version.getCurrentVersion());
    MemberIdentifier mbr = createAddress(8888);
    mbr.setMemberWeight((byte) 40);
    mbr.toData(out, mock(SerializationContext.class));
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    mbr = new MemberIdentifierImpl();
    mbr.fromData(in, mock(DeserializationContext.class));
    assertEquals(40, mbr.getMemberWeight());
  }

  @Test
  public void testSerializationError() throws Exception {
    for (int i = 0; i < 2; i++) {
      boolean enableMcast = (i == 1);
      initMocks(enableMcast);
      MemberIdentifier mbr = createAddress(8888);
      HeartbeatMessage msg =
          mock(HeartbeatMessage.class);
      when(msg.getRecipients()).thenReturn(Collections.singletonList(mbr));
      when(msg.getMulticast()).thenReturn(enableMcast);
      when(msg.getDSFID()).thenReturn((int) DataSerializableFixedID.HEARTBEAT_RESPONSE);

      // for code coverage we need to test with both a SerializationException and
      // an IOException. The former is wrapped in a MembershipIOException while the
      // latter is not
      Mockito.doThrow(new IOException("test exception")).when(msg).toData(any(DataOutput.class),
          any(SerializationContext.class));
      Set<?> failures = messenger.send(msg);
      assertThat(failures).isNotNull();
      assertThat(failures).isNotEmpty();
      if (enableMcast) {
        verify(msg, atLeastOnce()).registerProcessor();
      }
      doThrow(new IOException()).when(msg).toData(any(DataOutput.class),
          any(SerializationContext.class));
      failures = messenger.send(msg);
      assertThat(failures).isNotNull();
      assertThat(failures).isNotEmpty();
    }
  }

  @Test
  public void testJChannelError() throws Exception {
    for (int i = 0; i < 2; i++) {
      boolean enableMcast = (i == 1);
      initMocks(enableMcast);
      JChannel mockChannel = mock(JChannel.class);
      when(mockChannel.isConnected()).thenReturn(true);
      doThrow(new RuntimeException()).when(mockChannel).send(any(org.jgroups.Message.class));
      JChannel realChannel = messenger.myChannel;
      messenger.myChannel = mockChannel;
      try {
        MemberIdentifier mbr = createAddress(8888);
        HeartbeatMessage msg = mock(HeartbeatMessage.class);
        when(msg.getRecipients()).thenReturn(Collections.singletonList(mbr));
        when(msg.getMulticast()).thenReturn(enableMcast);
        when(msg.getDSFID()).thenReturn((int) DataSerializableFixedID.HEARTBEAT_RESPONSE);
        try {
          messenger.send(msg);
          fail("expected a failure");
        } catch (MembershipClosedException e) {
          // success
        }
        verify(mockChannel).send(isA(org.jgroups.Message.class));
      } finally {
        messenger.myChannel = realChannel;
      }
    }
  }

  @Test
  public void testJChannelErrorDuringDisconnect() throws Exception {
    for (int i = 0; i < 4; i++) {
      System.out.println("loop #" + i);
      boolean enableMcast = (i % 2 == 1);
      initMocks(enableMcast);
      JChannel mockChannel = mock(JChannel.class);
      when(mockChannel.isConnected()).thenReturn(true);
      Exception ex, shutdownCause;
      if (i < 2) {
        ex = new RuntimeException("");
        shutdownCause = new RuntimeException("shutdownCause");
      } else {
        shutdownCause = new MemberDisconnectedException("");
        ex = new RuntimeException("", shutdownCause);
      }
      doThrow(ex).when(mockChannel).send(any(org.jgroups.Message.class));
      JChannel realChannel = messenger.myChannel;
      messenger.myChannel = mockChannel;

      when(services.getShutdownCause()).thenReturn(shutdownCause);

      try {
        MemberIdentifier mbr = createAddress(8888);
        HeartbeatMessage msg = mock(HeartbeatMessage.class);
        when(msg.getRecipients()).thenReturn(Collections.singletonList(mbr));
        when(msg.getMulticast()).thenReturn(enableMcast);
        when(msg.getDSFID()).thenReturn((int) DataSerializableFixedID.HEARTBEAT_RESPONSE);
        try {
          messenger.send(msg);
          fail("expected a failure");
        } catch (MembershipClosedException e) {
          // the ultimate cause should be the shutdownCause returned
          // by Services.getShutdownCause()
          Throwable cause = e;
          while (cause.getCause() != null) {
            cause = cause.getCause();
          }
          assertTrue(cause != e);
          assertTrue(cause == shutdownCause);
        }
        verify(mockChannel).send(isA(org.jgroups.Message.class));
      } finally {
        messenger.myChannel = realChannel;
      }
    }
  }

  @Test
  public void testSendWhenChannelIsClosed() throws Exception {
    for (int i = 0; i < 2; i++) {
      initMocks(false);
      JChannel mockChannel = mock(JChannel.class);
      when(mockChannel.isConnected()).thenReturn(false);
      doThrow(new RuntimeException()).when(mockChannel).send(any(org.jgroups.Message.class));
      JChannel realChannel = messenger.myChannel;
      messenger.myChannel = mockChannel;
      try {
        MemberIdentifier mbr = createAddress(8888);
        HeartbeatMessage msg =
            mock(HeartbeatMessage.class);
        when(msg.getRecipients()).thenReturn(Collections.singletonList(mbr));
        when(msg.getMulticast()).thenReturn(false);
        try {
          messenger.send(msg);
          fail("expected a failure");
        } catch (MembershipClosedException e) {
          // success
        }
        verify(mockChannel, never()).send(isA(org.jgroups.Message.class));
      } finally {
        messenger.myChannel = realChannel;
      }
    }
  }

  @Test
  public void testSendUnreliably() throws Exception {
    for (int i = 0; i < 2; i++) {
      boolean enableMcast = (i == 1);
      initMocks(enableMcast);
      MemberIdentifier mbr = createAddress(8888);
      HeartbeatMessage msg =
          mock(HeartbeatMessage.class);
      when(msg.getRecipients()).thenReturn(Collections.singletonList(mbr));
      when(msg.getMulticast()).thenReturn(enableMcast);
      when(msg.getDSFID()).thenReturn((int) DataSerializableFixedID.HEARTBEAT_RESPONSE);
      interceptor.collectMessages = true;
      Set<?> failures = messenger.sendUnreliably(msg);
      assertTrue(failures == null || failures.isEmpty());
      if (enableMcast) {
        verify(msg, atLeastOnce()).registerProcessor();
      }
      verify(msg).toData(isA(DataOutput.class), any(SerializationContext.class));
      assertTrue("expected 1 message but found " + interceptor.collectedMessages,
          interceptor.collectedMessages.size() == 1);
      assertTrue(
          interceptor.collectedMessages.get(0).isFlagSet(org.jgroups.Message.Flag.NO_RELIABILITY));
    }
  }

  @Test
  public void testMessageDeliveredToHandler() throws Exception {
    doTestMessageDeliveredToHandler(false);
  }

  @Test
  public void testMessageDeliveredToHandlerMcast() throws Exception {
    doTestMessageDeliveredToHandler(true);
  }

  private void doTestMessageDeliveredToHandler(boolean mcast) throws Exception {
    initMocks(mcast);
    MessageHandler mh = mock(MessageHandler.class);
    messenger.addHandler(JoinRequestMessage.class, mh);

    MemberIdentifier addr = messenger.getMemberID();
    GMSMembershipView v = new GMSMembershipView(addr);
    when(joinLeave.getView()).thenReturn(v);


    MemberIdentifier sender = createAddress(8888);

    JoinRequestMessage msg = new JoinRequestMessage(messenger.localAddress, sender, null, -1, 0);

    org.jgroups.Message jmsg =
        messenger.createJGMessage(msg, messenger.jgAddress, messenger.localAddress,
            Version.getCurrentVersion().ordinal());
    interceptor.up(new Event(Event.MSG, jmsg));

    verify(mh, times(1)).processMessage(any(JoinRequestMessage.class));

    LeaveRequestMessage lmsg = new LeaveRequestMessage(messenger.localAddress, sender, "testing");
    when(joinLeave.getMemberID(any())).thenReturn(sender);
    jmsg = messenger.createJGMessage(lmsg, messenger.jgAddress, messenger.localAddress,
        Version.getCurrentVersion().ordinal());
    interceptor.up(new Event(Event.MSG, jmsg));

    verify(manager).processMessage(any(LeaveRequestMessage.class));

  }


  @Test
  public void testBigMessageIsFragmentedWhenSentPointToPoint() throws Exception {
    doTestBigMessageIsFragmented(false, false);
  }

  @Test
  public void testBigMessageIsFragmentedWhenMulticast() throws Exception {
    doTestBigMessageIsFragmented(true, true);
  }

  @Test
  public void testBigMessageIsFragmentedWhenBroadcast() throws Exception {
    doTestBigMessageIsFragmented(false, true);
  }

  public void doTestBigMessageIsFragmented(boolean mcastEnabled, boolean broadcastMessage)
      throws Exception {
    initMocks(mcastEnabled);
    MessageHandler mh = mock(MessageHandler.class);
    messenger.addHandler(JoinRequestMessage.class, mh);

    MemberIdentifier sender = messenger.getMemberID();
    GMSMembershipView v = new GMSMembershipView(sender);
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);

    // send a big message and expect fragmentation
    MemberIdentifier recipient = broadcastMessage ? null : messenger.localAddress;
    services.getSerializer().registerDSFID(ByteHolder.DSFID, ByteHolder.class);
    JoinRequestMessage msg = new JoinRequestMessage(recipient, sender,
        new ByteHolder(
            new byte[(int) (services.getConfig().getUdpFragmentSize()
                * 1.5)]),
        -1, 0);

    msg.setMulticast(broadcastMessage);

    // configure an incoming message handler for JoinRequestMessage
    final JoinRequestMessage[] messageReceived = new JoinRequestMessage[1];
    messenger.addHandler(JoinRequestMessage.class,
        (MessageHandler<JoinRequestMessage>) message -> messageReceived[0] = message);

    // configure the outgoing message interceptor
    interceptor.mcastSentDataMessages = 0;
    interceptor.unicastSentDataMessages = 0;
    interceptor.collectMessages = true;
    interceptor.collectedMessages.clear();

    messenger.send(msg);

    boolean jgroupsWillUseMulticast = mcastEnabled && broadcastMessage;
    if (jgroupsWillUseMulticast) {
      assertTrue(
          "expected 2 messages to be broadcast but found " + interceptor.mcastSentDataMessages,
          interceptor.mcastSentDataMessages == 2);
    } else {
      assertTrue("expected 2 messages to be sent but found " + interceptor.unicastSentDataMessages,
          interceptor.unicastSentDataMessages == 2);
    }

    List<org.jgroups.Message> messages = new ArrayList<>(interceptor.collectedMessages);
    UUID fakeMember = new UUID(50, 50);
    short unicastHeaderId = ClassConfigurator.getProtocolId(UNICAST3.class);
    int seqno = 1;
    for (org.jgroups.Message m : messages) {
      if (jgroupsWillUseMulticast) {
        m.setSrc(messenger.localAddress.getUUID());
      } else {
        m.setSrc(fakeMember);
        UNICAST3.Header oldHeader = (UNICAST3.Header) m.getHeader(unicastHeaderId);
        if (oldHeader == null)
          continue;
        UNICAST3.Header newHeader =
            UNICAST3.Header.createDataHeader(seqno, oldHeader.connId(), seqno == 1);
        seqno += 1;
        m.putHeader(unicastHeaderId, newHeader);
      }
      interceptor.up(new Event(Event.MSG, m));
    }
    assertNotNull(messageReceived[0]);
  }

  @Test
  public void testSendToMultipleMembers() throws Exception {
    initMocks(false);
    MemberIdentifier sender = messenger.getMemberID();
    MemberIdentifier other = createAddress(8888);

    GMSMembershipView v = new GMSMembershipView(sender);
    v.add(other);
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);

    List<MemberIdentifier> recipients = v.getMembers();
    HeartbeatMessage msg = new HeartbeatMessage();
    msg.setRecipients(recipients);

    messenger.send(msg);
    int sentMessages = interceptor.unicastSentDataMessages;
    assertTrue("expected 2 message to be sent but found " + sentMessages, sentMessages == 2);
  }

  @Test
  public void testChannelStillConnectedAfterEmergencyCloseAfterForcedDisconnectWithAutoReconnect()
      throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(MemberDisconnectedException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doCallRealMethod().when(services).isShutdownDueToForcedDisconnect();
    doCallRealMethod().when(services).isAutoReconnectEnabled();
    services.setShutdownCause(new MemberDisconnectedException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertTrue(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelStillConnectedAfterStopAfterForcedDisconnectWithAutoReconnect()
      throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(MemberDisconnectedException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doCallRealMethod().when(services).isShutdownDueToForcedDisconnect();
    doCallRealMethod().when(services).isAutoReconnectEnabled();
    services.setShutdownCause(new MemberDisconnectedException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertTrue(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelStillConnectedAfteremergencyWhileReconnectingDS() throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(MemberDisconnectedException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    doReturn(false).when(services).isAutoReconnectEnabled();
    doReturn(true).when(manager).isReconnectingDS();
    services.setShutdownCause(new MemberDisconnectedException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertTrue(messenger.myChannel.isConnected());
  }


  @Test
  public void testChannelStillConnectedAfterStopWhileReconnectingDS() throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(MemberDisconnectedException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    doReturn(false).when(services).isAutoReconnectEnabled();
    doReturn(true).when(manager).isReconnectingDS();
    services.setShutdownCause(new MemberDisconnectedException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertTrue(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelClosedOnEmergencyClose() throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(MemberDisconnectedException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    doReturn(false).when(services).isAutoReconnectEnabled();
    doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new MemberDisconnectedException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertFalse(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelClosedOnStop() throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(MemberDisconnectedException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    doReturn(false).when(services).isAutoReconnectEnabled();
    doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new MemberDisconnectedException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertFalse(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelClosedAfterEmergencyCloseForcedDisconnectWithoutAutoReconnect()
      throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(MemberDisconnectedException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(true).when(services).isShutdownDueToForcedDisconnect();
    doReturn(false).when(services).isAutoReconnectEnabled();
    doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new MemberDisconnectedException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertFalse(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelStillConnectedStopAfterForcedDisconnectWithoutAutoReconnect()
      throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(MemberDisconnectedException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(true).when(services).isShutdownDueToForcedDisconnect();
    doReturn(false).when(services).isAutoReconnectEnabled();
    doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new MemberDisconnectedException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertFalse(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelClosedAfterEmergencyCloseNotForcedDisconnectWithAutoReconnect()
      throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(MemberDisconnectedException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    doReturn(true).when(services).isAutoReconnectEnabled();
    doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new MemberDisconnectedException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertFalse(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelStillConnectedStopNotForcedDisconnectWithAutoReconnect() throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(MemberDisconnectedException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    doReturn(true).when(services).isAutoReconnectEnabled();
    doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new MemberDisconnectedException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertFalse(messenger.myChannel.isConnected());
  }

  @Test
  public void testMessageFiltering() throws Exception {
    initMocks(true);
    MemberIdentifier mbr = createAddress(8888);
    GMSMembershipView view = new GMSMembershipView(mbr);

    // the digest should be set in an outgoing join response
    JoinResponseMessage joinResponse = new JoinResponseMessage(mbr, view, 0);
    messenger.filterOutgoingMessage(joinResponse);
    assertNotNull(joinResponse.getMessengerData());

    // save the view digest for later
    byte[] data = joinResponse.getMessengerData();

    // the digest should be used and the message bytes nulled out in an incoming join response
    messenger.filterIncomingMessage(joinResponse);
    assertNull(joinResponse.getMessengerData());

    // the digest shouldn't be set in an outgoing rejection message
    joinResponse =
        new JoinResponseMessage("you can't join my distributed system.  nyah nyah nyah!", 0);
    messenger.filterOutgoingMessage(joinResponse);
    assertNull(joinResponse.getMessengerData());

    // the digest shouldn't be installed from an incoming rejection message
    joinResponse.setMessengerData(data);
    messenger.filterIncomingMessage(joinResponse);
    assertNotNull(joinResponse.getMessengerData());
  }

  @Test
  public void testPingPong() throws Exception {
    initMocks(false);
    GMSPingPonger pinger = messenger.pingPonger;
    MemberIdentifier mbr = createAddress(8888);
    JGAddress addr = new JGAddress(mbr);

    org.jgroups.Message pingMessage = pinger.createPingMessage(null, addr);
    assertTrue(pinger.isPingMessage(pingMessage.getBuffer()));
    assertFalse(pinger.isPongMessage(pingMessage.getBuffer()));

    org.jgroups.Message pongMessage = pinger.createPongMessage(null, addr);
    assertTrue(pinger.isPongMessage(pongMessage.getBuffer()));
    assertFalse(pinger.isPingMessage(pongMessage.getBuffer()));

    interceptor.collectMessages = true;
    pinger.sendPingMessage(messenger.myChannel, null, addr);
    Assert.assertEquals("expected 1 message but found " + interceptor.collectedMessages,
        interceptor.collectedMessages.size(), 1);
    pingMessage = interceptor.collectedMessages.get(0);
    assertTrue(pinger.isPingMessage(pingMessage.getBuffer()));

    interceptor.collectedMessages.clear();
    pinger.sendPongMessage(messenger.myChannel, null, addr);
    Assert.assertEquals("expected 1 message but found " + interceptor.collectedMessages,
        interceptor.collectedMessages.size(), 1);
    pongMessage = interceptor.collectedMessages.get(0);
    assertTrue(pinger.isPongMessage(pongMessage.getBuffer()));

    interceptor.collectedMessages.clear();
    JGroupsReceiver receiver = (JGroupsReceiver) messenger.myChannel.getReceiver();
    long pongsReceived = messenger.pongsReceived.longValue();
    receiver.receive(pongMessage);
    assertEquals(pongsReceived + 1, messenger.pongsReceived.longValue());
    receiver.receive(pingMessage);
    Assert.assertEquals("expected 1 message but found " + interceptor.collectedMessages,
        interceptor.collectedMessages.size(), 1);
    org.jgroups.Message m = interceptor.collectedMessages.get(0);
    assertTrue(pinger.isPongMessage(m.getBuffer()));
  }

  /**
   * messages for the Manager that were queued by a quorum checker shouldn't be delivered to
   * a Manager
   */
  @Test
  public void testIgnoreManagerMessagesFromQuorumChecker() throws Exception {
    initMocks(false);
    MemberIdentifier memberIdentifier = createAddress(8888);
    JGAddress jgAddress = new JGAddress(memberIdentifier);

    ArgumentCaptor<Message> valueCapture = ArgumentCaptor.forClass(Message.class);
    doNothing().when(manager).processMessage(valueCapture.capture());
    org.jgroups.Message jgroupsMessage = messenger.createJGMessage(new TestMessage(), jgAddress,
        memberIdentifier, Version.CURRENT_ORDINAL);
    messenger.jgroupsReceiver.receive(jgroupsMessage, true);
    assertThat(valueCapture.getAllValues()).isEmpty();
  }

  @Test
  public void testJGroupsIOExceptionHandler() throws Exception {
    initMocks(false);
    MemberIdentifier mbr = createAddress(8888);
    GMSMembershipView v = new GMSMembershipView(mbr);
    v.add(messenger.getMemberID());
    messenger.installView(v);

    IOException ioe = new IOException("test exception");
    messenger.handleJGroupsIOException(ioe, new JGAddress(mbr));
    verify(healthMonitor).checkIfAvailable(mbr,
        "Unable to send messages to this member via JGroups", Boolean.TRUE);
  }

  @Test
  public void testReceiver() throws Exception {
    initMocks(false);
    JGroupsReceiver receiver = (JGroupsReceiver) messenger.myChannel.getReceiver();
    messenger.addHandler(HeartbeatMessage.class, message -> {
    });

    // a zero-length message is ignored
    org.jgroups.Message msg = new org.jgroups.Message(new JGAddress(messenger.getMemberID()));
    Object result = messenger.readJGMessage(msg);
    assertNull(result);

    // for code coverage we need to pump this message through the receiver
    receiver.receive(msg);

    // for more code coverage we need to actually set a buffer in the message
    msg.setBuffer(new byte[0]);
    result = messenger.readJGMessage(msg);
    assertNull(result);
    receiver.receive(msg);

    // now create a view and a real distribution-message
    MemberIdentifier myAddress = messenger.getMemberID();
    MemberIdentifier other = createAddress(8888);
    GMSMembershipView v = new GMSMembershipView(myAddress);
    v.add(other);
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);

    List<MemberIdentifier> recipients = v.getMembers();
    HeartbeatMessage dmsg = new HeartbeatMessage();
    dmsg.setRecipients(recipients);

    // a message is ignored during manager shutdown
    msg = messenger.createJGMessage(dmsg, new JGAddress(other),
        recipients.get(0), Version.getCurrentVersion().ordinal());
    when(manager.shutdownInProgress()).thenReturn(Boolean.TRUE);
    receiver.receive(msg);
    verify(manager, never()).processMessage(isA(Message.class));
  }

  @Test
  public void testUseOldJChannel() throws Exception {
    initMocks(false, true);
    ((TestMembershipConfig) membershipConfig).oldMembershipInfo =
        new MembershipInformationImpl(messenger.myChannel,
            new ConcurrentLinkedQueue<>(), null);
    JGroupsMessenger newMessenger = new JGroupsMessenger();
    newMessenger.init(services);
    newMessenger.start();
    newMessenger.started();
    newMessenger.stop();
    assertTrue(newMessenger.myChannel == messenger.myChannel);
  }

  @Test
  public void testGetMessageState() throws Exception {
    initMocks(true/* multicast */);
    messenger.testMulticast(50); // do some multicast messaging
    NAKACK2 nakack = (NAKACK2) messenger.myChannel.getProtocolStack().findProtocol("NAKACK2");
    assertNotNull(nakack);
    long seqno = nakack.getCurrentSeqno();
    Map state = new HashMap();
    messenger.getMessageState(null, state, true);
    assertEquals(1, state.size());
    Long stateLong = (Long) state.values().iterator().next();
    assertTrue(
        "expected multicast state to be at least " + seqno + " but it was " + stateLong.longValue(),
        stateLong.longValue() >= seqno);
  }

  @Test
  public void testGetMessageStateNoMulticast() throws Exception {
    initMocks(false/* multicast */);
    Map state = new HashMap();
    messenger.getMessageState(null, state, true);
    assertEquals("expected an empty map but received " + state, 0, state.size());
  }

  @Test
  public void testWaitForMessageStateSucceeds() throws Exception {
    initMocks(true/* multicast */);
    JGroupsMessenger.MessageTracker tracker = mock(JGroupsMessenger.MessageTracker.class);
    MemberIdentifier mbr = createAddress(1234);
    messenger.scheduledMcastSeqnos.put(mbr, tracker);
    when(tracker.get()).thenReturn(0l, 2l, 49l, 50l, 80l);
    Map state = new HashMap();
    state.put("JGroups.mcastState", Long.valueOf(50));
    messenger.waitForMessageState(mbr, state);
    verify(tracker, times(4)).get();

    reset(tracker);
    when(tracker.get()).thenReturn(0l, 2l, 60l);
    messenger.waitForMessageState(mbr, state);
    verify(tracker, times(3)).get();
  }

  @Test
  public void testWaitForMessageStateThrowsExceptionIfMessagesMissing() throws Exception {
    initMocks(true/* multicast */);
    NAKACK2 nakack = mock(NAKACK2.class);
    Digest digest = mock(Digest.class);
    when(nakack.getDigest(any(Address.class))).thenReturn(digest);
    when(digest.get(any(Address.class))).thenReturn(new long[] {0, 0}, new long[] {2, 50},
        new long[] {49, 50});
    try {
      // message 50 will never arrive
      Map state = new HashMap();
      state.put("JGroups.mcastState", Long.valueOf(50));
      MemberIdentifier mbr = createAddress(1234);
      messenger.scheduledMcastSeqnos.put(mbr, new JGroupsMessenger.MessageTracker(30));
      messenger.waitForMessageState(mbr, state);
      fail("expected a MembershipIOException to be thrown");
    } catch (TimeoutException e) {
      // pass
    }
  }

  private GMSMembershipView createView(MemberIdentifier otherMbr) {
    MemberIdentifier sender = messenger.getMemberID();
    List<MemberIdentifier> mbrs = new ArrayList<>();
    mbrs.add(sender);
    mbrs.add(otherMbr);
    GMSMembershipView v = new GMSMembershipView(sender, 1, mbrs);
    return v;
  }

  @Test
  public void testEncryptedFindCoordinatorRequest() throws Exception {
    MemberIdentifier otherMbr = MemberIdentifierUtil.createMemberID(8888);

    initMocks(false, true);

    GMSMembershipView v = createView(otherMbr);
    when(joinLeave.getMemberID(messenger.getMemberID()))
        .thenReturn(messenger.getMemberID());
    GMSEncrypt otherMbrEncrptor = new GMSEncrypt(services, AES_128);

    messenger.setPublicKey(otherMbrEncrptor.getPublicKeyBytes(), otherMbr);
    messenger.initClusterKey();

    FindCoordinatorRequest gfmsg = new FindCoordinatorRequest(messenger.getMemberID(),
        new ArrayList<MemberIdentifier>(2), 1,
        messenger.getPublicKey(messenger.getMemberID()), 1, "");
    List<MemberIdentifier> recipients = new ArrayList<>();
    recipients.add(otherMbr);
    gfmsg.setRecipients(recipients);

    short version = Version.getCurrentVersion().ordinal();

    BufferDataOutputStream out =
        new BufferDataOutputStream(Version.getCurrentVersion());

    messenger.writeEncryptedMessage(gfmsg, otherMbr, version, out);

    byte[] requestBytes = out.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    Message distributionMessage =
        messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(gfmsg, distributionMessage);
  }

  @Test
  public void testEncryptedFindCoordinatorResponse() throws Exception {
    MemberIdentifier otherMbr = MemberIdentifierUtil.createMemberID(8888);

    Properties p = new Properties();

    initMocks(false, true);

    GMSMembershipView v = createView(otherMbr);

    GMSEncrypt otherMbrEncrptor = new GMSEncrypt(services, AES_128);
    otherMbrEncrptor.setPublicKey(messenger.getPublicKey(messenger.getMemberID()),
        messenger.getMemberID());

    messenger.setPublicKey(otherMbrEncrptor.getPublicKeyBytes(), otherMbr);
    messenger.initClusterKey();

    FindCoordinatorResponse gfmsg = new FindCoordinatorResponse(messenger.getMemberID(),
        messenger.getMemberID(), messenger.getClusterSecretKey(), 1);
    List<MemberIdentifier> recipients = new ArrayList<>();
    recipients.add(otherMbr);
    gfmsg.setRecipients(recipients);

    short version = Version.getCurrentVersion().ordinal();

    BufferDataOutputStream out =
        new BufferDataOutputStream(Version.getCurrentVersion());

    messenger.writeEncryptedMessage(gfmsg, otherMbr, version, out);

    byte[] requestBytes = out.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    messenger.addRequestId(1, messenger.getMemberID());

    Message distributionMessage =
        messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(gfmsg, distributionMessage);
  }

  @Test
  public void testEncryptedJoinRequest() throws Exception {
    MemberIdentifier otherMbr = MemberIdentifierUtil.createMemberID(8888);

    initMocks(false, true);

    GMSMembershipView v = createView(otherMbr);

    GMSEncrypt otherMbrEncrptor = new GMSEncrypt(services, AES_128);

    messenger.setPublicKey(otherMbrEncrptor.getPublicKeyBytes(), otherMbr);
    messenger.initClusterKey();

    JoinRequestMessage gfmsg =
        new JoinRequestMessage(otherMbr, messenger.getMemberID(), null, 9789, 1);

    short version = Version.getCurrentVersion().ordinal();

    BufferDataOutputStream out =
        new BufferDataOutputStream(Version.getCurrentVersion());

    messenger.writeEncryptedMessage(gfmsg, otherMbr, version, out);

    byte[] requestBytes = out.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    Message distributionMessage =
        messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(gfmsg, distributionMessage);
  }

  @Test
  public void testEncryptedJoinResponse() throws Exception {
    MemberIdentifier otherMbr = MemberIdentifierUtil.createMemberID(8888);

    initMocks(false, true);

    GMSMembershipView v = createView(otherMbr);

    GMSEncrypt otherMbrEncrptor = new GMSEncrypt(services, AES_128);
    otherMbrEncrptor.setPublicKey(messenger.getPublicKey(messenger.getMemberID()),
        messenger.getMemberID());

    messenger.setPublicKey(otherMbrEncrptor.getPublicKeyBytes(), otherMbr);
    messenger.initClusterKey();

    JoinResponseMessage gfmsg =
        new JoinResponseMessage(otherMbr, messenger.getClusterSecretKey(), 1);

    short version = Version.getCurrentVersion().ordinal();

    BufferDataOutputStream out =
        new BufferDataOutputStream(Version.getCurrentVersion());

    messenger.writeEncryptedMessage(gfmsg, otherMbr, version, out);

    byte[] requestBytes = out.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    messenger.addRequestId(1, messenger.getMemberID());

    Message gfMessageAtOtherMbr =
        messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(gfmsg, gfMessageAtOtherMbr);

    // lets send view as well..

    InstallViewMessage installViewMessage = new InstallViewMessage(v, null, true);

    out = new BufferDataOutputStream(Version.getCurrentVersion());

    messenger.writeEncryptedMessage(installViewMessage, otherMbr, version, out);

    requestBytes = out.toByteArray();

    JoinResponseMessage joinResponseMessage = (JoinResponseMessage) gfMessageAtOtherMbr;
    otherMbrEncrptor.setClusterKey(joinResponseMessage.getSecretPk());

    dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    gfMessageAtOtherMbr = messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(installViewMessage, gfMessageAtOtherMbr);

  }

  private MemberIdentifier createAddress(int port) {
    MemberIdentifier gms = MemberIdentifierUtil.createMemberID(port);
    gms.getMemberData().setUUID(UUID.randomUUID());
    gms.setVmKind(MemberIdentifier.NORMAL_DM_TYPE);
    gms.setVersionObjectForTest(Version.getCurrentVersion());
    return gms;
  }


  public static class ByteHolder implements DataSerializableFixedID {
    static final int DSFID = 123456;
    private byte[] payload;

    @Override
    public int getDSFID() {
      return DSFID;
    }

    public ByteHolder(byte[] payload) {
      this();
      this.payload = payload;
    }

    public ByteHolder() {}

    @Override
    public void toData(DataOutput out, SerializationContext context) throws IOException {
      StaticSerialization.writeByteArray(payload, out);
    }

    @Override
    public void fromData(DataInput in, DeserializationContext context)
        throws IOException {
      payload = StaticSerialization.readByteArray(in);
    }

    @Override
    public Version[] getSerializationVersions() {
      return null;
    }
  }

  static class TestMembershipConfig implements MembershipConfig {
    Object oldMembershipInfo;

    @Override
    public Object getOldMembershipInfo() {
      return oldMembershipInfo;
    }
  }

}
