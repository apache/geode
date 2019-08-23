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

import static org.apache.geode.distributed.ConfigurationProperties.ACK_WAIT_THRESHOLD;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_TCP;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_TTL;
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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.util.Digest;
import org.jgroups.util.UUID;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.GemFireIOException;
import org.apache.geode.SerializationException;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.ServiceConfig;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.Services.Stopper;
import org.apache.geode.distributed.internal.membership.gms.interfaces.GMSMessage;
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
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
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
  private long statsId = 123;

  private void initMocks(boolean enableMcast) throws Exception {
    initMocks(enableMcast, new Properties());
  }

  /**
   * Create stub and mock objects
   */
  private void initMocks(boolean enableMcast, Properties addProp) throws Exception {
    if (messenger != null) {
      messenger.stop();
      messenger = null;
    }
    Properties nonDefault = new Properties();
    nonDefault.put(DISABLE_TCP, "true");
    nonDefault.put(MCAST_PORT,
        enableMcast ? "" + AvailablePortHelper.getRandomAvailableUDPPort() : "0");
    nonDefault.put(MCAST_TTL, "0");
    nonDefault.put(LOG_FILE, "");
    nonDefault.put(LOG_LEVEL, "fine");
    nonDefault.put(LOCATORS, "localhost[10344]");
    nonDefault.put(ACK_WAIT_THRESHOLD, "1");
    nonDefault.putAll(addProp);
    DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
    RemoteTransportConfig tconfig =
        new RemoteTransportConfig(config, GMSMember.NORMAL_DM_TYPE);

    stopper = mock(Stopper.class);
    when(stopper.isCancelInProgress()).thenReturn(false);

    manager = mock(Manager.class);
    when(manager.isMulticastAllowed()).thenReturn(enableMcast);
    when(manager.wrapMessage(any(Object.class)))
        .thenAnswer(invocation -> invocation.getArgument(0));
    when(manager.unwrapMessage(any(GMSMessage.class)))
        .thenAnswer(invocation -> invocation.getArgument(0));

    healthMonitor = mock(HealthMonitor.class);

    joinLeave = mock(JoinLeave.class);

    ServiceConfig serviceConfig = new ServiceConfig(tconfig, config);

    services = mock(Services.class);
    when(services.getConfig()).thenReturn(serviceConfig);
    when(services.getCancelCriterion()).thenReturn(stopper);
    when(services.getHealthMonitor()).thenReturn(healthMonitor);
    when(services.getManager()).thenReturn(manager);
    when(services.getJoinLeave()).thenReturn(joinLeave);

    when(services.getStatistics()).thenReturn(mock(DistributionStats.class));

    messenger = new JGroupsMessenger();
    messenger.init(services);

    // if I do this earlier then test this return messenger as null
    when(services.getMessenger()).thenReturn(messenger);

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
    GMSMembershipView v = createView();
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);
    messenger.handleJGroupsIOException(new IOException("je m'en fiche"),
        new JGAddress(v.getMembers().get(1)));
    verify(healthMonitor).suspect(isA(GMSMember.class), isA(String.class));
  }

  @Test
  public void ioExceptionDuringShutdownAvoidsSuspectProcessing() throws Exception {
    // see GEODE-634
    initMocks(false);
    GMSMembershipView v = createView();
    when(joinLeave.getView()).thenReturn(v);
    when(manager.shutdownInProgress()).thenReturn(true);
    messenger.installView(v);
    messenger.handleJGroupsIOException(new IOException("fichez-moi le camp"),
        new JGAddress(v.getMembers().get(1)));
    verify(healthMonitor, never()).checkIfAvailable(isA(GMSMember.class),
        isA(String.class), isA(Boolean.class));
  }

  private GMSMembershipView createView() {
    GMSMember sender = messenger.getMemberID();
    List<GMSMember> mbrs = new ArrayList<>();
    mbrs.add(sender);
    mbrs.add(createAddress(100));
    mbrs.add(createAddress(101));
    GMSMembershipView v = new GMSMembershipView(sender, 1, mbrs);
    return v;
  }

  @Test
  public void normalMessagesUseFlowControl() throws Exception {
    initMocks(false);
    Message jgmsg = new Message();
    GMSMessage dmsg = mock(GMSMessage.class);
    when(dmsg.isHighPriority()).thenReturn(false);
    messenger.setMessageFlags(dmsg, jgmsg);
    assertFalse("expected flow-control to be used: " + jgmsg,
        jgmsg.isFlagSet(Message.Flag.NO_FC));
  }

  @Test
  public void highPriorityMessagesBypassFlowControl() throws Exception {
    initMocks(false);
    Message jgmsg = new Message();
    GMSMessage dmsg = mock(GMSMessage.class);
    when(dmsg.isHighPriority()).thenReturn(true);
    messenger.setMessageFlags(dmsg, jgmsg);
    assertTrue("expected flow-control to not be used: " + jgmsg,
        jgmsg.isFlagSet(Message.Flag.NO_FC));
  }

  @Test
  public void testMemberWeightIsSerialized() throws Exception {
    HeapDataOutputStream out = new HeapDataOutputStream(500, Version.CURRENT);
    GMSMember mbr = createAddress(8888);
    mbr.setMemberWeight((byte) 40);
    mbr.toData(out);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    mbr = new GMSMember();
    mbr.fromData(in);
    assertEquals(40, mbr.getMemberWeight());
  }

  @Test
  public void testSerializationError() throws Exception {
    for (int i = 0; i < 2; i++) {
      boolean enableMcast = (i == 1);
      initMocks(enableMcast);
      GMSMember mbr = createAddress(8888);
      HeartbeatMessage msg =
          mock(HeartbeatMessage.class);
      when(msg.getRecipients()).thenReturn(Collections.singletonList(mbr));
      when(msg.getMulticast()).thenReturn(enableMcast);
      when(msg.getDSFID()).thenReturn((int) DataSerializableFixedID.HEARTBEAT_RESPONSE);

      // for code coverage we need to test with both a SerializationException and
      // an IOException. The former is wrapped in a GemfireIOException while the
      // latter is not
      doThrow(new SerializationException("")).when(msg).toData(any(DataOutput.class));
      try {
        messenger.send(msg);
        fail("expected a failure");
      } catch (GemFireIOException e) {
        // success
      }
      if (enableMcast) {
        verify(msg, atLeastOnce()).registerProcessor();
      }
      doThrow(new IOException()).when(msg).toData(any(DataOutput.class));
      try {
        messenger.send(msg);
        fail("expected a failure");
      } catch (GemFireIOException e) {
        // success
      }
    }
  }

  @Test
  public void testJChannelError() throws Exception {
    for (int i = 0; i < 2; i++) {
      boolean enableMcast = (i == 1);
      initMocks(enableMcast);
      JChannel mockChannel = mock(JChannel.class);
      when(mockChannel.isConnected()).thenReturn(true);
      doThrow(new RuntimeException()).when(mockChannel).send(any(Message.class));
      JChannel realChannel = messenger.myChannel;
      messenger.myChannel = mockChannel;
      try {
        GMSMember mbr = createAddress(8888);
        HeartbeatMessage msg = mock(HeartbeatMessage.class);
        when(msg.getRecipients()).thenReturn(Collections.singletonList(mbr));
        when(msg.getMulticast()).thenReturn(enableMcast);
        when(msg.getDSFID()).thenReturn((int) DataSerializableFixedID.HEARTBEAT_RESPONSE);
        try {
          messenger.send(msg);
          fail("expected a failure");
        } catch (DistributedSystemDisconnectedException e) {
          // success
        }
        verify(mockChannel).send(isA(Message.class));
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
        shutdownCause = new ForcedDisconnectException("");
        ex = new RuntimeException("", shutdownCause);
      }
      doThrow(ex).when(mockChannel).send(any(Message.class));
      JChannel realChannel = messenger.myChannel;
      messenger.myChannel = mockChannel;

      when(services.getShutdownCause()).thenReturn(shutdownCause);

      try {
        GMSMember mbr = createAddress(8888);
        HeartbeatMessage msg = mock(HeartbeatMessage.class);
        when(msg.getRecipients()).thenReturn(Collections.singletonList(mbr));
        when(msg.getMulticast()).thenReturn(enableMcast);
        when(msg.getDSFID()).thenReturn((int) DataSerializableFixedID.HEARTBEAT_RESPONSE);
        try {
          messenger.send(msg);
          fail("expected a failure");
        } catch (DistributedSystemDisconnectedException e) {
          // the ultimate cause should be the shutdownCause returned
          // by Services.getShutdownCause()
          Throwable cause = e;
          while (cause.getCause() != null) {
            cause = cause.getCause();
          }
          assertTrue(cause != e);
          assertTrue(cause == shutdownCause);
        }
        verify(mockChannel).send(isA(Message.class));
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
      doThrow(new RuntimeException()).when(mockChannel).send(any(Message.class));
      JChannel realChannel = messenger.myChannel;
      messenger.myChannel = mockChannel;
      try {
        GMSMember mbr = createAddress(8888);
        HeartbeatMessage msg =
            mock(HeartbeatMessage.class);
        when(msg.getRecipients()).thenReturn(Collections.singletonList(mbr));
        when(msg.getMulticast()).thenReturn(false);
        try {
          messenger.send(msg);
          fail("expected a failure");
        } catch (DistributedSystemDisconnectedException e) {
          // success
        }
        verify(mockChannel, never()).send(isA(Message.class));
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
      GMSMember mbr = createAddress(8888);
      HeartbeatMessage msg =
          mock(HeartbeatMessage.class);
      when(msg.getRecipients()).thenReturn(Collections.singletonList(mbr));
      when(msg.getMulticast()).thenReturn(enableMcast);
      when(msg.getDSFID()).thenReturn((int) DataSerializableFixedID.HEARTBEAT_RESPONSE);
      interceptor.collectMessages = true;
      try {
        messenger.sendUnreliably(msg);
      } catch (GemFireIOException e) {
        fail("expected success");
      }
      if (enableMcast) {
        verify(msg, atLeastOnce()).registerProcessor();
      }
      verify(msg).toData(isA(DataOutput.class));
      assertTrue("expected 1 message but found " + interceptor.collectedMessages,
          interceptor.collectedMessages.size() == 1);
      assertTrue(interceptor.collectedMessages.get(0).isFlagSet(Message.Flag.NO_RELIABILITY));
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

    GMSMember addr = messenger.getMemberID();
    GMSMembershipView v = new GMSMembershipView(addr);
    when(joinLeave.getView()).thenReturn(v);


    GMSMember sender = createAddress(8888);

    JoinRequestMessage msg = new JoinRequestMessage(messenger.localAddress, sender, null, -1, 0);

    Message jmsg = messenger.createJGMessage(msg, messenger.jgAddress, messenger.localAddress,
        Version.CURRENT_ORDINAL);
    interceptor.up(new Event(Event.MSG, jmsg));

    verify(mh, times(1)).processMessage(any(JoinRequestMessage.class));

    LeaveRequestMessage lmsg = new LeaveRequestMessage(messenger.localAddress, sender, "testing");
    when(joinLeave.getMemberID(any())).thenReturn(sender);
    jmsg = messenger.createJGMessage(lmsg, messenger.jgAddress, messenger.localAddress,
        Version.CURRENT_ORDINAL);
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

    GMSMember sender = messenger.getMemberID();
    GMSMembershipView v = new GMSMembershipView(sender);
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);

    // send a big message and expect fragmentation
    GMSMember recipient = broadcastMessage ? null : messenger.localAddress;
    JoinRequestMessage msg = new JoinRequestMessage(recipient, sender,
        new byte[(int) (services.getConfig().getDistributionConfig().getUdpFragmentSize() * (1.5))],
        -1, 0);

    msg.setMulticast(broadcastMessage);

    // configure an incoming message handler for JoinRequestMessage
    final JoinRequestMessage[] messageReceived = new JoinRequestMessage[1];
    messenger.addHandler(JoinRequestMessage.class, message -> messageReceived[0] = message);

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

    List<Message> messages = new ArrayList<>(interceptor.collectedMessages);
    UUID fakeMember = new UUID(50, 50);
    short unicastHeaderId = ClassConfigurator.getProtocolId(UNICAST3.class);
    int seqno = 1;
    for (Message m : messages) {
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
    GMSMember sender = messenger.getMemberID();
    GMSMember other = createAddress(8888);

    GMSMembershipView v = new GMSMembershipView(sender);
    v.add(other);
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);

    List<GMSMember> recipients = v.getMembers();
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
    doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doCallRealMethod().when(services).isShutdownDueToForcedDisconnect();
    doCallRealMethod().when(services).isAutoReconnectEnabled();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertTrue(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelStillConnectedAfterStopAfterForcedDisconnectWithAutoReconnect()
      throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doCallRealMethod().when(services).isShutdownDueToForcedDisconnect();
    doCallRealMethod().when(services).isAutoReconnectEnabled();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertTrue(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelStillConnectedAfteremergencyWhileReconnectingDS() throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    doReturn(false).when(services).isAutoReconnectEnabled();
    doReturn(true).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertTrue(messenger.myChannel.isConnected());
  }


  @Test
  public void testChannelStillConnectedAfterStopWhileReconnectingDS() throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    doReturn(false).when(services).isAutoReconnectEnabled();
    doReturn(true).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertTrue(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelClosedOnEmergencyClose() throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    doReturn(false).when(services).isAutoReconnectEnabled();
    doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertFalse(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelClosedOnStop() throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    doReturn(false).when(services).isAutoReconnectEnabled();
    doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertFalse(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelClosedAfterEmergencyCloseForcedDisconnectWithoutAutoReconnect()
      throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(true).when(services).isShutdownDueToForcedDisconnect();
    doReturn(false).when(services).isAutoReconnectEnabled();
    doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertFalse(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelStillConnectedStopAfterForcedDisconnectWithoutAutoReconnect()
      throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(true).when(services).isShutdownDueToForcedDisconnect();
    doReturn(false).when(services).isAutoReconnectEnabled();
    doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertFalse(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelClosedAfterEmergencyCloseNotForcedDisconnectWithAutoReconnect()
      throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    doReturn(true).when(services).isAutoReconnectEnabled();
    doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertFalse(messenger.myChannel.isConnected());
  }

  @Test
  public void testChannelStillConnectedStopNotForcedDisconnectWithAutoReconnect() throws Exception {
    initMocks(false);
    doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    doCallRealMethod().when(services).getShutdownCause();
    doCallRealMethod().when(services).emergencyClose();
    doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    doReturn(true).when(services).isAutoReconnectEnabled();
    doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertFalse(messenger.myChannel.isConnected());
  }

  @Test
  public void testMessageFiltering() throws Exception {
    initMocks(true);
    GMSMember mbr = createAddress(8888);
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
    GMSMember mbr = createAddress(8888);
    JGAddress addr = new JGAddress(mbr);

    Message pingMessage = pinger.createPingMessage(null, addr);
    assertTrue(pinger.isPingMessage(pingMessage.getBuffer()));
    assertFalse(pinger.isPongMessage(pingMessage.getBuffer()));

    Message pongMessage = pinger.createPongMessage(null, addr);
    assertTrue(pinger.isPongMessage(pongMessage.getBuffer()));
    assertFalse(pinger.isPingMessage(pongMessage.getBuffer()));

    interceptor.collectMessages = true;
    pinger.sendPingMessage(messenger.myChannel, null, addr);
    assertEquals("expected 1 message but found " + interceptor.collectedMessages,
        interceptor.collectedMessages.size(), 1);
    pingMessage = interceptor.collectedMessages.get(0);
    assertTrue(pinger.isPingMessage(pingMessage.getBuffer()));

    interceptor.collectedMessages.clear();
    pinger.sendPongMessage(messenger.myChannel, null, addr);
    assertEquals("expected 1 message but found " + interceptor.collectedMessages,
        interceptor.collectedMessages.size(), 1);
    pongMessage = interceptor.collectedMessages.get(0);
    assertTrue(pinger.isPongMessage(pongMessage.getBuffer()));

    interceptor.collectedMessages.clear();
    JGroupsReceiver receiver = (JGroupsReceiver) messenger.myChannel.getReceiver();
    long pongsReceived = messenger.pongsReceived.longValue();
    receiver.receive(pongMessage);
    assertEquals(pongsReceived + 1, messenger.pongsReceived.longValue());
    receiver.receive(pingMessage);
    assertEquals("expected 1 message but found " + interceptor.collectedMessages,
        interceptor.collectedMessages.size(), 1);
    Message m = interceptor.collectedMessages.get(0);
    assertTrue(pinger.isPongMessage(m.getBuffer()));
  }

  @Test
  public void testJGroupsIOExceptionHandler() throws Exception {
    initMocks(false);
    GMSMember mbr = createAddress(8888);
    GMSMembershipView v = new GMSMembershipView(mbr);
    v.add(messenger.getMemberID());
    messenger.installView(v);

    IOException ioe = new IOException("test exception");
    messenger.handleJGroupsIOException(ioe, new JGAddress(mbr));
    messenger.handleJGroupsIOException(ioe, new JGAddress(mbr)); // should be ignored
    verify(healthMonitor).suspect(mbr, "Unable to send messages to this member via JGroups");
  }

  @Test
  public void testReceiver() throws Exception {
    initMocks(false);
    JGroupsReceiver receiver = (JGroupsReceiver) messenger.myChannel.getReceiver();
    messenger.addHandler(HeartbeatMessage.class, message -> {
    });

    // a zero-length message is ignored
    Message msg = new Message(new JGAddress(messenger.getMemberID()));
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
    GMSMember myAddress = messenger.getMemberID();
    GMSMember other = createAddress(8888);
    GMSMembershipView v = new GMSMembershipView(myAddress);
    v.add(other);
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);

    List<GMSMember> recipients = v.getMembers();
    HeartbeatMessage dmsg = new HeartbeatMessage();
    dmsg.setRecipients(recipients);

    // a message is ignored during manager shutdown
    msg = messenger.createJGMessage(dmsg, new JGAddress(other),
        recipients.get(0), Version.CURRENT_ORDINAL);
    when(manager.shutdownInProgress()).thenReturn(Boolean.TRUE);
    receiver.receive(msg);
    verify(manager, never()).processMessage(isA(GMSMessage.class));
    verify(services.getStatistics(), times(3)).incUDPDispatchRequestTime(isA(Long.class));
  }

  @Test
  public void testUseOldJChannel() throws Exception {
    initMocks(false);
    JChannel channel = messenger.myChannel;
    services.getConfig().getTransport().setOldDSMembershipInfo(new MembershipInformation(channel,
        Collections.singleton(new GMSMember("localhost", 10000)),
        new ConcurrentLinkedQueue<>()));
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
    GMSMember mbr = createAddress(1234);
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
      GMSMember mbr = createAddress(1234);
      messenger.scheduledMcastSeqnos.put(mbr, new JGroupsMessenger.MessageTracker(30));
      messenger.waitForMessageState(mbr, state);
      fail("expected a GemFireIOException to be thrown");
    } catch (GemFireIOException e) {
      // pass
    }
  }

  private GMSMembershipView createView(GMSMember otherMbr) {
    GMSMember sender = messenger.getMemberID();
    List<GMSMember> mbrs = new ArrayList<>();
    mbrs.add(sender);
    mbrs.add(otherMbr);
    GMSMembershipView v = new GMSMembershipView(sender, 1, mbrs);
    return v;
  }

  @Test
  public void testEncryptedFindCoordinatorRequest() throws Exception {
    GMSMember otherMbr = new GMSMember("localhost", 8888);

    Properties p = new Properties();
    final String udpDhalgo = "AES:128";
    p.put(ConfigurationProperties.SECURITY_UDP_DHALGO, udpDhalgo);
    initMocks(false, p);

    GMSMembershipView v = createView(otherMbr);
    when(joinLeave.getMemberID(messenger.getMemberID()))
        .thenReturn(messenger.getMemberID());
    GMSEncrypt otherMbrEncrptor = new GMSEncrypt(services, udpDhalgo);

    messenger.setPublicKey(otherMbrEncrptor.getPublicKeyBytes(), otherMbr);
    messenger.initClusterKey();

    FindCoordinatorRequest gfmsg = new FindCoordinatorRequest(messenger.getMemberID(),
        new ArrayList<GMSMember>(2), 1,
        messenger.getPublicKey(messenger.getMemberID()), 1, "");
    List<GMSMember> recipients = new ArrayList<>();
    recipients.add(otherMbr);
    gfmsg.setRecipients(recipients);

    short version = Version.CURRENT_ORDINAL;

    HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);

    messenger.writeEncryptedMessage(gfmsg, otherMbr, version, out);

    byte[] requestBytes = out.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    GMSMessage distributionMessage =
        messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(gfmsg, distributionMessage);
  }

  @Test
  public void testEncryptedFindCoordinatorResponse() throws Exception {
    GMSMember otherMbr = new GMSMember("localhost", 8888);

    Properties p = new Properties();

    p.put(ConfigurationProperties.SECURITY_UDP_DHALGO, AES_128);
    initMocks(false, p);

    GMSMembershipView v = createView(otherMbr);

    GMSEncrypt otherMbrEncrptor = new GMSEncrypt(services, AES_128);
    otherMbrEncrptor.setPublicKey(messenger.getPublicKey(messenger.getMemberID()),
        messenger.getMemberID());

    messenger.setPublicKey(otherMbrEncrptor.getPublicKeyBytes(), otherMbr);
    messenger.initClusterKey();

    FindCoordinatorResponse gfmsg = new FindCoordinatorResponse(messenger.getMemberID(),
        messenger.getMemberID(), messenger.getClusterSecretKey(), 1);
    List<GMSMember> recipients = new ArrayList<>();
    recipients.add(otherMbr);
    gfmsg.setRecipients(recipients);

    short version = Version.CURRENT_ORDINAL;

    HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);

    messenger.writeEncryptedMessage(gfmsg, otherMbr, version, out);

    byte[] requestBytes = out.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    messenger.addRequestId(1, messenger.getMemberID());

    GMSMessage distributionMessage =
        messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(gfmsg, distributionMessage);
  }

  @Test
  public void testEncryptedJoinRequest() throws Exception {
    GMSMember otherMbr = new GMSMember("localhost", 8888);

    Properties p = new Properties();
    p.put(ConfigurationProperties.SECURITY_UDP_DHALGO, AES_128);
    initMocks(false, p);

    GMSMembershipView v = createView(otherMbr);

    GMSEncrypt otherMbrEncrptor = new GMSEncrypt(services, AES_128);

    messenger.setPublicKey(otherMbrEncrptor.getPublicKeyBytes(), otherMbr);
    messenger.initClusterKey();

    JoinRequestMessage gfmsg =
        new JoinRequestMessage(otherMbr, messenger.getMemberID(), null, 9789, 1);

    short version = Version.CURRENT_ORDINAL;

    HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);

    messenger.writeEncryptedMessage(gfmsg, otherMbr, version, out);

    byte[] requestBytes = out.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    GMSMessage distributionMessage =
        messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(gfmsg, distributionMessage);
  }

  @Test
  public void testEncryptedJoinResponse() throws Exception {
    GMSMember otherMbr = new GMSMember("localhost", 8888);

    Properties p = new Properties();
    p.put(ConfigurationProperties.SECURITY_UDP_DHALGO, AES_128);
    initMocks(false, p);

    GMSMembershipView v = createView(otherMbr);

    GMSEncrypt otherMbrEncrptor = new GMSEncrypt(services, AES_128);
    otherMbrEncrptor.setPublicKey(messenger.getPublicKey(messenger.getMemberID()),
        messenger.getMemberID());

    messenger.setPublicKey(otherMbrEncrptor.getPublicKeyBytes(), otherMbr);
    messenger.initClusterKey();

    JoinResponseMessage gfmsg =
        new JoinResponseMessage(otherMbr, messenger.getClusterSecretKey(), 1);

    short version = Version.CURRENT_ORDINAL;

    HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);

    messenger.writeEncryptedMessage(gfmsg, otherMbr, version, out);

    byte[] requestBytes = out.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    messenger.addRequestId(1, messenger.getMemberID());

    GMSMessage gfMessageAtOtherMbr =
        messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(gfmsg, gfMessageAtOtherMbr);

    // lets send view as well..

    InstallViewMessage installViewMessage = new InstallViewMessage(v, null, true);

    out = new HeapDataOutputStream(Version.CURRENT);

    messenger.writeEncryptedMessage(installViewMessage, otherMbr, version, out);

    requestBytes = out.toByteArray();

    JoinResponseMessage joinResponseMessage = (JoinResponseMessage) gfMessageAtOtherMbr;
    otherMbrEncrptor.setClusterKey(joinResponseMessage.getSecretPk());

    dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    gfMessageAtOtherMbr = messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(installViewMessage, gfMessageAtOtherMbr);

  }

  private GMSMember createAddress(int port) {
    GMSMember gms = new GMSMember("localhost", port);
    gms.setUUID(UUID.randomUUID());
    gms.setVmKind(GMSMember.NORMAL_DM_TYPE);
    gms.setVersionOrdinal(Version.CURRENT_ORDINAL);
    return gms;
  }
}
