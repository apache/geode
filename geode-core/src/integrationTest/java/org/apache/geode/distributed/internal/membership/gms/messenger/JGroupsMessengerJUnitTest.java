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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.SerializationException;
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
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.SerialAckedMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.ServiceConfig;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.Services.Stopper;
import org.apache.geode.distributed.internal.membership.gms.interfaces.HealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.distributed.internal.membership.gms.interfaces.MessageHandler;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.distributed.internal.membership.gms.messages.InstallViewMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinResponseMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messenger.JGroupsMessenger.JGroupsReceiver;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.cache.DistributedCacheOperation;
import org.apache.geode.internal.logging.log4j.AlertAppender;
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
        new RemoteTransportConfig(config, ClusterDistributionManager.NORMAL_DM_TYPE);

    stopper = mock(Stopper.class);
    when(stopper.isCancelInProgress()).thenReturn(false);

    manager = mock(Manager.class);
    when(manager.isMulticastAllowed()).thenReturn(enableMcast);

    healthMonitor = mock(HealthMonitor.class);

    joinLeave = mock(JoinLeave.class);

    ServiceConfig serviceConfig = new ServiceConfig(tconfig, config);

    services = mock(Services.class);
    when(services.getConfig()).thenReturn(serviceConfig);
    when(services.getCancelCriterion()).thenReturn(stopper);
    when(services.getHealthMonitor()).thenReturn(healthMonitor);
    when(services.getManager()).thenReturn(manager);
    when(services.getJoinLeave()).thenReturn(joinLeave);

    DistributionManager dm = mock(DistributionManager.class);
    InternalDistributedSystem system =
        InternalDistributedSystem.newInstanceForTesting(dm, nonDefault);
    when(services.getStatistics()).thenReturn(new DistributionStats(system, statsId));

    messenger = new JGroupsMessenger();
    messenger.init(services);

    // if I do this earlier then test this return messenger as null
    when(services.getMessenger()).thenReturn(messenger);

    String jgroupsConfig = messenger.getJGroupsStackConfig();
    int startIdx = jgroupsConfig.indexOf("<org");
    int insertIdx = jgroupsConfig.indexOf('>', startIdx + 4) + 1;
    jgroupsConfig = jgroupsConfig.substring(0, insertIdx) + "<" + InterceptUDP.class.getName()
        + "/>" + jgroupsConfig.substring(insertIdx);
    messenger.setJGroupsStackConfigForTesting(jgroupsConfig);
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
    NetView v = createView();
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);
    messenger.handleJGroupsIOException(new IOException("je m'en fiche"),
        new JGAddress(v.getMembers().get(1)));
    verify(healthMonitor).suspect(isA(InternalDistributedMember.class), isA(String.class));
  }

  @Test
  public void ioExceptionDuringShutdownAvoidsSuspectProcessing() throws Exception {
    // see GEODE-634
    initMocks(false);
    NetView v = createView();
    when(joinLeave.getView()).thenReturn(v);
    when(manager.shutdownInProgress()).thenReturn(true);
    messenger.installView(v);
    messenger.handleJGroupsIOException(new IOException("fichez-moi le camp"),
        new JGAddress(v.getMembers().get(1)));
    verify(healthMonitor, never()).checkIfAvailable(isA(InternalDistributedMember.class),
        isA(String.class), isA(Boolean.class));
  }

  private NetView createView() {
    InternalDistributedMember sender = messenger.getMemberID();
    List<InternalDistributedMember> mbrs = new ArrayList<>();
    mbrs.add(sender);
    mbrs.add(createAddress(100));
    mbrs.add(createAddress(101));
    NetView v = new NetView(sender, 1, mbrs);
    return v;
  }

  @Test
  public void alertMessagesBypassFlowControl() throws Exception {
    initMocks(false);
    Message jgmsg = new Message();
    DistributionMessage dmsg = mock(DistributionMessage.class);
    when(dmsg.getProcessorType()).thenReturn(ClusterDistributionManager.SERIAL_EXECUTOR);
    messenger.setMessageFlags(dmsg, jgmsg);
    assertFalse("expected no_fc to not be set in " + jgmsg.getFlags(),
        jgmsg.isFlagSet(Message.Flag.NO_FC));
    AlertAppender.setIsAlerting(true);
    try {
      messenger.setMessageFlags(dmsg, jgmsg);
      assertTrue("expected no_fc to be set in " + jgmsg.getFlags(),
          jgmsg.isFlagSet(Message.Flag.NO_FC));
    } finally {
      AlertAppender.setIsAlerting(false);
    }
  }

  @Test
  public void testMemberWeightIsSerialized() throws Exception {
    HeapDataOutputStream out = new HeapDataOutputStream(500, Version.CURRENT);
    InternalDistributedMember mbr = createAddress(8888);
    ((GMSMember) mbr.getNetMember()).setMemberWeight((byte) 40);
    mbr.toData(out);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    mbr = new InternalDistributedMember();
    mbr.fromData(in);
    assertEquals(40, mbr.getNetMember().getMemberWeight());
  }

  @Test
  public void testSerializationError() throws Exception {
    for (int i = 0; i < 2; i++) {
      boolean enableMcast = (i == 1);
      initMocks(enableMcast);
      InternalDistributedMember mbr = createAddress(8888);
      DistributedCacheOperation.CacheOperationMessage msg =
          mock(DistributedCacheOperation.CacheOperationMessage.class);
      when(msg.getRecipients()).thenReturn(new InternalDistributedMember[] {mbr});
      when(msg.getMulticast()).thenReturn(enableMcast);
      if (!enableMcast) {
        // for non-mcast we send a message with a reply-processor
        when(msg.getProcessorId()).thenReturn(1234);
      } else {
        // for mcast we send a direct-ack message and expect the messenger
        // to register it
        when(msg.isDirectAck()).thenReturn(true);
      }
      when(msg.getDSFID()).thenReturn((int) DataSerializableFixedID.PUT_ALL_MESSAGE);

      // for code coverage we need to test with both a SerializationException and
      // an IOException. The former is wrapped in a GemfireIOException while the
      // latter is not
      doThrow(new SerializationException()).when(msg).toData(any(DataOutput.class));
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
        InternalDistributedMember mbr = createAddress(8888);
        DistributedCacheOperation.CacheOperationMessage msg =
            mock(DistributedCacheOperation.CacheOperationMessage.class);
        when(msg.getRecipients()).thenReturn(new InternalDistributedMember[] {mbr});
        when(msg.getMulticast()).thenReturn(enableMcast);
        when(msg.getProcessorId()).thenReturn(1234);
        when(msg.getDSFID()).thenReturn((int) DataSerializableFixedID.PUT_ALL_MESSAGE);
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
        InternalDistributedMember mbr = createAddress(8888);
        DistributedCacheOperation.CacheOperationMessage msg =
            mock(DistributedCacheOperation.CacheOperationMessage.class);
        when(msg.getRecipients()).thenReturn(new InternalDistributedMember[] {mbr});
        when(msg.getMulticast()).thenReturn(enableMcast);
        when(msg.getProcessorId()).thenReturn(1234);
        when(msg.getDSFID()).thenReturn((int) DataSerializableFixedID.PUT_ALL_MESSAGE);
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
        InternalDistributedMember mbr = createAddress(8888);
        DistributedCacheOperation.CacheOperationMessage msg =
            mock(DistributedCacheOperation.CacheOperationMessage.class);
        when(msg.getRecipients()).thenReturn(new InternalDistributedMember[] {mbr});
        when(msg.getMulticast()).thenReturn(false);
        when(msg.getProcessorId()).thenReturn(1234);
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
      InternalDistributedMember mbr = createAddress(8888);
      DistributedCacheOperation.CacheOperationMessage msg =
          mock(DistributedCacheOperation.CacheOperationMessage.class);
      when(msg.getRecipients()).thenReturn(new InternalDistributedMember[] {mbr});
      when(msg.getMulticast()).thenReturn(enableMcast);
      if (!enableMcast) {
        // for non-mcast we send a message with a reply-processor
        when(msg.getProcessorId()).thenReturn(1234);
      } else {
        // for mcast we send a direct-ack message and expect the messenger
        // to register it
        when(msg.isDirectAck()).thenReturn(true);
      }
      when(msg.getDSFID()).thenReturn((int) DataSerializableFixedID.PUT_ALL_MESSAGE);
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

    InternalDistributedMember addr = messenger.getMemberID();
    NetView v = new NetView(addr);
    when(joinLeave.getView()).thenReturn(v);


    InternalDistributedMember sender = createAddress(8888);

    JoinRequestMessage msg = new JoinRequestMessage(messenger.localAddress, sender, null, -1, 0);

    Message jmsg = messenger.createJGMessage(msg, messenger.jgAddress, Version.CURRENT_ORDINAL);
    interceptor.up(new Event(Event.MSG, jmsg));

    verify(mh, times(1)).processMessage(any(JoinRequestMessage.class));

    LeaveRequestMessage lmsg = new LeaveRequestMessage(messenger.localAddress, sender, "testing");
    when(joinLeave.getMemberID(any())).thenReturn(sender);
    jmsg = messenger.createJGMessage(lmsg, messenger.jgAddress, Version.CURRENT_ORDINAL);
    interceptor.up(new Event(Event.MSG, jmsg));

    verify(manager).processMessage(any(LeaveRequestMessage.class));

  }


  @Test
  public void testBigMessageIsFragmented() throws Exception {
    doTestBigMessageIsFragmented(false, false);
  }

  @Test
  public void testBigMessageIsFragmentedMcast() throws Exception {
    doTestBigMessageIsFragmented(true, true);
  }

  @Test
  public void testBroadcastUDPMessage() throws Exception {
    doTestBigMessageIsFragmented(false, true);
  }

  public void doTestBigMessageIsFragmented(boolean mcastEnabled, boolean mcastMsg)
      throws Exception {
    initMocks(mcastEnabled);
    MessageHandler mh = mock(MessageHandler.class);
    messenger.addHandler(JoinRequestMessage.class, mh);

    InternalDistributedMember sender = messenger.getMemberID();
    NetView v = new NetView(sender);
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);
    JoinRequestMessage msg = new JoinRequestMessage(messenger.localAddress, sender, null, -1, 0);
    if (mcastMsg) {
      msg.setMulticast(true);
    }

    messenger.send(msg);
    int sentMessages = (mcastEnabled && mcastMsg) ? interceptor.mcastSentDataMessages
        : interceptor.unicastSentDataMessages;
    assertTrue("expected 1 message to be sent but found " + sentMessages, sentMessages == 1);

    // send a big message and expect fragmentation
    msg = new JoinRequestMessage(messenger.localAddress, sender,
        new byte[(int) (services.getConfig().getDistributionConfig().getUdpFragmentSize() * (1.5))],
        -1, 0);

    // configure an incoming message handler for JoinRequestMessage
    final DistributionMessage[] messageReceived = new DistributionMessage[1];
    MessageHandler handler = new MessageHandler() {
      @Override
      public void processMessage(DistributionMessage m) {
        messageReceived[0] = m;
      }
    };
    messenger.addHandler(JoinRequestMessage.class, handler);

    // configure the outgoing message interceptor
    interceptor.unicastSentDataMessages = 0;
    interceptor.collectMessages = true;
    interceptor.collectedMessages.clear();

    messenger.send(msg);

    assertTrue("expected 2 messages to be sent but found " + interceptor.unicastSentDataMessages,
        interceptor.unicastSentDataMessages == 2);

    List<Message> messages = new ArrayList<>(interceptor.collectedMessages);
    UUID fakeMember = new UUID(50, 50);
    short unicastHeaderId = ClassConfigurator.getProtocolId(UNICAST3.class);
    int seqno = 1;
    for (Message m : messages) {
      m.setSrc(fakeMember);
      UNICAST3.Header oldHeader = (UNICAST3.Header) m.getHeader(unicastHeaderId);
      if (oldHeader == null)
        continue;
      UNICAST3.Header newHeader =
          UNICAST3.Header.createDataHeader(seqno, oldHeader.connId(), seqno == 1);
      seqno += 1;
      m.putHeader(unicastHeaderId, newHeader);
      interceptor.up(new Event(Event.MSG, m));
    }
    Thread.sleep(5000);
    System.out.println("received message = " + messageReceived[0]);
  }

  @Test
  public void testSendToMultipleMembers() throws Exception {
    initMocks(false);
    InternalDistributedMember sender = messenger.getMemberID();
    InternalDistributedMember other = createAddress(8888);

    NetView v = new NetView(sender);
    v.add(other);
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);

    List<InternalDistributedMember> recipients = v.getMembers();
    SerialAckedMessage msg = new SerialAckedMessage();
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
    InternalDistributedMember mbr = createAddress(8888);
    NetView view = new NetView(mbr);

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
    GMSPingPonger pinger = messenger.getPingPonger();
    InternalDistributedMember mbr = createAddress(8888);
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
    InternalDistributedMember mbr = createAddress(8888);
    NetView v = new NetView(mbr);
    v.add(messenger.getMemberID());
    messenger.installView(v);

    IOException ioe = new IOException("test exception");
    messenger.handleJGroupsIOException(ioe, new JGAddress(mbr));
    messenger.handleJGroupsIOException(ioe, new JGAddress(mbr)); // should be ignored
    verify(healthMonitor).suspect(mbr, "Unable to send messages to this member via JGroups");
  }

  @Test
  public void testReceiver() throws Exception {
    try {
      DistributionStats.enableClockStats = true;
      initMocks(false);
      JGroupsReceiver receiver = (JGroupsReceiver) messenger.myChannel.getReceiver();

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
      InternalDistributedMember myAddress = messenger.getMemberID();
      InternalDistributedMember other = createAddress(8888);
      NetView v = new NetView(myAddress);
      v.add(other);
      when(joinLeave.getView()).thenReturn(v);
      messenger.installView(v);

      List<InternalDistributedMember> recipients = v.getMembers();
      SerialAckedMessage dmsg = new SerialAckedMessage();
      dmsg.setRecipients(recipients);

      // a message is ignored during manager shutdown
      msg = messenger.createJGMessage(dmsg, new JGAddress(other), Version.CURRENT_ORDINAL);
      when(manager.shutdownInProgress()).thenReturn(Boolean.TRUE);
      receiver.receive(msg);
      verify(manager, never()).processMessage(isA(DistributionMessage.class));

      assertTrue("There should be UDPDispatchRequestTime stats",
          services.getStatistics().getUDPDispatchRequestTime() > 0);
    } finally {
      DistributionStats.enableClockStats = false;
    }
  }

  @Test
  public void testUseOldJChannel() throws Exception {
    initMocks(false);
    JChannel channel = messenger.myChannel;
    services.getConfig().getTransport().setOldDSMembershipInfo(new MembershipInformation(channel,
        Collections.singleton(new InternalDistributedMember("localhost", 10000))));
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
    InternalDistributedMember mbr = createAddress(1234);
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
      InternalDistributedMember mbr = createAddress(1234);
      messenger.scheduledMcastSeqnos.put(mbr, new JGroupsMessenger.MessageTracker(30));
      messenger.waitForMessageState(mbr, state);
      fail("expected a GemFireIOException to be thrown");
    } catch (GemFireIOException e) {
      // pass
    }
  }

  @Test
  public void testMulticastTest() throws Exception {
    initMocks(true);
    boolean result = messenger.testMulticast(50);
    // this shouldln't succeed because there's no-one to respond
    assertFalse(result);
    assertFalse(AvailablePort.isPortAvailable(
        services.getConfig().getDistributionConfig().getMcastPort(), AvailablePort.MULTICAST));
  }

  private NetView createView(InternalDistributedMember otherMbr) {
    InternalDistributedMember sender = messenger.getMemberID();
    List<InternalDistributedMember> mbrs = new ArrayList<>();
    mbrs.add(sender);
    mbrs.add(otherMbr);
    NetView v = new NetView(sender, 1, mbrs);
    return v;
  }

  @Test
  public void testEncryptedFindCoordinatorRequest() throws Exception {
    InternalDistributedMember otherMbr = new InternalDistributedMember("localhost", 8888);

    Properties p = new Properties();
    final String udpDhalgo = "AES:128";
    p.put(ConfigurationProperties.SECURITY_UDP_DHALGO, udpDhalgo);
    initMocks(false, p);

    NetView v = createView(otherMbr);
    when(joinLeave.getMemberID(messenger.getMemberID().getNetMember()))
        .thenReturn(messenger.getMemberID());
    GMSEncrypt otherMbrEncrptor = new GMSEncrypt(services, udpDhalgo);

    messenger.setPublicKey(otherMbrEncrptor.getPublicKeyBytes(), otherMbr);
    messenger.initClusterKey();

    FindCoordinatorRequest gfmsg = new FindCoordinatorRequest(messenger.getMemberID(),
        new ArrayList<InternalDistributedMember>(2), 1,
        messenger.getPublicKey(messenger.getMemberID()), 1, "");
    Set<InternalDistributedMember> recipients = new HashSet<>();
    recipients.add(otherMbr);
    gfmsg.setRecipients(recipients);

    short version = Version.CURRENT_ORDINAL;

    HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);

    messenger.writeEncryptedMessage(gfmsg, version, out);

    byte[] requestBytes = out.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    DistributionMessage distributionMessage =
        messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(gfmsg, distributionMessage);
  }

  @Test
  public void testEncryptedFindCoordinatorResponse() throws Exception {
    InternalDistributedMember otherMbr = new InternalDistributedMember("localhost", 8888);

    Properties p = new Properties();

    p.put(ConfigurationProperties.SECURITY_UDP_DHALGO, AES_128);
    initMocks(false, p);

    NetView v = createView(otherMbr);

    GMSEncrypt otherMbrEncrptor = new GMSEncrypt(services, AES_128);
    otherMbrEncrptor.setPublicKey(messenger.getPublicKey(messenger.getMemberID()),
        messenger.getMemberID());

    messenger.setPublicKey(otherMbrEncrptor.getPublicKeyBytes(), otherMbr);
    messenger.initClusterKey();

    FindCoordinatorResponse gfmsg = new FindCoordinatorResponse(messenger.getMemberID(),
        messenger.getMemberID(), messenger.getClusterSecretKey(), 1);
    Set<InternalDistributedMember> recipients = new HashSet<>();
    recipients.add(otherMbr);
    gfmsg.setRecipients(recipients);

    short version = Version.CURRENT_ORDINAL;

    HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);

    messenger.writeEncryptedMessage(gfmsg, version, out);

    byte[] requestBytes = out.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    messenger.addRequestId(1, messenger.getMemberID());

    DistributionMessage distributionMessage =
        messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(gfmsg, distributionMessage);
  }

  @Test
  public void testEncryptedJoinRequest() throws Exception {
    InternalDistributedMember otherMbr = new InternalDistributedMember("localhost", 8888);

    Properties p = new Properties();
    p.put(ConfigurationProperties.SECURITY_UDP_DHALGO, AES_128);
    initMocks(false, p);

    NetView v = createView(otherMbr);

    GMSEncrypt otherMbrEncrptor = new GMSEncrypt(services, AES_128);

    messenger.setPublicKey(otherMbrEncrptor.getPublicKeyBytes(), otherMbr);
    messenger.initClusterKey();

    JoinRequestMessage gfmsg =
        new JoinRequestMessage(otherMbr, messenger.getMemberID(), null, 9789, 1);

    short version = Version.CURRENT_ORDINAL;

    HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);

    messenger.writeEncryptedMessage(gfmsg, version, out);

    byte[] requestBytes = out.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    DistributionMessage distributionMessage =
        messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(gfmsg, distributionMessage);
  }

  @Test
  public void testEncryptedJoinResponse() throws Exception {
    InternalDistributedMember otherMbr = new InternalDistributedMember("localhost", 8888);

    Properties p = new Properties();
    p.put(ConfigurationProperties.SECURITY_UDP_DHALGO, AES_128);
    initMocks(false, p);

    NetView v = createView(otherMbr);

    GMSEncrypt otherMbrEncrptor = new GMSEncrypt(services, AES_128);
    otherMbrEncrptor.setPublicKey(messenger.getPublicKey(messenger.getMemberID()),
        messenger.getMemberID());

    messenger.setPublicKey(otherMbrEncrptor.getPublicKeyBytes(), otherMbr);
    messenger.initClusterKey();

    JoinResponseMessage gfmsg =
        new JoinResponseMessage(otherMbr, messenger.getClusterSecretKey(), 1);

    short version = Version.CURRENT_ORDINAL;

    HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);

    messenger.writeEncryptedMessage(gfmsg, version, out);

    byte[] requestBytes = out.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    messenger.addRequestId(1, messenger.getMemberID());

    DistributionMessage gfMessageAtOtherMbr =
        messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(gfmsg, gfMessageAtOtherMbr);

    // lets send view as well..

    InstallViewMessage installViewMessage = new InstallViewMessage(v, null, true);

    out = new HeapDataOutputStream(Version.CURRENT);

    messenger.writeEncryptedMessage(installViewMessage, version, out);

    requestBytes = out.toByteArray();

    otherMbrEncrptor.setClusterKey(((JoinResponseMessage) gfMessageAtOtherMbr).getSecretPk());

    dis = new DataInputStream(new ByteArrayInputStream(requestBytes));

    gfMessageAtOtherMbr = messenger.readEncryptedMessage(dis, version, otherMbrEncrptor);

    assertEquals(installViewMessage, gfMessageAtOtherMbr);

  }

  /**
   * creates an InternalDistributedMember address that can be used with the doctored JGroups
   * channel. This includes a logical (UUID) address and a physical (IpAddress) address.
   *
   * @param port the UDP port to use for the new address
   */
  private InternalDistributedMember createAddress(int port) {
    GMSMember gms = new GMSMember("localhost", port);
    gms.setUUID(UUID.randomUUID());
    gms.setVmKind(ClusterDistributionManager.NORMAL_DM_TYPE);
    gms.setVersionOrdinal(Version.CURRENT_ORDINAL);
    return new InternalDistributedMember(gms);
  }

}
