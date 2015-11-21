/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal.membership.gms.messenger;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.util.UUID;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.gemstone.gemfire.ForcedDisconnectException;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.SerialAckedMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSMember;
import com.gemstone.gemfire.distributed.internal.membership.gms.ServiceConfig;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services.Stopper;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.JoinLeave;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Manager;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.MessageHandler;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.JoinRequestMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class JGroupsMessengerJUnitTest {
  private Services services;
  private JGroupsMessenger messenger;
  private JoinLeave joinLeave;
  private Manager manager;
  private Stopper stopper;
  private InterceptUDP interceptor;


  /**
   * Create stub and mock objects
   */
  private void initMocks(boolean enableMcast) throws Exception {
    Properties nonDefault = new Properties();
    nonDefault.put(DistributionConfig.DISABLE_TCP_NAME, "true");
    nonDefault.put(DistributionConfig.MCAST_PORT_NAME, enableMcast? ""+AvailablePortHelper.getRandomAvailableUDPPort() : "0");
    nonDefault.put(DistributionConfig.MCAST_TTL_NAME, "0");
    nonDefault.put(DistributionConfig.LOG_FILE_NAME, "");
    nonDefault.put(DistributionConfig.LOG_LEVEL_NAME, "fine");
    nonDefault.put(DistributionConfig.LOCATORS_NAME, "localhost[10344]");
    DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
    RemoteTransportConfig tconfig = new RemoteTransportConfig(config,
        DistributionManager.NORMAL_DM_TYPE);
    
    stopper = mock(Stopper.class);
    when(stopper.isCancelInProgress()).thenReturn(false);
    
    manager = mock(Manager.class);
    when(manager.isMulticastAllowed()).thenReturn(enableMcast);
    
    joinLeave = mock(JoinLeave.class);
    
    ServiceConfig serviceConfig = new ServiceConfig(tconfig, config);
    
    services = mock(Services.class);
    when(services.getConfig()).thenReturn(serviceConfig);
    when(services.getCancelCriterion()).thenReturn(stopper);
    when(services.getManager()).thenReturn(manager);
    when(services.getJoinLeave()).thenReturn(joinLeave);
    when(services.getStatistics()).thenReturn(mock(DMStats.class));
    
    messenger = new JGroupsMessenger();
    messenger.init(services);
    
    String jgroupsConfig = messenger.getJGroupsStackConfig();
    int startIdx = jgroupsConfig.indexOf("<com");
    int insertIdx = jgroupsConfig.indexOf('>', startIdx+4) + 1;
    jgroupsConfig = jgroupsConfig.substring(0, insertIdx) +
        "<"+InterceptUDP.class.getName()+"/>" +
        jgroupsConfig.substring(insertIdx);
    messenger.setJGroupsStackConfigForTesting(jgroupsConfig);
    System.out.println("jgroups config: " + jgroupsConfig);
    
    messenger.start();
    messenger.started();
    
    interceptor = (InterceptUDP)messenger.myChannel
        .getProtocolStack().getTransport().getUpProtocol();
    
  }
  
  @After
  public void stopMessenger() {
    if (messenger != null && messenger.myChannel != null) {
      messenger.stop();
    }
  }
  
  @Test
  public void testMemberWeightIsSerialized() throws Exception {
    HeapDataOutputStream out = new HeapDataOutputStream(500, Version.CURRENT);
    InternalDistributedMember m = new InternalDistributedMember("localhost", 8888);
    ((GMSMember)m.getNetMember()).setMemberWeight((byte)40);
    m.toData(out);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    m = new InternalDistributedMember();
    m.fromData(in);
    assertEquals(40, m.getNetMember().getMemberWeight());
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
    JoinRequestMessage msg = new JoinRequestMessage(messenger.localAddress, sender, null, -1);
    
    Message jmsg = messenger.createJGMessage(msg, messenger.jgAddress, Version.CURRENT_ORDINAL);
    interceptor.up(new Event(Event.MSG, jmsg));
    
    verify(mh, times(1)).processMessage(any(JoinRequestMessage.class));
    
    LeaveRequestMessage lmsg = new LeaveRequestMessage(messenger.localAddress, sender, "testing");
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

  public void doTestBigMessageIsFragmented(boolean mcastEnabled, boolean mcastMsg) throws Exception {
    initMocks(mcastEnabled);
    MessageHandler mh = mock(MessageHandler.class);
    messenger.addHandler(JoinRequestMessage.class, mh);
    
    InternalDistributedMember sender = messenger.getMemberID();
    NetView v = new NetView(sender);
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);
    JoinRequestMessage msg = new JoinRequestMessage(messenger.localAddress, sender, null, -1);
    if (mcastMsg) {
      msg.setMulticast(true);
    }

    messenger.send(msg);
    int sentMessages = (mcastEnabled && mcastMsg) ? interceptor.mcastSentDataMessages
        : interceptor.unicastSentDataMessages;
    assertTrue("expected 1 message to be sent but found "+ sentMessages,
        sentMessages == 1);

    // send a big message and expect fragmentation
    msg = new JoinRequestMessage(messenger.localAddress, sender, new byte[(int)(services.getConfig().getDistributionConfig().getUdpFragmentSize()*(1.5))], -1);

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
    
    assertTrue("expected 2 messages to be sent but found "+ interceptor.unicastSentDataMessages,
        interceptor.unicastSentDataMessages == 2);
    
    List<Message> messages = new ArrayList<>(interceptor.collectedMessages);
    UUID fakeMember = new UUID(50, 50);
    short unicastHeaderId = ClassConfigurator.getProtocolId(UNICAST3.class);
    int seqno = 1;
    for (Message m: messages) {
      m.setSrc(fakeMember);
      UNICAST3.Header oldHeader = (UNICAST3.Header)m.getHeader(unicastHeaderId);
      if (oldHeader == null) continue;
      UNICAST3.Header newHeader = UNICAST3.Header.createDataHeader(seqno, oldHeader.connId(), seqno==1);
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
    InternalDistributedMember other = new InternalDistributedMember("localhost", 8888);

    NetView v = new NetView(sender);
    v.add(other);
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);

    List<InternalDistributedMember> recipients = v.getMembers();
    SerialAckedMessage msg = new SerialAckedMessage();
    msg.setRecipients(recipients);

    messenger.send(msg);
    int sentMessages = interceptor.unicastSentDataMessages;
    assertTrue("expected 2 message to be sent but found "+ sentMessages,
        sentMessages == 2);
  }
  
  @Test
  public void testChannelStillConnectedAfterEmergencyCloseAfterForcedDisconnectWithAutoReconnect() throws Exception {
    initMocks(false);
    Mockito.doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    Mockito.doCallRealMethod().when(services).getShutdownCause();
    Mockito.doCallRealMethod().when(services).emergencyClose();
    Mockito.doCallRealMethod().when(services).isShutdownDueToForcedDisconnect();
    Mockito.doCallRealMethod().when(services).isAutoReconnectEnabled();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertTrue(messenger.myChannel.isConnected());
  }
  
  @Test
  public void testChannelStillConnectedAfterStopAfterForcedDisconnectWithAutoReconnect() throws Exception {
    initMocks(false);
    Mockito.doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    Mockito.doCallRealMethod().when(services).getShutdownCause();
    Mockito.doCallRealMethod().when(services).emergencyClose();
    Mockito.doCallRealMethod().when(services).isShutdownDueToForcedDisconnect();
    Mockito.doCallRealMethod().when(services).isAutoReconnectEnabled();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertTrue(messenger.myChannel.isConnected());
  }
  
  @Test
  public void testChannelStillConnectedAfteremergencyWhileReconnectingDS() throws Exception {
    initMocks(false);
    Mockito.doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    Mockito.doCallRealMethod().when(services).getShutdownCause();
    Mockito.doCallRealMethod().when(services).emergencyClose();
    Mockito.doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    Mockito.doReturn(false).when(services).isAutoReconnectEnabled();
    Mockito.doReturn(true).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertTrue(messenger.myChannel.isConnected());
  }
  
  
  @Test
  public void testChannelStillConnectedAfterStopWhileReconnectingDS() throws Exception {
    initMocks(false);
    Mockito.doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    Mockito.doCallRealMethod().when(services).getShutdownCause();
    Mockito.doCallRealMethod().when(services).emergencyClose();
    Mockito.doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    Mockito.doReturn(false).when(services).isAutoReconnectEnabled();
    Mockito.doReturn(true).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertTrue(messenger.myChannel.isConnected());
  }
  
  @Test
  public void testChannelClosedOnEmergencyClose() throws Exception {
    initMocks(false);
    Mockito.doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    Mockito.doCallRealMethod().when(services).getShutdownCause();
    Mockito.doCallRealMethod().when(services).emergencyClose();
    Mockito.doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    Mockito.doReturn(false).when(services).isAutoReconnectEnabled();
    Mockito.doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertFalse(messenger.myChannel.isConnected());
  }
  
  @Test
  public void testChannelClosedOnStop() throws Exception {
    initMocks(false);
    Mockito.doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    Mockito.doCallRealMethod().when(services).getShutdownCause();
    Mockito.doCallRealMethod().when(services).emergencyClose();
    Mockito.doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    Mockito.doReturn(false).when(services).isAutoReconnectEnabled();
    Mockito.doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertFalse(messenger.myChannel.isConnected());
  }
  
  @Test
  public void testChannelClosedAfterEmergencyCloseForcedDisconnectWithoutAutoReconnect() throws Exception {
    initMocks(false);
    Mockito.doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    Mockito.doCallRealMethod().when(services).getShutdownCause();
    Mockito.doCallRealMethod().when(services).emergencyClose();
    Mockito.doReturn(true).when(services).isShutdownDueToForcedDisconnect();
    Mockito.doReturn(false).when(services).isAutoReconnectEnabled();
    Mockito.doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertFalse(messenger.myChannel.isConnected());
  }
  
  @Test
  public void testChannelStillConnectedStopAfterForcedDisconnectWithoutAutoReconnect() throws Exception {
    initMocks(false);
    Mockito.doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    Mockito.doCallRealMethod().when(services).getShutdownCause();
    Mockito.doCallRealMethod().when(services).emergencyClose();
    Mockito.doReturn(true).when(services).isShutdownDueToForcedDisconnect();
    Mockito.doReturn(false).when(services).isAutoReconnectEnabled();
    Mockito.doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertFalse(messenger.myChannel.isConnected());
  }
  
  @Test
  public void testChannelClosedAfterEmergencyCloseNotForcedDisconnectWithAutoReconnect() throws Exception {
    initMocks(false);
    Mockito.doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    Mockito.doCallRealMethod().when(services).getShutdownCause();
    Mockito.doCallRealMethod().when(services).emergencyClose();
    Mockito.doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    Mockito.doReturn(true).when(services).isAutoReconnectEnabled();
    Mockito.doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.emergencyClose();
    assertFalse(messenger.myChannel.isConnected());
  }
  
  @Test
  public void testChannelStillConnectedStopNotForcedDisconnectWithAutoReconnect() throws Exception {
    initMocks(false);
    Mockito.doCallRealMethod().when(services).setShutdownCause(any(ForcedDisconnectException.class));
    Mockito.doCallRealMethod().when(services).getShutdownCause();
    Mockito.doCallRealMethod().when(services).emergencyClose();
    Mockito.doReturn(false).when(services).isShutdownDueToForcedDisconnect();
    Mockito.doReturn(true).when(services).isAutoReconnectEnabled();
    Mockito.doReturn(false).when(manager).isReconnectingDS();
    services.setShutdownCause(new ForcedDisconnectException("Test Forced Disconnect"));
    assertTrue(messenger.myChannel.isConnected());
    messenger.stop();
    assertFalse(messenger.myChannel.isConnected());
  }
  
  /**
   * creates an InternalDistributedMember address that can be used
   * with the doctored JGroups channel.  This includes a logical
   * (UUID) address and a physical (IpAddress) address.
   * 
   * @param port the UDP port to use for the new address
   */
  private InternalDistributedMember createAddress(int port) {
    GMSMember gms = new GMSMember("localhost", 8888);
    gms.setUUID(UUID.randomUUID());
    gms.setVmKind(DistributionManager.NORMAL_DM_TYPE);
    gms.setVersionOrdinal(Version.CURRENT_ORDINAL);
    return new InternalDistributedMember(gms);
  }
  
}
