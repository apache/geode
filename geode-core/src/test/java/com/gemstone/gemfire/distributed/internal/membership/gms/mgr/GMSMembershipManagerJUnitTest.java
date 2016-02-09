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
package com.gemstone.gemfire.distributed.internal.membership.gms.mgr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;

import org.jgroups.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.HighPriorityAckedMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.MembershipListener;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.direct.DirectChannel;
import com.gemstone.gemfire.distributed.internal.membership.DistributedMembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSMember;
import com.gemstone.gemfire.distributed.internal.membership.gms.ServiceConfig;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services.Stopper;
import com.gemstone.gemfire.distributed.internal.membership.gms.SuspectMember;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Authenticator;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.HealthMonitor;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.JoinLeave;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Messenger;
import com.gemstone.gemfire.distributed.internal.membership.gms.mgr.GMSMembershipManager.StartupEvent;
import com.gemstone.gemfire.internal.admin.remote.AlertListenerMessage;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.internal.tcp.ConnectExceptions;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GMSMembershipManagerJUnitTest {
  private Services services;
  private ServiceConfig mockConfig;
  private DistributionConfig distConfig;
  private Properties distProperties;
  private Authenticator authenticator;
  private HealthMonitor healthMonitor;
  private InternalDistributedMember myMemberId;
  private InternalDistributedMember[] mockMembers;
  private Messenger messenger;
  private JoinLeave joinLeave;
  private Stopper stopper;
  DistributedMembershipListener listener;
  private GMSMembershipManager manager;
  private List<InternalDistributedMember> members;
  private DirectChannel dc;

  @Before
  public void initMocks() throws Exception {
    Properties nonDefault = new Properties();
    nonDefault.put(DistributionConfig.ACK_WAIT_THRESHOLD_NAME, "1");
    nonDefault.put(DistributionConfig.ACK_SEVERE_ALERT_THRESHOLD_NAME, "10");
    nonDefault.put(DistributionConfig.DISABLE_TCP_NAME, "true");
    nonDefault.put(DistributionConfig.MCAST_PORT_NAME, "0");
    nonDefault.put(DistributionConfig.MCAST_TTL_NAME, "0");
    nonDefault.put(DistributionConfig.LOG_FILE_NAME, "");
    nonDefault.put(DistributionConfig.LOG_LEVEL_NAME, "fine");
    nonDefault.put(DistributionConfig.MEMBER_TIMEOUT_NAME, "2000");
    nonDefault.put(DistributionConfig.LOCATORS_NAME, "localhost[10344]");
    distConfig = new DistributionConfigImpl(nonDefault);
    distProperties = nonDefault;
    RemoteTransportConfig tconfig = new RemoteTransportConfig(distConfig,
        DistributionManager.NORMAL_DM_TYPE);
    

    mockConfig = mock(ServiceConfig.class);
    when(mockConfig.getDistributionConfig()).thenReturn(distConfig);
    when(mockConfig.getTransport()).thenReturn(tconfig);
    
    authenticator = mock(Authenticator.class);
    myMemberId = new InternalDistributedMember("localhost", 8887);
    
    messenger = mock(Messenger.class);
    when(messenger.getMemberID()).thenReturn(myMemberId);

    stopper = mock(Stopper.class);
    when(stopper.isCancelInProgress()).thenReturn(false);
    
    healthMonitor = mock(HealthMonitor.class);
    when(healthMonitor.getFailureDetectionPort()).thenReturn(Integer.valueOf(-1));
    
    joinLeave = mock(JoinLeave.class);
    
    services = mock(Services.class);
    when(services.getAuthenticator()).thenReturn(authenticator);
    when(services.getConfig()).thenReturn(mockConfig);
    when(services.getMessenger()).thenReturn(messenger);
    when(services.getCancelCriterion()).thenReturn(stopper);
    when(services.getHealthMonitor()).thenReturn(healthMonitor);
    when(services.getJoinLeave()).thenReturn(joinLeave);
    
    Timer t = new Timer(true);
    when(services.getTimer()).thenReturn(t);
    
    Random r = new Random();
    mockMembers = new InternalDistributedMember[5];
    for (int i = 0; i < mockMembers.length; i++) {
      mockMembers[i] = new InternalDistributedMember("localhost", 8888 + i);
      GMSMember m = (GMSMember)mockMembers[i].getNetMember();
      UUID uuid = new UUID(r.nextLong(), r.nextLong());
      m.setUUID(uuid);
    }
    members = new ArrayList<>(Arrays.asList(mockMembers));

    listener = mock(DistributedMembershipListener.class);
    
    manager = new GMSMembershipManager(listener);
    manager.init(services);
    when(services.getManager()).thenReturn(manager);
  }
  
  @After
  public void tearDown() throws Exception {
    if (manager != null) {
      manager.stop();
      manager.stopped();
    }
  }
  
  @Test
  public void testSendMessage() throws Exception {
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    m.setRecipient(mockMembers[0]);
    manager.start();
    manager.started();
    manager.installView(new NetView(myMemberId, 1, members));
    Set<InternalDistributedMember> failures = manager.send(m);
    verify(messenger).send(m);
    if (failures != null) {
      assertEquals(0, failures.size());
    }
  }
  
  @Test
  public void testSendAdminMessageFailsDuringShutdown() throws Exception {
    AlertListenerMessage m = AlertListenerMessage.create(mockMembers[0], 1, 
       new Date(System.currentTimeMillis()), "thread", "", 1L, "", "");
    manager.start();
    manager.started();
    manager.installView(new NetView(myMemberId, 1, members));
    manager.setShutdown();
    Set<InternalDistributedMember> failures = manager.send(m);
    verify(messenger, never()).send(m);
    assertEquals(1, failures.size());
    assertEquals(mockMembers[0], failures.iterator().next());
  }
  
  @Test
  public void testSendToEmptyListIsRejected() throws Exception {
    InternalDistributedMember[] emptyList = new InternalDistributedMember[0];
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    m.setRecipient(mockMembers[0]);
    manager.start();
    manager.started();
    manager.installView(new NetView(myMemberId, 1, members));
    Set<InternalDistributedMember> failures = manager.send(null, m, null);
    verify(messenger, never()).send(m);
    reset(messenger);
    failures = manager.send(emptyList, m, null);
    verify(messenger, never()).send(m);
  }
  
  @Test
  public void testStartupEvents() throws Exception {
    manager.start();
    manager.started();
    manager.isJoining = true;

    List<InternalDistributedMember> viewmembers = Arrays.asList(new InternalDistributedMember[] {mockMembers[0], myMemberId});
    manager.installView(new NetView(myMemberId, 2, viewmembers));

    // add a surprise member that will be shunned due to it's having
    // an old view ID
    InternalDistributedMember surpriseMember = mockMembers[2];
    surpriseMember.setVmViewId(1);
    manager.handleOrDeferSurpriseConnect(surpriseMember);
    assertEquals(1, manager.getStartupEvents().size());

    // add a surprise member that will be accepted
    InternalDistributedMember surpriseMember2 = mockMembers[3];
    surpriseMember2.setVmViewId(3);
    manager.handleOrDeferSurpriseConnect(surpriseMember2);
    assertEquals(2, manager.getStartupEvents().size());

    // suspect a member
    InternalDistributedMember suspectMember = mockMembers[1];
    manager.handleOrDeferSuspect(new SuspectMember(mockMembers[0], suspectMember, "testing"));
    // suspect messages aren't queued - they're ignored before joining the system
    assertEquals(2, manager.getStartupEvents().size());
    verify(listener, never()).memberSuspect(suspectMember, mockMembers[0], "testing");

    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    mockMembers[0].setVmViewId(1);
    m.setRecipient(mockMembers[0]);
    m.setSender(mockMembers[1]);
    manager.handleOrDeferMessage(m);
    assertEquals(3, manager.getStartupEvents().size());

    // this view officially adds surpriseMember2
    viewmembers = Arrays.asList(
        new InternalDistributedMember[] {mockMembers[0], myMemberId, surpriseMember2});
    manager.handleOrDeferViewEvent(new NetView(myMemberId, 3, viewmembers));
    assertEquals(4, manager.getStartupEvents().size());
    
    // add a surprise member that will be shunned due to it's having
    // an old view ID
    InternalDistributedMember surpriseMember3 = mockMembers[4];
    surpriseMember.setVmViewId(1);
    manager.handleOrDeferSurpriseConnect(surpriseMember);
    assertEquals(5, manager.getStartupEvents().size());

    
    // process a new view after we finish joining but before event processing has started
    manager.isJoining = false;
    mockMembers[4].setVmViewId(4);
    viewmembers = Arrays.asList(new InternalDistributedMember[] {mockMembers[0], myMemberId, surpriseMember2, mockMembers[4]});
    manager.handleOrDeferViewEvent(new NetView(myMemberId, 4, viewmembers));
    assertEquals(6, manager.getStartupEvents().size());
    
    // exercise the toString methods for code coverage
    for (StartupEvent ev: manager.getStartupEvents()) {
      ev.toString();
    }
    
    manager.startEventProcessing();

    // all startup events should have been processed 
    assertEquals(0, manager.getStartupEvents().size());
    // the new view should have been installed
    assertEquals(4, manager.getView().getViewId());
    // supriseMember2 should have been announced
    verify(listener).newMemberConnected(surpriseMember2);
    // supriseMember should have been rejected (old view ID)
    verify(listener, never()).newMemberConnected(surpriseMember);
    
    // for code coverage also install a view after we finish joining but before
    // event processing has started.  This should notify the distribution manager
    // with a LocalViewMessage to process the view
    reset(listener);
    manager.handleOrDeferViewEvent(new NetView(myMemberId, 5, viewmembers));
    assertEquals(0, manager.getStartupEvents().size());
    verify(listener).messageReceived(isA(LocalViewMessage.class));

    // process a suspect now - it will be passed to the listener
    reset(listener);
    suspectMember = mockMembers[1];
    manager.handleOrDeferSuspect(new SuspectMember(mockMembers[0], suspectMember, "testing"));
    verify(listener).memberSuspect(suspectMember, mockMembers[0], "testing");
  }
  
  /**
   * Some tests require a DirectChannel mock
   */
  private void setUpDirectChannelMock() throws Exception {
    dc = mock(DirectChannel.class);
    when(dc.send(any(GMSMembershipManager.class), any(mockMembers.getClass()), any(DistributionMessage.class), anyInt(), anyInt()))
      .thenReturn(100);

    manager.start();
    manager.started();
    
    manager.setDirectChannel(dc);

    NetView view = new NetView(myMemberId, 1, members);
    manager.installView(view);
    when(joinLeave.getView()).thenReturn(view);
    
    manager.startEventProcessing();
  }

  @Test
  public void testDirectChannelSend() throws Exception {
    setUpDirectChannelMock();
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    InternalDistributedMember[] recipients = new InternalDistributedMember[] {mockMembers[2], mockMembers[3]};
    m.setRecipients(Arrays.asList(recipients));
    Set<InternalDistributedMember> failures = manager.directChannelSend(recipients, m, null);
    assertTrue(failures == null);
    verify(dc).send(isA(GMSMembershipManager.class), isA(mockMembers.getClass()), isA(DistributionMessage.class), anyInt(), anyInt());
  }
  
  @Test
  public void testDirectChannelSendFailureToOneRecipient() throws Exception {
    setUpDirectChannelMock();
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    InternalDistributedMember[] recipients = new InternalDistributedMember[] {mockMembers[2], mockMembers[3]};
    m.setRecipients(Arrays.asList(recipients));
    Set<InternalDistributedMember> failures = manager.directChannelSend(recipients, m, null);

    ConnectExceptions exception = new ConnectExceptions();
    exception.addFailure(recipients[0], new Exception("testing"));
    when(dc.send(any(GMSMembershipManager.class), any(mockMembers.getClass()), any(DistributionMessage.class), anyInt(), anyInt()))
      .thenThrow(exception);
    failures = manager.directChannelSend(recipients, m, null);
    assertTrue(failures != null);
    assertEquals(1, failures.size());
    assertEquals(recipients[0], failures.iterator().next()); 
  }
  
  @Test
  public void testDirectChannelSendFailureToAll() throws Exception {
    setUpDirectChannelMock();
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    InternalDistributedMember[] recipients = new InternalDistributedMember[] {mockMembers[2], mockMembers[3]};
    m.setRecipients(Arrays.asList(recipients));
    Set<InternalDistributedMember> failures = manager.directChannelSend(recipients, m, null);
    when(dc.send(any(GMSMembershipManager.class), any(mockMembers.getClass()), any(DistributionMessage.class), anyInt(), anyInt()))
      .thenReturn(0);
    when(stopper.cancelInProgress()).thenReturn("stopping for test");
    try {
      manager.directChannelSend(recipients, m, null);
      throw new RuntimeException("expected directChannelSend to throw an exception");
    } catch (DistributedSystemDisconnectedException e) {
      // expected
    }
  }
  
  @Test
  public void testDirectChannelSendAllRecipients() throws Exception {
    setUpDirectChannelMock();
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    m.setRecipient(DistributionMessage.ALL_RECIPIENTS);
    assertTrue(m.forAll());
    Set<InternalDistributedMember> failures = manager.directChannelSend(null, m, null);
    assertTrue(failures == null);
    verify(dc).send(isA(GMSMembershipManager.class), isA(mockMembers.getClass()), isA(DistributionMessage.class), anyInt(), anyInt());
  }
  
  /**
   * This test ensures that the membership manager can accept an ID that
   * does not have a UUID and replace it with one that does have a UUID
   * from the current membership view.
   */
  @Test
  public void testAddressesWithoutUUIDs() throws Exception {
    manager.start();
    manager.started();
    manager.isJoining = true;

    List<InternalDistributedMember> viewmembers = Arrays.asList(new InternalDistributedMember[] {mockMembers[0], mockMembers[1], myMemberId});
    manager.installView(new NetView(myMemberId, 2, viewmembers));
    
    InternalDistributedMember[] destinations = new InternalDistributedMember[viewmembers.size()];
    for (int i=0; i<destinations.length; i++) {
      InternalDistributedMember id = viewmembers.get(i);
      destinations[i] = new InternalDistributedMember(id.getHost(), id.getPort());
    }
    manager.checkAddressesForUUIDs(destinations);
    // each destination w/o a UUID should have been replaced with the corresponding
    // ID from the membership view
    for (int i=0; i<destinations.length; i++) {
      assertTrue(viewmembers.get(i) == destinations[i]);
    }
  }
  
  @Test
  public void testReplyProcessorInitiatesSuspicion() throws Exception {
    DM dm = mock(DM.class);
    DMStats stats = mock(DMStats.class);
    
    InternalDistributedSystem system = InternalDistributedSystem.newInstanceForTesting(dm, distProperties);

    when(dm.getStats()).thenReturn(stats);
    when(dm.getSystem()).thenReturn(system);
    when(dm.getCancelCriterion()).thenReturn(stopper);
    when(dm.getMembershipManager()).thenReturn(manager);
    when(dm.getViewMembers()).thenReturn(members);
    when(dm.getDistributionManagerIds()).thenReturn(new HashSet(members));
    when(dm.addMembershipListenerAndGetDistributionManagerIds(any(MembershipListener.class))).thenReturn(new HashSet(members));
    
    manager.start();
    manager.started();
    manager.isJoining = true;

    List<InternalDistributedMember> viewmembers = Arrays.asList(new InternalDistributedMember[] {mockMembers[0], mockMembers[1], myMemberId});
    manager.installView(new NetView(myMemberId, 2, viewmembers));

    List<InternalDistributedMember> mbrs = new ArrayList<>(1);
    mbrs.add(mockMembers[0]);
    ReplyProcessor21 rp = new ReplyProcessor21(dm, mbrs);
    rp.enableSevereAlertProcessing();
    boolean result = rp.waitForReplies(2000);
    assertFalse(result);  // the wait should have timed out
    verify(healthMonitor, atLeastOnce()).checkIfAvailable(isA(InternalDistributedMember.class), isA(String.class), isA(Boolean.class));
  }
  
}

