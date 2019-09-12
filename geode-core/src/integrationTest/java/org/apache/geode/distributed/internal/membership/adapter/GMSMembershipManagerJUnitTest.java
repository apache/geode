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
package org.apache.geode.distributed.internal.membership.adapter;

import static org.apache.geode.distributed.ConfigurationProperties.ACK_SEVERE_ALERT_THRESHOLD;
import static org.apache.geode.distributed.ConfigurationProperties.ACK_WAIT_THRESHOLD;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_TCP;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_TTL;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.jgroups.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.HighPriorityAckedMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.direct.DirectChannel;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipView;
import org.apache.geode.distributed.internal.membership.adapter.GMSMembershipManager.StartupEvent;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.Services.Stopper;
import org.apache.geode.distributed.internal.membership.gms.SuspectMember;
import org.apache.geode.distributed.internal.membership.gms.api.Authenticator;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipListener;
import org.apache.geode.distributed.internal.membership.gms.api.MessageListener;
import org.apache.geode.distributed.internal.membership.gms.interfaces.GMSMessage;
import org.apache.geode.distributed.internal.membership.gms.interfaces.HealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Messenger;
import org.apache.geode.internal.admin.remote.AlertListenerMessage;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.statistics.DummyStatisticsRegistry;
import org.apache.geode.internal.tcp.ConnectExceptions;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class GMSMembershipManagerJUnitTest {

  private static final long WAIT_FOR_REPLIES_MILLIS = 2000;

  private Services services;
  private MembershipConfig mockConfig;
  private DistributionConfig distConfig;
  private Properties distProperties;
  private Authenticator authenticator;
  private HealthMonitor healthMonitor;
  private InternalDistributedMember myMemberId;
  private InternalDistributedMember[] mockMembers;
  private Messenger messenger;
  private JoinLeave joinLeave;
  private Stopper stopper;
  private MembershipListener listener;
  private GMSMembershipManager manager;
  private List<InternalDistributedMember> members;
  private DirectChannel dc;
  private MessageListener messageListener;

  @Before
  public void initMocks() throws Exception {
    Properties nonDefault = new Properties();
    nonDefault.put(ACK_WAIT_THRESHOLD, "1");
    nonDefault.put(ACK_SEVERE_ALERT_THRESHOLD, "10");
    nonDefault.put(DISABLE_TCP, "true");
    nonDefault.put(MCAST_PORT, "0");
    nonDefault.put(MCAST_TTL, "0");
    nonDefault.put(LOG_FILE, "");
    nonDefault.put(LOG_LEVEL, "fine");
    nonDefault.put(MEMBER_TIMEOUT, "2000");
    nonDefault.put(LOCATORS, "localhost[10344]");
    distConfig = new DistributionConfigImpl(nonDefault);
    distProperties = nonDefault;
    RemoteTransportConfig tconfig =
        new RemoteTransportConfig(distConfig, ClusterDistributionManager.NORMAL_DM_TYPE);

    mockConfig = new ServiceConfig(tconfig, distConfig);

    authenticator = mock(Authenticator.class);
    myMemberId = new InternalDistributedMember("localhost", 8887);
    GMSMember m = ((GMSMemberAdapter) myMemberId.getNetMember()).getGmsMember();
    UUID uuid = new UUID(12345, 12345);
    m.setUUID(uuid);

    messenger = mock(Messenger.class);
    when(messenger.getMemberID())
        .thenReturn(((GMSMemberAdapter) myMemberId.getNetMember()).getGmsMember());

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
      m = ((GMSMemberAdapter) mockMembers[i].getNetMember()).getGmsMember();
      uuid = new UUID(r.nextLong(), r.nextLong());
      m.setUUID(uuid);
    }
    members = new ArrayList<>(Arrays.asList(mockMembers));

    listener = mock(MembershipListener.class);
    messageListener = mock(MessageListener.class);
    manager = new GMSMembershipManager(listener, messageListener, null);
    manager.getGMSManager().init(services);
    when(services.getManager()).thenReturn(manager.getGMSManager());
  }

  @After
  public void tearDown() throws Exception {
    if (manager != null) {
      manager.getGMSManager().stop();
      manager.getGMSManager().stopped();
    }
  }

  @Test
  public void testSendMessage() throws Exception {
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    m.setRecipient(mockMembers[0]);
    manager.getGMSManager().start();
    manager.getGMSManager().started();
    GMSMember myGMSMemberId = ((GMSMemberAdapter) myMemberId.getNetMember()).getGmsMember();
    List<GMSMember> gmsMembers =
        members.stream().map(x -> ((GMSMemberAdapter) x.getNetMember()).getGmsMember()).collect(
            Collectors.toList());
    manager.getGMSManager().installView(new GMSMembershipView(myGMSMemberId, 1, gmsMembers));
    Set<InternalDistributedMember> failures =
        manager.send(m.getRecipients(), m);
    verify(messenger).send(isA(GMSMessageAdapter.class));
    if (failures != null) {
      assertEquals(0, failures.size());
    }
  }



  private GMSMembershipView createView(InternalDistributedMember creator, int viewId,
      List<InternalDistributedMember> members) {
    List<GMSMember> gmsMembers = new ArrayList<>(members.size());
    for (InternalDistributedMember member : members) {
      gmsMembers.add(((GMSMemberAdapter) member.getNetMember()).getGmsMember());
    }
    return new GMSMembershipView(((GMSMemberAdapter) creator.getNetMember()).getGmsMember(), viewId,
        gmsMembers);
  }

  @Test
  public void testSendAdminMessageFailsDuringShutdown() throws Exception {
    AlertListenerMessage m = AlertListenerMessage.create(mockMembers[0], 1,
        new Date(System.currentTimeMillis()), "thread", "", 1L, "", "");
    manager.getGMSManager().start();
    manager.getGMSManager().started();
    manager.getGMSManager().installView(createView(myMemberId, 1, members));
    manager.setShutdown();
    Set<InternalDistributedMember> failures =
        manager.send(new InternalDistributedMember[] {mockMembers[0]}, m);
    verify(messenger, never()).send(isA(GMSMessage.class));
    assertEquals(1, failures.size());
    assertEquals(mockMembers[0], failures.iterator().next());
  }

  @Test
  public void testSendToEmptyListIsRejected() throws Exception {
    InternalDistributedMember[] emptyList = new InternalDistributedMember[0];
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    m.setRecipient(mockMembers[0]);
    manager.getGMSManager().start();
    manager.getGMSManager().started();
    manager.getGMSManager().installView(createView(myMemberId, 1, members));
    Set<InternalDistributedMember> failures = manager.send(null, m);
    verify(messenger, never()).send(isA(GMSMessage.class));
    reset(messenger);
    failures = manager.send(emptyList, m);
    verify(messenger, never()).send(isA(GMSMessage.class));
  }

  @Test
  public void testStartupEvents() throws Exception {
    manager.getGMSManager().start();
    manager.getGMSManager().started();
    manager.isJoining = true;

    List<InternalDistributedMember> viewmembers =
        Arrays.asList(new InternalDistributedMember[] {mockMembers[0], myMemberId});
    manager.getGMSManager().installView(createView(myMemberId, 2, viewmembers));

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
    manager.handleOrDeferSuspect(
        new SuspectMember(((GMSMemberAdapter) mockMembers[0].getNetMember()).getGmsMember(),
            ((GMSMemberAdapter) suspectMember.getNetMember()).getGmsMember(), "testing"));
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
    viewmembers = Arrays
        .asList(new InternalDistributedMember[] {mockMembers[0], myMemberId, surpriseMember2});
    manager.handleOrDeferViewEvent(new MembershipView(myMemberId, 3, viewmembers));
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
    viewmembers = Arrays.asList(new InternalDistributedMember[] {mockMembers[0], myMemberId,
        surpriseMember2, mockMembers[4]});
    manager.handleOrDeferViewEvent(new MembershipView(myMemberId, 4, viewmembers));
    assertEquals(6, manager.getStartupEvents().size());

    // exercise the toString methods for code coverage
    for (StartupEvent ev : manager.getStartupEvents()) {
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
    // event processing has started. This should notify the distribution manager
    // with a LocalViewMessage to process the view
    reset(listener);
    manager.handleOrDeferViewEvent(new MembershipView(myMemberId, 5, viewmembers));
    assertEquals(0, manager.getStartupEvents().size());
    verify(messageListener).messageReceived(isA(LocalViewMessage.class));

    // process a suspect now - it will be passed to the listener
    reset(listener);
    suspectMember = mockMembers[1];
    manager.handleOrDeferSuspect(
        new SuspectMember(((GMSMemberAdapter) mockMembers[0].getNetMember()).getGmsMember(),
            ((GMSMemberAdapter) suspectMember.getNetMember()).getGmsMember(), "testing"));
    verify(listener).memberSuspect(suspectMember, mockMembers[0], "testing");
  }

  @Test
  public void testDirectChannelSend() throws Exception {
    setUpDirectChannelMock();
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    InternalDistributedMember[] recipients =
        new InternalDistributedMember[] {mockMembers[2], mockMembers[3]};
    m.setRecipients(Arrays.asList(recipients));
    Set<InternalDistributedMember> failures = manager.directChannelSend(recipients, m);
    assertTrue(failures == null);
    verify(dc).send(isA(GMSMembershipManager.class), isA(mockMembers.getClass()),
        isA(DistributionMessage.class), anyLong(), anyLong());
  }

  @Test
  public void testDirectChannelSendFailureToOneRecipient() throws Exception {
    setUpDirectChannelMock();
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    InternalDistributedMember[] recipients =
        new InternalDistributedMember[] {mockMembers[2], mockMembers[3]};
    m.setRecipients(Arrays.asList(recipients));
    Set<InternalDistributedMember> failures = manager.directChannelSend(recipients, m);

    ConnectExceptions exception = new ConnectExceptions();
    exception.addFailure(recipients[0], new Exception("testing"));
    when(dc.send(any(GMSMembershipManager.class), any(mockMembers.getClass()),
        any(DistributionMessage.class), anyLong(), anyLong())).thenThrow(exception);
    failures = manager.directChannelSend(recipients, m);
    assertTrue(failures != null);
    assertEquals(1, failures.size());
    assertEquals(recipients[0], failures.iterator().next());
  }

  @Test
  public void testDirectChannelSendFailureToAll() throws Exception {
    setUpDirectChannelMock();
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    InternalDistributedMember[] recipients =
        new InternalDistributedMember[] {mockMembers[2], mockMembers[3]};
    m.setRecipients(Arrays.asList(recipients));
    Set<InternalDistributedMember> failures = manager.directChannelSend(recipients, m);
    when(dc.send(any(GMSMembershipManager.class), any(mockMembers.getClass()),
        any(DistributionMessage.class), anyInt(), anyInt())).thenReturn(0);
    when(stopper.isCancelInProgress()).thenReturn(Boolean.TRUE);
    try {
      manager.directChannelSend(recipients, m);
      fail("expected directChannelSend to throw an exception");
    } catch (DistributedSystemDisconnectedException expected) {
    }
  }

  @Test
  public void testDirectChannelSendAllRecipients() throws Exception {
    setUpDirectChannelMock();
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    m.setRecipient(DistributionMessage.ALL_RECIPIENTS);
    assertTrue(m.forAll());
    Set<InternalDistributedMember> failures = manager.directChannelSend(null, m);
    assertTrue(failures == null);
    verify(dc).send(isA(GMSMembershipManager.class), isA(mockMembers.getClass()),
        isA(DistributionMessage.class), anyLong(), anyLong());
  }

  @Test
  public void testDirectChannelSendFailureDueToForcedDisconnect() throws Exception {
    setUpDirectChannelMock();
    HighPriorityAckedMessage m = new HighPriorityAckedMessage();
    InternalDistributedMember[] recipients =
        new InternalDistributedMember[] {mockMembers[2], mockMembers[3]};
    m.setRecipients(Arrays.asList(recipients));
    Set<InternalDistributedMember> failures = manager.directChannelSend(recipients, m);
    manager.setShutdown();
    ConnectExceptions exception = new ConnectExceptions();
    exception.addFailure(recipients[0], new Exception("testing"));
    when(dc.send(any(GMSMembershipManager.class), any(mockMembers.getClass()),
        any(DistributionMessage.class), anyLong(), anyLong())).thenThrow(exception);
    Assertions.assertThatThrownBy(() -> {
      manager.directChannelSend(recipients, m);
    }).isInstanceOf(DistributedSystemDisconnectedException.class);
  }

  /**
   * This test ensures that the membership manager can accept an ID that does not have a UUID and
   * replace it with one that does have a UUID from the current membership view.
   */
  @Test
  public void testAddressesWithoutUUIDs() throws Exception {
    manager.getGMSManager().start();
    manager.getGMSManager().started();
    manager.isJoining = true;

    List<InternalDistributedMember> viewmembers =
        Arrays.asList(new InternalDistributedMember[] {mockMembers[0], mockMembers[1], myMemberId});
    GMSMembershipView view = createView(myMemberId, 2, viewmembers);
    manager.getGMSManager().installView(view);
    when(services.getJoinLeave().getView()).thenReturn(view);

    InternalDistributedMember[] destinations = new InternalDistributedMember[viewmembers.size()];
    for (int i = 0; i < destinations.length; i++) {
      InternalDistributedMember id = viewmembers.get(i);
      destinations[i] = new InternalDistributedMember(id.getHost(), id.getPort());
    }
    manager.checkAddressesForUUIDs(destinations);
    // each destination w/o a UUID should have been replaced with the corresponding
    // ID from the membership view
    for (int i = 0; i < destinations.length; i++) {
      assertTrue(((GMSMemberAdapter) destinations[i].getNetMember()).getGmsMember().hasUUID());
    }
  }

  @Test
  public void testReplyProcessorInitiatesSuspicion() throws Exception {
    DistributionManager dm = mock(DistributionManager.class);
    DMStats stats = mock(DMStats.class);

    InternalDistributedSystem system =
        new InternalDistributedSystem.BuilderForTesting(distProperties)
            .setDistributionManager(dm)
            .setStatisticsManagerFactory(
                (name, startTime, statsDisabled) -> new DummyStatisticsRegistry(name, startTime))
            .build();

    when(dm.getStats()).thenReturn(stats);
    when(dm.getSystem()).thenReturn(system);
    when(dm.getCancelCriterion()).thenReturn(stopper);
    when(dm.getMembershipManager()).thenReturn(manager);
    when(dm.getViewMembers()).thenReturn(members);
    when(dm.getDistributionManagerIds()).thenReturn(members);
    when(dm.addMembershipListenerAndGetDistributionManagerIds(any(
        org.apache.geode.distributed.internal.MembershipListener.class)))
            .thenReturn(members);

    manager.getGMSManager().start();
    manager.getGMSManager().started();
    manager.isJoining = true;

    List<InternalDistributedMember> viewmembers =
        Arrays.asList(new InternalDistributedMember[] {mockMembers[0], mockMembers[1], myMemberId});
    manager.getGMSManager().installView(createView(myMemberId, 2, viewmembers));

    List<InternalDistributedMember> mbrs = new ArrayList<>(1);
    mbrs.add(mockMembers[0]);
    ReplyProcessor21 rp = new ReplyProcessor21(dm, mbrs);
    rp.enableSevereAlertProcessing();
    boolean result = rp.waitForReplies(WAIT_FOR_REPLIES_MILLIS);
    assertFalse(result); // the wait should have timed out
    verify(healthMonitor, atLeastOnce()).checkIfAvailable(isA(GMSMember.class),
        isA(String.class), isA(Boolean.class));
  }

  /**
   * Some tests require a DirectChannel mock
   */
  private void setUpDirectChannelMock() throws Exception {
    dc = mock(DirectChannel.class);
    when(dc.send(any(GMSMembershipManager.class), any(mockMembers.getClass()),
        any(DistributionMessage.class), anyInt(), anyInt())).thenReturn(100);

    manager.getGMSManager().start();
    manager.getGMSManager().started();

    manager.setDirectChannel(dc);

    GMSMembershipView view = createView(myMemberId, 1, members);
    manager.getGMSManager().installView(view);
    when(joinLeave.getView()).thenReturn(view);

    manager.startEventProcessing();
  }

}
