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
package org.apache.geode.distributed.internal.membership.gms.membership;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.internal.verification.Times;
import org.mockito.verification.Timeout;

import org.apache.geode.distributed.internal.membership.api.Authenticator;
import org.apache.geode.distributed.internal.membership.api.MemberDataBuilder;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactoryImpl;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.GMSUtil;
import org.apache.geode.distributed.internal.membership.gms.MemberIdentifierImpl;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.Services.Stopper;
import org.apache.geode.distributed.internal.membership.gms.interfaces.HealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Locator;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Messenger;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.SearchState;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.ViewCreator;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.ViewReplyProcessor;
import org.apache.geode.distributed.internal.membership.gms.messages.InstallViewMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinResponseMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.NetworkPartitionMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.RemoveMemberMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.ViewAckMessage;
import org.apache.geode.distributed.internal.membership.gms.util.MemberIdentifierUtil;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class GMSJoinLeaveJUnitTest {
  private Services services;
  private MembershipConfig mockConfig;
  private Authenticator authenticator;
  private HealthMonitor healthMonitor;
  private MemberIdentifier gmsJoinLeaveMemberId;
  private MemberIdentifier[] mockMembers;
  private MemberIdentifier mockOldMember;
  private Properties credentials = new Properties();
  private Messenger messenger;
  private GMSJoinLeave gmsJoinLeave;
  private Manager manager;
  private Stopper stopper;
  private TestLocator testLocator;
  private MemberIdentifier removeMember = null;
  private MemberIdentifier leaveMember = null;
  private TcpClient locatorClient;

  public void initMocks() throws Exception {
    initMocks(false);
  }

  public void initMocks(boolean enableNetworkPartition) throws Exception {
    initMocks(enableNetworkPartition, false);
  }

  public void initMocks(boolean enableNetworkPartition, boolean useTestGMSJoinLeave)
      throws Exception {
    String locator = "localhost[12345]";
    initMocks(enableNetworkPartition, useTestGMSJoinLeave, locator, locator);
  }

  public void initMocks(boolean enableNetworkPartition, boolean useTestGMSJoinLeave,
      String locators, String startLocator)
      throws Exception {
    mockConfig = mock(MembershipConfig.class);
    when(mockConfig.isNetworkPartitionDetectionEnabled()).thenReturn(enableNetworkPartition);
    when(mockConfig.getSecurityUDPDHAlgo()).thenReturn("");
    when(mockConfig.getStartLocator()).thenReturn(startLocator);
    when(mockConfig.getLocators()).thenReturn(locators);
    when(mockConfig.getMcastPort()).thenReturn(0);
    when(mockConfig.getMemberTimeout()).thenReturn(2000L);

    authenticator = mock(Authenticator.class);

    stopper = mock(Stopper.class);
    when(stopper.isCancelInProgress()).thenReturn(false);

    manager = mock(Manager.class);

    healthMonitor = mock(HealthMonitor.class);
    when(healthMonitor.getFailureDetectionPort()).thenReturn(Integer.valueOf(-1));

    services = mock(Services.class);
    when(services.getAuthenticator()).thenReturn(authenticator);
    when(services.getConfig()).thenReturn(mockConfig);
    when(services.getCancelCriterion()).thenReturn(stopper);
    when(services.getManager()).thenReturn(manager);
    when(services.getHealthMonitor()).thenReturn(healthMonitor);
    when(services.getMemberFactory())
        .thenReturn(new MemberIdentifierFactoryImpl());

    gmsJoinLeaveMemberId = services.getMemberFactory().create(
        MemberDataBuilder.newBuilderForLocalHost("localhost")
            .setMembershipPort(8887).build());


    messenger = mock(Messenger.class);
    when(messenger.getMemberID()).thenReturn(gmsJoinLeaveMemberId);
    when(services.getMessenger()).thenReturn(messenger);

    testLocator = new TestLocator();
    when(services.getLocator()).thenReturn(testLocator);

    Timer t = new Timer(true);
    when(services.getTimer()).thenReturn(t);

    mockMembers = new MemberIdentifier[4];
    for (int i = 0; i < mockMembers.length; i++) {
      mockMembers[i] = services.getMemberFactory().create(
          MemberDataBuilder.newBuilderForLocalHost("localhost")
              .setMembershipPort(8888 + i).build());
    }
    mockOldMember = services.getMemberFactory().create(
        MemberDataBuilder.newBuilderForLocalHost("localhost")
            .setMembershipPort(8700).build());
    ((MemberIdentifierImpl) mockOldMember).setVersionObjectForTest(Version.GFE_56);
    locatorClient = mock(TcpClient.class);

    if (useTestGMSJoinLeave) {
      gmsJoinLeave = new GMSJoinLeaveTest(locatorClient);
    } else {
      gmsJoinLeave = new GMSJoinLeave(locatorClient);
    }
    gmsJoinLeave.init(services);
    gmsJoinLeave.start();
    gmsJoinLeave.started();
    gmsJoinLeave.setLocalAddress(gmsJoinLeaveMemberId);
  }

  @After
  public void tearDown() throws Exception {
    if (gmsJoinLeave != null) {
      gmsJoinLeave.stop();
      gmsJoinLeave.stopped();
    }
  }

  static class TestLocator implements Locator {

    boolean isCoordinator;

    @Override
    public void installView(GMSMembershipView v) {}

    @Override
    public void setIsCoordinator(boolean isCoordinator) {
      this.isCoordinator = isCoordinator;
    }

    public boolean isCoordinator() {
      return isCoordinator;
    }

  }

  @Test
  public void testFindCoordinatorPausesWhenLocatorWaitTimeIsSet() throws Exception {
    initMocks(false);
    when(mockConfig.getLocatorWaitTime()).thenReturn(15000);

    when(locatorClient.requestToServer(isA(HostAndPort.class),
        isA(FindCoordinatorRequest.class), anyInt(), anyBoolean()))
            .thenThrow(new IOException("Connection refused"));

    // interrupt this thread so that findCoordinator() won't keep looping
    // and will throw an exception when going to pause
    Thread.currentThread().interrupt();
    assertThatThrownBy(() -> gmsJoinLeave.findCoordinator())
        .isInstanceOf(MemberStartupException.class)
        .hasMessageContaining("Interrupted while trying to contact locators");
    assertThat(Thread.currentThread().interrupted()).isTrue();
    verify(locatorClient, times(1)).requestToServer(isA(HostAndPort.class),
        isA(FindCoordinatorRequest.class), anyInt(), anyBoolean());
  }

  @Test
  public void testFindCoordinatorInView() throws Exception {
    initMocks();

    int viewId = 1;
    List<MemberIdentifier> mbrs = new LinkedList<>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);

    // prepare the view
    GMSMembershipView netView = new GMSMembershipView(mockMembers[0], viewId, mbrs);
    SearchState state = gmsJoinLeave.searchState;
    state.view = netView;
    state.viewId = netView.getViewId();

    MemberIdentifier coordinator = mockMembers[2];
    coordinator.setVmViewId(viewId);

    // already tried joining using members 0 and 1
    Set<MemberIdentifier> set = new HashSet<>();
    mockMembers[0].setVmViewId(viewId - 1);
    set.add(mockMembers[0]);
    mockMembers[1].setVmViewId(viewId - 1);
    set.add(mockMembers[1]);
    state.alreadyTried = set;
    state.hasContactedAJoinedLocator = true;

    // simulate a response being received
    MemberIdentifier sender = mockMembers[2];
    FindCoordinatorResponse resp = new FindCoordinatorResponse(coordinator, sender, null, 0);
    gmsJoinLeave.processMessage(resp);
    // tell GMSJoinLeave that a unit test is running so it won't clear the
    // responses collection
    gmsJoinLeave.unitTesting.add("findCoordinatorFromView");

    // now for the test
    boolean result = gmsJoinLeave.findCoordinatorFromView();
    assertTrue("should have found coordinator " + mockMembers[2], result);
    assertTrue("should have found " + coordinator + " but found " + state.possibleCoordinator,
        state.possibleCoordinator == coordinator);
  }

  @Test
  public void testProcessJoinMessageRejectOldMemberVersion() throws Exception {
    initMocks();

    gmsJoinLeave.processMessage(new JoinRequestMessage(mockOldMember, mockOldMember, null, -1, 0));
    assertTrue("JoinRequest should not have been added to view request",
        gmsJoinLeave.getViewRequests().size() == 0);
    verify(messenger).send(isA(JoinResponseMessage.class));
  }

  @Test
  public void testViewWithoutMemberInitiatesForcedDisconnect() throws Exception {
    initMocks();
    becomeCoordinatorForTest(gmsJoinLeave);
    List<MemberIdentifier> members = Arrays.asList(mockMembers);
    GMSMembershipView v = new GMSMembershipView(mockMembers[0], 2, members);
    InstallViewMessage message = getInstallViewMessage(v, null, false);
    gmsJoinLeave.processMessage(message);
    verify(manager).forceDisconnect(isA(String.class));
  }


  @Test
  public void testProcessJoinMessageWithBadAuthentication() throws Exception {
    initMocks();
    when(authenticator.authenticate(mockMembers[0], credentials))
        .thenThrow(new SecurityException("we want to fail auth here"));

    gmsJoinLeave
        .processMessage(new JoinRequestMessage(mockMembers[0], mockMembers[0], credentials, -1, 0));
    assertTrue("JoinRequest should not have been added to view request",
        gmsJoinLeave.getViewRequests().size() == 0);
    verify(messenger).send(isA(JoinResponseMessage.class));
  }

  @Test
  public void testProcessJoinMessageWithAuthenticationButNullCredentials() throws Exception {
    initMocks();
    when(authenticator.authenticate(mockMembers[0], null))
        .thenThrow(new SecurityException("we want to fail auth here"));

    gmsJoinLeave
        .processMessage(new JoinRequestMessage(mockMembers[0], mockMembers[0], null, -1, 0));
    assertTrue("JoinRequest should not have been added to view request",
        gmsJoinLeave.getViewRequests().size() == 0);
    verify(messenger).send(isA(JoinResponseMessage.class));
  }

  // This test does not test the actual join process but rather that the join response gets logged
  @Test
  public void testProcessJoinResponseIsRecorded() throws Exception {
    initMocks();
    when(authenticator.authenticate(mockMembers[0], null))
        .thenThrow(new SecurityException("we want to fail auth here"));

    JoinResponseMessage[] joinResponse = gmsJoinLeave.getJoinResponseMessage();

    JoinResponseMessage jrm = new JoinResponseMessage(mockMembers[0], new byte[9], 233);
    gmsJoinLeave.processMessage(jrm);
    // this should NOT logs, this is just to inform member succesful joining
    Assert.assertEquals(null, joinResponse[0]);

    jrm = new JoinResponseMessage("rejected...", 0);
    gmsJoinLeave.processMessage(jrm);
    // this should log..
    Assert.assertEquals(jrm, joinResponse[0]);
  }

  /**
   * prepares and install a view
   *
   */
  private void prepareAndInstallView(MemberIdentifier coordinator,
      List<MemberIdentifier> members) throws IOException {
    int viewId = 1;

    // prepare the view
    GMSMembershipView<MemberIdentifier> netView =
        new GMSMembershipView(coordinator, viewId, members);
    for (MemberIdentifier member : netView.getMembers()) {
      netView.setPublicKey(member, member.toString());
    }
    InstallViewMessage installViewMessage = getInstallViewMessage(netView, credentials, true);
    gmsJoinLeave.processMessage(installViewMessage);
    verify(messenger).send(isA(ViewAckMessage.class));

    // install the view
    installViewMessage = getInstallViewMessage(netView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);
    Assert.assertEquals(netView, gmsJoinLeave.getView());
  }

  private List<MemberIdentifier> createMemberList(MemberIdentifier... members) {
    List<MemberIdentifier> memberList =
        new ArrayList<MemberIdentifier>(members.length);
    for (MemberIdentifier member : members) {
      memberList.add(member);
    }
    return memberList;
  }

  @Test
  public void testRemoveMember() throws Exception {
    initMocks();
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));
    gmsJoinLeave.remove(gmsJoinLeaveMemberId, "removing for test");
    verify(messenger, timeout(2000).atLeastOnce()).send(isA(RemoveMemberMessage.class));
  }

  @Test
  public void testIsMemberLeaving() throws Exception {
    initMocks();
    prepareAndInstallView(mockMembers[0],
        createMemberList(mockMembers[0], mockMembers[1], gmsJoinLeaveMemberId));
    assertFalse(gmsJoinLeave.isMemberLeaving(mockMembers[0]));
    assertFalse(gmsJoinLeave.isMemberLeaving(mockMembers[1]));
    gmsJoinLeave.remove(mockMembers[0], "removing for test");
    verify(messenger, timeout(2000).atLeastOnce()).send(isA(RemoveMemberMessage.class));
    assertTrue(gmsJoinLeave.isMemberLeaving(mockMembers[0]));
    LeaveRequestMessage msg =
        new LeaveRequestMessage(gmsJoinLeave.getMemberID(), mockMembers[1], "leaving for test");
    msg.setSender(mockMembers[1]);
    gmsJoinLeave.processMessage(msg);
    assertTrue(gmsJoinLeave.isMemberLeaving(mockMembers[1]));
  }

  @Test
  public void testRemoveAndLeaveIsNotACrash() throws Exception {
    // simultaneous leave & remove requests for a member
    // should not result in it's being seen as a crashed member
    initMocks();
    final int viewInstallationTime = 15000;

    when(healthMonitor.checkIfAvailable(isA(MemberIdentifier.class), isA(String.class),
        isA(Boolean.class))).thenReturn(true);

    gmsJoinLeave.delayViewCreationForTest(5000); // ensures multiple requests are queued for a view
                                                 // change
    becomeCoordinatorForTest(gmsJoinLeave);

    await()
        .until(() -> gmsJoinLeave.getView() != null);

    GMSMembershipView oldView = gmsJoinLeave.getView();

    GMSMembershipView newView = new GMSMembershipView(oldView, oldView.getViewId() + 1);
    newView.add(mockMembers[1]);
    newView.add(mockMembers[2]);
    gmsJoinLeave.installView(newView);

    gmsJoinLeave.memberShutdown(mockMembers[1], "shutting down for test");
    gmsJoinLeave.remove(mockMembers[1], "removing for test");

    await()
        .until(() -> gmsJoinLeave.getView().getViewId() > newView.getViewId());
    assertFalse(gmsJoinLeave.getView().getCrashedMembers().contains(mockMembers[1]));
  }

  @Test
  public void multipleLocatorsWithSameAddressAreCanonicalized() throws Exception {
    List<HostAndPort> locators = GMSUtil.parseLocators(
        "localhost[1234],localhost[1234],localhost[1234]", (InetAddress) null);
    assertThat(locators.size()).isEqualTo(1);
  }


  @Test
  public void testRejectOlderView() throws Exception {
    initMocks();
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));

    List<MemberIdentifier> mbrs = new LinkedList<>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);

    // try to install an older view where viewId < currentView.viewId
    GMSMembershipView olderNetView = new GMSMembershipView(mockMembers[0], 0, mbrs);
    InstallViewMessage installViewMessage = getInstallViewMessage(olderNetView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);
    Assert.assertNotEquals(gmsJoinLeave.getView(), olderNetView);
  }

  @Test
  public void testForceDisconnectedFromNewView() throws Exception {
    initMocks(true);// enabledNetworkPartition;
    Manager mockManager = mock(Manager.class);
    when(services.getManager()).thenReturn(mockManager);
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));

    int viewId = 2;
    List<MemberIdentifier> mbrs = new LinkedList<>();
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);
    mbrs.add(mockMembers[3]);

    // install the view
    GMSMembershipView netView = new GMSMembershipView(mockMembers[0], viewId, mbrs);
    InstallViewMessage installViewMessage = getInstallViewMessage(netView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);

    Assert.assertNotEquals(netView, gmsJoinLeave.getView());
    verify(mockManager).forceDisconnect(isA(String.class));
  }

  @Test
  public void testNonMemberCantRemoveMember() throws Exception {
    String reason = "testing";
    initMocks();
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));
    // test that a non-member can't remove another member
    RemoveMemberMessage msg = new RemoveMemberMessage(mockMembers[0], mockMembers[1], reason);
    msg.setSender(MemberIdentifierUtil.createMemberID(9000));
    gmsJoinLeave.processMessage(msg);
    assertTrue("RemoveMemberMessage should not have been added to view requests",
        gmsJoinLeave.getViewRequests().size() == 0);
  }

  @Test
  public void testDuplicateLeaveRequestDoesNotCauseNewView() throws Exception {
    String reason = "testing";
    initMocks();
    gmsJoinLeave.unitTesting.add("noRandomViewChange");
    prepareAndInstallView(gmsJoinLeaveMemberId,
        createMemberList(gmsJoinLeaveMemberId, mockMembers[0]));
    becomeCoordinatorForTest(gmsJoinLeave);

    LeaveRequestMessage msg =
        new LeaveRequestMessage(gmsJoinLeave.getMemberID(), mockMembers[0], reason);
    msg.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(msg);
    msg = new LeaveRequestMessage(gmsJoinLeave.getMemberID(), mockMembers[0], reason);
    msg.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(msg);

    waitForViewAndNoRequestsInProgress(7);

    GMSMembershipView view = gmsJoinLeave.getView();
    assertTrue("expected member to be removed: " + mockMembers[0] + "; view: " + view,
        !view.contains(mockMembers[0]));
    assertTrue("expected member to be in shutdownMembers collection: " + mockMembers[0] + "; view: "
        + view, view.getShutdownMembers().contains(mockMembers[0]));
  }

  @Test
  public void testDuplicateRemoveRequestDoesNotCauseNewView() throws Exception {
    String reason = "testing";
    initMocks();
    prepareAndInstallView(gmsJoinLeaveMemberId,
        createMemberList(gmsJoinLeaveMemberId, mockMembers[0]));
    gmsJoinLeave.getView().add(mockMembers[1]);
    gmsJoinLeave.unitTesting.add("noRandomViewChange");
    becomeCoordinatorForTest(gmsJoinLeave);
    RemoveMemberMessage msg =
        new RemoveMemberMessage(gmsJoinLeave.getMemberID(), mockMembers[0], reason);
    msg.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(msg);
    msg = new RemoveMemberMessage(gmsJoinLeave.getMemberID(), mockMembers[0], reason);
    msg.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(msg);

    waitForViewAndNoRequestsInProgress(7);

    GMSMembershipView view = gmsJoinLeave.getView();
    assertTrue("expected member to be removed: " + mockMembers[0] + "; view: " + view,
        !view.contains(mockMembers[0]));
    assertTrue(
        "expected member to be in crashedMembers collection: " + mockMembers[0] + "; view: " + view,
        view.getCrashedMembers().contains(mockMembers[0]));
  }

  @Test
  public void testDuplicateJoinRequestDoesNotCauseNewView() throws Exception {
    initMocks();
    when(healthMonitor.checkIfAvailable(isA(MemberIdentifier.class), isA(String.class),
        isA(Boolean.class))).thenReturn(true);
    gmsJoinLeave.unitTesting.add("noRandomViewChange");
    prepareAndInstallView(gmsJoinLeaveMemberId,
        createMemberList(gmsJoinLeaveMemberId, mockMembers[0]));
    gmsJoinLeave.getView().add(mockMembers[1]);
    becomeCoordinatorForTest(gmsJoinLeave);
    JoinRequestMessage msg =
        new JoinRequestMessage(gmsJoinLeaveMemberId, mockMembers[2], null, -1, 0);
    msg.setSender(mockMembers[2]);
    gmsJoinLeave.processMessage(msg);
    msg = new JoinRequestMessage(gmsJoinLeaveMemberId, mockMembers[2], null, -1, 0);
    msg.setSender(mockMembers[2]);
    gmsJoinLeave.processMessage(msg);

    waitForViewAndNoRequestsInProgress(7);

    GMSMembershipView view = gmsJoinLeave.getView();
    assertTrue("expected member to be added: " + mockMembers[2] + "; view: " + view,
        view.contains(mockMembers[2]));
    List<MemberIdentifier> members = view.getMembers();
    int occurrences = 0;
    for (MemberIdentifier mbr : members) {
      if (mbr.equals(mockMembers[2])) {
        occurrences += 1;
      }
    }
    assertTrue("expected member to only be in the view once: " + mockMembers[2] + "; view: " + view,
        occurrences == 1);
    verify(healthMonitor, times(5)).checkIfAvailable(isA(MemberIdentifier.class),
        isA(String.class), isA(Boolean.class));
  }


  private void waitForViewAndNoRequestsInProgress(int viewId) throws InterruptedException {
    // wait for the view processing thread to collect and process the requests
    int sleeps = 0;
    while (!gmsJoinLeave.isStopping() && !gmsJoinLeave.getViewCreator().isWaiting()
        && (!gmsJoinLeave.getViewRequests().isEmpty()
            || gmsJoinLeave.getView().getViewId() != viewId)) {
      if (sleeps++ > 20) {
        throw new RuntimeException("timeout waiting for view #" + viewId + " current view: "
            + gmsJoinLeave.getView() + "; view requests: " + gmsJoinLeave.getViewRequests());
      }
      Thread.sleep(1000);
    }
  }

  @Test
  public void testRemoveMessageForRogueCausesImmediateRemovalMessageToRogue() throws Exception {
    initMocks();
    gmsJoinLeave.currentView = createView();
    gmsJoinLeave.isCoordinator = true;
    RemoveMemberMessage msg = new RemoveMemberMessage(gmsJoinLeaveMemberId,
        MemberIdentifierUtil.createMemberID(10000), "removing for test");
    msg.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(msg);
    verify(messenger).send(isA(RemoveMemberMessage.class));
  }

  @Test
  public void testRemoveRequestCausesForcedDisconnectInRogue() throws Exception {
    initMocks();
    // gmsJoinLeave mistakenly uses an old viewID when joining, making it a rogue member
    gmsJoinLeaveMemberId.setVmViewId(-1);
    MemberIdentifier previousMemberId = services.getMemberFactory().create(
        MemberDataBuilder.newBuilder(gmsJoinLeaveMemberId.getInetAddress(),
            gmsJoinLeaveMemberId.getHostName())
            .setMembershipPort(gmsJoinLeaveMemberId.getMembershipPort())
            .build());
    previousMemberId.setVmViewId(0);
    previousMemberId.setUUID(gmsJoinLeaveMemberId.getUUID());
    GMSMembershipView view = new GMSMembershipView(mockMembers[0], 1,
        createMemberList(mockMembers[0], previousMemberId, mockMembers[1]));
    InstallViewMessage viewMessage = new InstallViewMessage(view, 0, false);
    viewMessage.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(viewMessage);
    assertEquals(0, gmsJoinLeaveMemberId.getVmViewId());
    // a RemoveMember message should cause it to force-disconnect
    RemoveMemberMessage msg =
        new RemoveMemberMessage(gmsJoinLeaveMemberId, gmsJoinLeaveMemberId, "removing for test");
    msg.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(msg);
    verify(manager).forceDisconnect("removing for test");
  }

  @Test
  public void testRemoveCausesForcedDisconnect() throws Exception {
    String reason = "testing";
    initMocks();
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));
    gmsJoinLeave.getView().add(mockMembers[1]);
    RemoveMemberMessage msg =
        new RemoveMemberMessage(mockMembers[0], gmsJoinLeave.getMemberID(), reason);
    msg.setSender(mockMembers[1]);
    gmsJoinLeave.processMessage(msg);
    verify(manager).forceDisconnect(reason);
  }

  @Test
  public void testLeaveCausesForcedDisconnect() throws Exception {
    String reason = "testing";
    initMocks();
    prepareAndInstallView(gmsJoinLeaveMemberId,
        createMemberList(gmsJoinLeaveMemberId, mockMembers[0]));
    gmsJoinLeave.getView().add(mockMembers[1]);
    LeaveRequestMessage msg =
        new LeaveRequestMessage(gmsJoinLeave.getMemberID(), gmsJoinLeave.getMemberID(), reason);
    msg.setSender(mockMembers[1]);
    gmsJoinLeave.processMessage(msg);
    verify(manager).forceDisconnect(reason);
  }

  @Test
  public void testLeaveOfNonMemberIsNoOp() throws Exception {
    String reason = "testing";
    initMocks();
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));
    mockMembers[1].setVmViewId(gmsJoinLeave.getView().getViewId() - 1);
    LeaveRequestMessage msg =
        new LeaveRequestMessage(gmsJoinLeave.getMemberID(), mockMembers[1], reason);
    msg.setSender(mockMembers[1]);
    gmsJoinLeave.processMessage(msg);
    assertTrue("Expected leave request from non-member to be ignored",
        gmsJoinLeave.getViewRequests().isEmpty());
  }

  @Test
  public void testBecomeCoordinatorOnStartup() throws Exception {
    initMocks();
    becomeCoordinatorForTest(gmsJoinLeave);
    await().until(() -> gmsJoinLeave.isCoordinator());
  }

  @Test
  public void testBecomeCoordinator() throws Exception {
    String reason = "testing";
    initMocks();
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));
    GMSMembershipView view = gmsJoinLeave.getView();
    view.add(gmsJoinLeaveMemberId);
    MemberIdentifier creator = view.getCreator();
    LeaveRequestMessage msg = new LeaveRequestMessage(creator, creator, reason);
    msg.setSender(creator);
    gmsJoinLeave.processMessage(msg);
    assertTrue("Expected becomeCoordinator to be invoked", gmsJoinLeave.isCoordinator());
  }

  @Test
  public void testBecomeCoordinatorThroughRemove() throws Exception {
    String reason = "testing";
    initMocks();
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));
    GMSMembershipView view = gmsJoinLeave.getView();
    view.add(gmsJoinLeaveMemberId);
    MemberIdentifier creator = view.getCreator();
    RemoveMemberMessage msg = new RemoveMemberMessage(creator, creator, reason);
    msg.setSender(creator);
    gmsJoinLeave.processMessage(msg);
    assertTrue("Expected becomeCoordinator to be invoked", gmsJoinLeave.isCoordinator());
  }

  /**
   * Given a view with [A, B, C, D, E] where C is coordinator, A failed availability checks and
   * C shuts down we should see B become the coordinator.
   */
  @Test
  public void testBecomeCoordinatorThroughShutdownWhenOlderMemberCrashed() throws Exception {
    initMocks();
    MemberIdentifier A = mockMembers[0],
        B = gmsJoinLeaveMemberId,
        C = mockMembers[1],
        D = mockMembers[2],
        E = mockMembers[3];
    prepareAndInstallView(C, createMemberList(A, B, C, D, E));
    LeaveRequestMessage msg = new LeaveRequestMessage(B, C, "leaving for test");
    msg.setSender(C);
    gmsJoinLeave.processMessage(msg);
    RemoveMemberMessage removeMemberMessage = new RemoveMemberMessage(B, A, "removing for test");
    removeMemberMessage.setSender(B);
    gmsJoinLeave.processMessage(removeMemberMessage);
    assertTrue("Expected becomeCoordinator to be invoked", gmsJoinLeave.isCoordinator());
  }

  /**
   * Given a view with [A, B, C, D] where C is coordinator, a failed availability check on A
   * and a pending join request from E show that B becomes coordinator when C sends it a
   * Leave request and that E is allowed into the system and retains its view ID.
   * See GEODE-6570
   */
  @Test
  public void testBecomeCoordinatorAndAcceptMemberWithViewID() throws Exception {
    initMocks();
    MemberIdentifier A = mockMembers[0],
        B = gmsJoinLeaveMemberId,
        C = mockMembers[1],
        D = mockMembers[2],
        E = mockMembers[3];

    prepareAndInstallView(C, createMemberList(A, B, C, D));

    // have the Messenger acknowledge all membership view messages so no-one is kicked out for
    // failure to respond
    when(messenger.send(isA(InstallViewMessage.class), isA(GMSMembershipView.class)))
        .thenAnswer((request) -> {
          InstallViewMessage<MemberIdentifier> installViewMessage = request.getArgument(0);
          for (MemberIdentifier recipient : installViewMessage.getRecipients()) {
            ViewAckMessage viewAckMessage =
                new ViewAckMessage(gmsJoinLeaveMemberId, installViewMessage.getView().getViewId(),
                    installViewMessage.isPreparing());
            viewAckMessage.setSender(recipient);
            gmsJoinLeave.processMessage(viewAckMessage);
          }
          return null;
        });

    E.setVmViewId(2);

    gmsJoinLeave.recordViewRequest(new LeaveRequestMessage(B, C, "removing for test"));

    gmsJoinLeave.processMessage(new JoinRequestMessage(B, E, null, 1, 1));

    RemoveMemberMessage msg = new RemoveMemberMessage(B, A, "crashed for test");
    msg.setSender(D);
    gmsJoinLeave.processMessage(msg);

    await().until(() -> gmsJoinLeave.isCoordinator() && gmsJoinLeave.getViewRequests().isEmpty());

    // E should have joined and retained its view ID of 2
    await().until(() -> gmsJoinLeave.getView().contains(E));
    assertEquals(2, E.getVmViewId());
  }

  @Test
  public void testBecomeCoordinatorThroughViewChange() throws Exception {
    initMocks();
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));
    GMSMembershipView oldView = gmsJoinLeave.getView();
    oldView.add(gmsJoinLeaveMemberId);
    GMSMembershipView view = new GMSMembershipView(oldView, oldView.getViewId() + 1);
    MemberIdentifier creator = view.getCreator();
    view.remove(creator);
    InstallViewMessage msg = getInstallViewMessage(view, creator, false);
    msg.setSender(creator);
    gmsJoinLeave.processMessage(msg);
    assertTrue("Expected it to become coordinator", gmsJoinLeave.isCoordinator());
  }

  @Test
  public void testBecomeCoordinatorThroughViewChangeWhenCoordinatorIsShuttingDown()
      throws Exception {
    initMocks();
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));
    GMSMembershipView oldView = gmsJoinLeave.getView();
    oldView.add(gmsJoinLeaveMemberId);
    GMSMembershipView view = new GMSMembershipView(oldView, oldView.getViewId() + 1);
    MemberIdentifier creator = view.getCreator();
    LeaveRequestMessage leaveRequestMessage =
        new LeaveRequestMessage(gmsJoinLeaveMemberId, mockMembers[0], "leaving for test");
    gmsJoinLeave.processMessage(leaveRequestMessage);
    assertTrue(gmsJoinLeave.isCoordinator());
    InstallViewMessage msg = getInstallViewMessage(view, creator, false);
    msg.setSender(creator);
    gmsJoinLeave.processMessage(msg);
    assertTrue("Expected it to remain coordinator", gmsJoinLeave.isCoordinator());
  }

  @Test
  public void testBecomeParticipantThroughViewChange() throws Exception {
    initMocks();
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));
    GMSMembershipView oldView = gmsJoinLeave.getView();
    oldView.add(gmsJoinLeaveMemberId);
    MemberIdentifier creator = oldView.getCreator();
    becomeCoordinatorForTest(gmsJoinLeave);
    GMSMembershipView view = new GMSMembershipView(2, gmsJoinLeave.getView().getViewId() + 1);
    view.setCreator(creator);
    view.add(creator);
    view.add(gmsJoinLeaveMemberId);
    InstallViewMessage msg = getInstallViewMessage(view, creator, false);
    msg.setSender(creator);
    gmsJoinLeave.processMessage(msg);
    assertTrue("Expected it to stop being coordinator", !gmsJoinLeave.isCoordinator());
  }

  private InstallViewMessage getInstallViewMessage(GMSMembershipView view, Object credentials,
      boolean preparing) {
    InstallViewMessage installViewMessage = new InstallViewMessage(view, credentials, preparing);
    installViewMessage.setSender(gmsJoinLeaveMemberId);
    return installViewMessage;
  }

  @Test
  public void testNetworkPartitionDetected() throws Exception {
    initMocks(true);
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));

    // set up a view with sufficient members, then create a new view
    // where enough weight is lost to cause a network partition

    List<MemberIdentifier> mbrs = new LinkedList<>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);
    mbrs.add(gmsJoinLeaveMemberId);

    mockMembers[1].setMemberWeight((byte) 20);

    GMSMembershipView newView =
        new GMSMembershipView(mockMembers[0], gmsJoinLeave.getView().getViewId() + 1, mbrs);
    InstallViewMessage installViewMessage = getInstallViewMessage(newView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);

    Set<MemberIdentifier> crashes = new HashSet<>();
    crashes.add(mockMembers[1]);
    crashes.add(mockMembers[2]);
    mbrs = new LinkedList<>(mbrs);
    mbrs.remove(mockMembers[1]);
    mbrs.remove(mockMembers[2]);
    GMSMembershipView partitionView =
        new GMSMembershipView(mockMembers[0], newView.getViewId() + 1, mbrs, Collections.emptySet(),
            crashes);
    installViewMessage = getInstallViewMessage(partitionView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);

    verify(manager).forceDisconnect(isA(String.class));
    verify(manager).quorumLost(crashes, newView);
  }

  // Possibly modify test to check for network partition message in the force disconnect
  @Test
  public void testNetworkPartitionMessageReceived() throws Exception {
    initMocks();
    becomeCoordinatorForTest(gmsJoinLeave);
    NetworkPartitionMessage message = new NetworkPartitionMessage();
    gmsJoinLeave.processMessage(message);
    verify(manager).forceDisconnect(isA(String.class));
  }


  @Test
  public void testQuorumLossNotificationWithNetworkPartitionDetectionDisabled() throws Exception {
    initMocks(false);
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));

    // set up a view with sufficient members, then create a new view
    // where enough weight is lost to cause a network partition

    List<MemberIdentifier> mbrs = new LinkedList<>();
    Set<MemberIdentifier> shutdowns = new HashSet<>();
    Set<MemberIdentifier> crashes = new HashSet<>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);
    mbrs.add(gmsJoinLeaveMemberId);

    mockMembers[1].setMemberWeight((byte) 20);

    GMSMembershipView newView =
        new GMSMembershipView(mockMembers[0], gmsJoinLeave.getView().getViewId() + 1, mbrs,
            shutdowns, crashes);
    InstallViewMessage installViewMessage = getInstallViewMessage(newView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);

    crashes = new HashSet<>(crashes);
    crashes.add(mockMembers[1]);
    crashes.add(mockMembers[2]);
    mbrs = new LinkedList<>(mbrs);
    mbrs.remove(mockMembers[1]);
    mbrs.remove(mockMembers[2]);
    GMSMembershipView partitionView =
        new GMSMembershipView(mockMembers[0], newView.getViewId() + 1, mbrs, shutdowns, crashes);
    installViewMessage = getInstallViewMessage(partitionView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);

    verify(manager, never()).forceDisconnect(isA(String.class));
    verify(manager).quorumLost(crashes, newView);
  }

  @Test
  public void testConflictingPrepare() throws Exception {
    initMocks(true);
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));

    GMSMembershipView gmsView = gmsJoinLeave.getView();
    GMSMembershipView newView = new GMSMembershipView(gmsView, gmsView.getViewId() + 6);
    InstallViewMessage msg = getInstallViewMessage(newView, null, true);
    gmsJoinLeave.processMessage(msg);

    GMSMembershipView alternateView = new GMSMembershipView(gmsView, gmsView.getViewId() + 1);
    msg = getInstallViewMessage(alternateView, null, true);
    gmsJoinLeave.processMessage(msg);

    assertTrue(gmsJoinLeave.getPreparedView().equals(newView));
  }

  @Test
  public void testNoViewAckCausesRemovalMessage() throws Exception {
    initMocks(true);
    when(healthMonitor.checkIfAvailable(isA(MemberIdentifier.class), isA(String.class),
        isA(Boolean.class))).thenReturn(false);
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));
    GMSMembershipView oldView = gmsJoinLeave.getView();
    GMSMembershipView newView = new GMSMembershipView(oldView, oldView.getViewId() + 1);

    // the new view will remove the old coordinator (normal shutdown) and add a new member
    // who will not ack the view. This should cause it to be removed from the system
    // with a RemoveMemberMessage
    newView.add(mockMembers[2]);
    newView.remove(mockMembers[0]);

    InstallViewMessage installViewMessage = getInstallViewMessage(newView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);

    // this test's member-timeout * 3
    await()
        .until(() -> gmsJoinLeave.getView().getViewId() != oldView.getViewId());
    assertTrue(gmsJoinLeave.isCoordinator());
    // wait for suspect processing

    verify(healthMonitor, timeout(10000).atLeast(1)).checkIfAvailable(
        isA(MemberIdentifier.class),
        isA(String.class), isA(Boolean.class));
    // verify(messenger, atLeast(1)).send(isA(RemoveMemberMessage.class));
  }

  /**
   * This tests a member shutdown using the memberShutdown call (simulating the call from
   * DistributionManager) The gmsJoinLeaveMemberId is not the coordinator but should now become the
   * coordinator.
   */
  @Test
  public void testCoordinatorShutsdownAndWeBecomeCoordinatorAndSendOutCorrectView()
      throws Exception {
    initMocks(false);
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId,
        mockMembers[1], mockMembers[2], mockMembers[3]));
    Assert.assertFalse(gmsJoinLeave.isCoordinator());

    // The coordinator shuts down
    gmsJoinLeave.memberShutdown(mockMembers[0], "Shutdown");
    GMSMembershipView nextView = gmsJoinLeave.getViewCreator().initialView;

    assertTrue(gmsJoinLeave.isCoordinator());
    assertTrue(nextView.getCoordinator().equals(gmsJoinLeaveMemberId));
    assertTrue(nextView.getMembers().contains(mockMembers[1]));
    assertTrue(nextView.getMembers().contains(mockMembers[2]));
    assertTrue(nextView.getMembers().contains(mockMembers[3]));
  }


  /**
   * This tests a member shutdown using the memberShutdown call (simulating the call from
   * DistributionManager) The gmsJoinLeaveMemberId is not the coordinator but should now become the
   * coordinator and remove all members that have sent us leave requests prior to us becoming
   * coordinator
   */
  @Test
  public void testCoordinatorAndOthersShutdownAndWeBecomeCoordinatorProcessQueuedUpLeaveMessages()
      throws Exception {
    initMocks(false);
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], mockMembers[1],
        mockMembers[2], gmsJoinLeaveMemberId, mockMembers[3]));
    Assert.assertFalse(gmsJoinLeave.isCoordinator());
    // The coordinator and other members shutdown
    gmsJoinLeave.memberShutdown(mockMembers[1], "Shutdown");
    gmsJoinLeave.memberShutdown(mockMembers[2], "Shutdown");
    gmsJoinLeave.memberShutdown(mockMembers[0], "Shutdown");
    GMSMembershipView nextView = gmsJoinLeave.getViewCreator().initialView;

    assertTrue(gmsJoinLeave.isCoordinator());
    assertTrue(nextView.getCoordinator().equals(gmsJoinLeaveMemberId));
    Assert.assertFalse(nextView.getMembers().contains(mockMembers[1]));
    Assert.assertFalse(nextView.getMembers().contains(mockMembers[2]));
    assertTrue(nextView.getMembers().contains(mockMembers[3]));
  }

  /**
   * If a locator is started and sends out a view to take control of the cluster another member that
   * is also in the process of sending out a view should relinquish control to the new locator.
   *
   */
  @Test
  public void testCoordinatorGetsConflictingViewFromLocator() throws Exception {
    // create the GMSJoinLeave instance we'll be testing
    initMocks(false);
    MemberIdentifier otherMember = mockMembers[0];
    gmsJoinLeaveMemberId.setVmKind(MemberIdentifier.NORMAL_DM_TYPE);
    List<MemberIdentifier> members = createMemberList(gmsJoinLeaveMemberId, otherMember);
    prepareAndInstallView(gmsJoinLeaveMemberId, members);
    GMSMembershipView installedView = gmsJoinLeave.getView();

    gmsJoinLeave.unitTesting.add("noRandomViewChange"); // keep view numbers predictable

    // create a view coming from the locator that conflicts with the installed view
    MemberIdentifier locatorMemberId = MemberIdentifierUtil.createMemberID(
        mockMembers[mockMembers.length - 1].getMembershipPort() + 1);
    locatorMemberId.setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);
    List<MemberIdentifier> newMemberList = new ArrayList<>(members);
    newMemberList.add(locatorMemberId);
    GMSMembershipView locatorView =
        new GMSMembershipView(locatorMemberId, installedView.getViewId() + 10, newMemberList);

    // start the process to make our GMSJoinLeave become coordinator. It will send out a view
    // and want an ACK from
    synchronized (gmsJoinLeave.getViewInstallationLock()) {
      gmsJoinLeave.becomeCoordinator();
    }
    await()
        .until(() -> gmsJoinLeave.prepareProcessor.isWaiting());

    int newViewId = 6; // becomeCoordinator will bump the initial view Id by 5

    ViewAckMessage msg = new ViewAckMessage(gmsJoinLeaveMemberId, newViewId, true);
    msg.setSender(gmsJoinLeaveMemberId);
    gmsJoinLeave.processMessage(msg);

    // ack the view on behalf of the other member, returning a conflicting view coming from a
    // locator that is trying to become coordinator
    msg = new ViewAckMessage(newViewId, gmsJoinLeaveMemberId, locatorView);
    msg.setSender(otherMember);
    gmsJoinLeave.processMessage(msg);

    await()
        .until(() -> gmsJoinLeave.getViewCreator() != null);
    await()
        .until(() -> gmsJoinLeave.getViewCreator().getAbandonedViewCount() > 0);
    assertEquals(installedView, gmsJoinLeave.getView());

  }

  /**
   * In a scenario where we have a member leave at the same time as an install view The member that
   * left should be recorded on all members, if the coordinator also happens to leave, the new
   * coordinator should be able to process the new view correctly
   */
  @Test
  public void testTimingWhereInstallViewComeAndDoesNotClearOutLeftMembersList() throws Exception {
    initMocks(false);
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], mockMembers[1],
        mockMembers[2], gmsJoinLeaveMemberId, mockMembers[3]));
    Assert.assertFalse(gmsJoinLeave.isCoordinator());
    // The coordinator and other members shutdown
    gmsJoinLeave.memberShutdown(mockMembers[1], "Shutdown");
    gmsJoinLeave.memberShutdown(mockMembers[2], "Shutdown");

    // Install a view that still contains one of the left members (as if something like a new
    // member, triggered a new view before coordinator leaves)
    GMSMembershipView netView = new GMSMembershipView(mockMembers[0], 3/* new view id */,
        createMemberList(mockMembers[0], gmsJoinLeaveMemberId, mockMembers[1], mockMembers[3]));
    InstallViewMessage installViewMessage = getInstallViewMessage(netView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);

    // Now coordinator leaves
    gmsJoinLeave.memberShutdown(mockMembers[0], "Shutdown");
    GMSMembershipView nextView = gmsJoinLeave.getViewCreator().initialView;

    assertTrue(gmsJoinLeave.isCoordinator());
    assertTrue(nextView.getCoordinator().equals(gmsJoinLeaveMemberId));
    Assert.assertFalse(nextView.getMembers().contains(mockMembers[1]));
    Assert.assertFalse(nextView.getMembers().contains(mockMembers[2]));
    assertTrue(nextView.getMembers().contains(mockMembers[3]));
  }

  @Test
  public void testViewBroadcaster() throws Exception {
    initMocks();
    List<MemberIdentifier> members = new ArrayList<>(Arrays.asList(mockMembers));
    gmsJoinLeaveMemberId.setVmViewId(1);
    members.add(gmsJoinLeaveMemberId);
    prepareAndInstallView(gmsJoinLeaveMemberId, members);
    becomeCoordinatorForTest(gmsJoinLeave);
    GMSJoinLeave.ViewBroadcaster b = gmsJoinLeave.new ViewBroadcaster();
    b.run();
    verify(messenger).sendUnreliably(isA(InstallViewMessage.class));
  }

  private void installView(int viewId, MemberIdentifier coordinator,
      List<MemberIdentifier> members) throws IOException {
    // prepare the view
    GMSMembershipView netView = new GMSMembershipView(coordinator, viewId, members);
    InstallViewMessage installViewMessage = getInstallViewMessage(netView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);
    // verify(messenger).send(isA(ViewAckMessage.class));
  }

  @Test
  public void testIgnoreoldView() throws Exception {
    initMocks(false);
    installView(3, mockMembers[0], createMemberList(mockMembers[0], mockMembers[1], mockMembers[2],
        gmsJoinLeaveMemberId, mockMembers[3]));
    // now try to intall old view..
    installView(1, mockMembers[0], createMemberList(mockMembers[0], mockMembers[1], mockMembers[2],
        gmsJoinLeaveMemberId, mockMembers[3]));

    assertFalse("Expected view id is 3 but found " + gmsJoinLeave.getView().getViewId(),
        gmsJoinLeave.getView().getViewId() == 1);
  }

  @Test
  public void testClearViewRequests() throws Exception {
    try {
      initMocks(false);
      System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
      gmsJoinLeave.join();
      gmsJoinLeave.processMessage(
          new JoinRequestMessage(mockMembers[0], mockMembers[0], credentials, -1, 0));
      int viewRequests = gmsJoinLeave.getViewRequests().size();

      assertEquals("There should be 1 viewRequest", 1, viewRequests);
      await()
          .until(() -> gmsJoinLeave.getViewRequests().size(), equalTo(0));
    } finally {
      System.getProperties().remove(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
    }
  }

  /***
   * validating ViewReplyProcessor's memberSuspected, processLeaveRequest, processRemoveRequest,
   * processViewResponse method
   */
  @Test
  public void testViewReplyProcessor() throws Exception {
    try {
      initMocks(false);
      System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
      gmsJoinLeave.join();
      Set<MemberIdentifier> recips = new HashSet<>();
      recips.add(mockMembers[0]);
      recips.add(mockMembers[1]);
      recips.add(mockMembers[2]);
      recips.add(mockMembers[3]);
      ViewReplyProcessor prepareProcessor = gmsJoinLeave.getPrepareViewReplyProcessor();
      prepareProcessor.initialize(1, recips);
      assertTrue("Prepare processor should be waiting ",
          gmsJoinLeave.testPrepareProcessorWaiting());

      prepareProcessor.memberSuspected(mockMembers[0]);
      prepareProcessor.processLeaveRequest(mockMembers[1]);
      prepareProcessor.processRemoveRequest(mockMembers[2]);
      prepareProcessor.processViewResponse(1, mockMembers[3], null);

      assertFalse("Prepare processor should not be waiting ",
          gmsJoinLeave.testPrepareProcessorWaiting());
    } finally {
      System.getProperties().remove(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
    }
  }

  /***
   * validating ViewReplyProcessor's processPendingRequests method
   */
  @Test
  public void testViewReplyProcessor2() throws Exception {
    try {
      initMocks(false);
      System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
      gmsJoinLeave.join();
      Set<MemberIdentifier> recips = new HashSet<>();
      recips.add(mockMembers[0]);
      recips.add(mockMembers[1]);
      recips.add(mockMembers[2]);
      recips.add(mockMembers[3]);
      ViewReplyProcessor prepareProcessor = gmsJoinLeave.getPrepareViewReplyProcessor();
      prepareProcessor.initialize(1, recips);
      assertTrue("Prepare processor should be waiting ",
          gmsJoinLeave.testPrepareProcessorWaiting());
      Set<MemberIdentifier> pendingLeaves = new HashSet<>();
      pendingLeaves.add(mockMembers[0]);
      Set<MemberIdentifier> pendingRemovals = new HashSet<>();
      pendingRemovals.add(mockMembers[1]);

      prepareProcessor.processPendingRequests(pendingLeaves, pendingRemovals);

      prepareProcessor.processViewResponse(1, mockMembers[2], null);
      prepareProcessor.processViewResponse(1, mockMembers[3], null);

      assertFalse("Prepare processor should not be waiting ",
          gmsJoinLeave.testPrepareProcessorWaiting());
    } finally {
      System.getProperties().remove(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
    }
  }

  // With the removal of the JoinResponse message from GMSJoinLeave.processJoinRequest (GEODE-870)
  // This test now seems to be invalid
  // @Test
  // public void testJoinResponseMsgWithBecomeCoordinator() throws Exception {
  // initMocks(false);
  // gmsJoinLeaveMemberId.getNetMember().setPreferredForCoordinator(false);
  // JoinRequestMessage reqMsg = new JoinRequestMessage(gmsJoinLeaveMemberId, mockMembers[0], null,
  // 56734);
  // GMSMember ids = new InternalDistributedMember("localhost", 97898);
  // ids.getNetMember().setPreferredForCoordinator(true);
  // gmsJoinLeave.processMessage(reqMsg);
  // ArgumentCaptor<JoinResponseMessage> ac = ArgumentCaptor.forClass(JoinResponseMessage.class);
  // verify(messenger).send(ac.capture());
  //
  // assertTrue("Should have asked for becoming a coordinator",
  // ac.getValue().getBecomeCoordinator());
  // }

  @Test
  public void testNetworkPartionMessage() throws Exception {
    try {
      initMocks(true);
      System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
      gmsJoinLeave.join();
      installView(1, gmsJoinLeaveMemberId, createMemberList(mockMembers[0],
          gmsJoinLeaveMemberId));
      RemoveMemberMessage msg =
          new RemoveMemberMessage(gmsJoinLeaveMemberId, mockMembers[0], "crashed");
      msg.setSender(gmsJoinLeaveMemberId);
      gmsJoinLeave.processMessage(msg);
      Timeout to =
          new Timeout(3 * MembershipConfig.MEMBER_REQUEST_COLLECTION_INTERVAL, new Times(1));
      verify(messenger, to).send(isA(NetworkPartitionMessage.class));

    } finally {
      System.getProperties().remove(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
    }
  }

  @Test
  public void testViewIgnoredAfterShutdown() throws Exception {
    try {
      initMocks(true);
      System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
      gmsJoinLeave.join();
      installView(1, gmsJoinLeaveMemberId, createMemberList(mockMembers[0], mockMembers[1],
          mockMembers[2], gmsJoinLeaveMemberId, mockMembers[3]));
      gmsJoinLeave.stop();
      for (int i = 1; i < 4; i++) {
        RemoveMemberMessage msg =
            new RemoveMemberMessage(gmsJoinLeaveMemberId, mockMembers[i], "crashed");
        msg.setSender(gmsJoinLeaveMemberId);
        gmsJoinLeave.processMessage(msg);
      }
      Timeout to = new Timeout(2 * MembershipConfig.MEMBER_REQUEST_COLLECTION_INTERVAL, never());
      verify(messenger, to).send(isA(NetworkPartitionMessage.class));

    } finally {
      System.getProperties().remove(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
    }
  }

  @Test
  public void testViewNotSentWhenShuttingDown() throws Exception {
    try {
      initMocks(false);
      System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
      gmsJoinLeave.join();
      installView(1, gmsJoinLeaveMemberId, createMemberList(mockMembers[0], mockMembers[1],
          mockMembers[2], gmsJoinLeaveMemberId, mockMembers[3]));

      assertTrue(gmsJoinLeave.getViewCreator().isAlive());
      assertTrue(testLocator.isCoordinator);

      when(manager.shutdownInProgress()).thenReturn(Boolean.TRUE);
      for (int i = 1; i < 4; i++) {
        RemoveMemberMessage msg =
            new RemoveMemberMessage(gmsJoinLeaveMemberId, mockMembers[i], "crashed");
        msg.setSender(gmsJoinLeaveMemberId);
        gmsJoinLeave.processMessage(msg);
      }

      await("waiting for view creator to stop")
          .until(() -> !gmsJoinLeave.getViewCreator().isAlive());
      assertEquals(1, gmsJoinLeave.getView().getViewId());
      assertFalse(testLocator.isCoordinator);

    } finally {
      System.getProperties().remove(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
    }
  }

  @Test
  public void testPreparedViewFoundDuringBecomeCoordinator() throws Exception {
    initMocks(false);
    prepareAndInstallView(gmsJoinLeaveMemberId,
        createMemberList(gmsJoinLeaveMemberId, mockMembers[0]));
    // a new member is joining
    GMSMembershipView preparedView =
        new GMSMembershipView(gmsJoinLeave.getView(), gmsJoinLeave.getView().getViewId() + 5);
    mockMembers[1].setVmViewId(preparedView.getViewId());
    preparedView.add(mockMembers[1]);

    InstallViewMessage msg = getInstallViewMessage(preparedView, null, true);
    gmsJoinLeave.processMessage(msg);

    becomeCoordinatorForTest(gmsJoinLeave);

    Thread.sleep(2000);
    ViewCreator vc = gmsJoinLeave.getViewCreator();
    int viewId = 0;
    if (gmsJoinLeave.getPreparedView() == null) {
      viewId = gmsJoinLeave.getView().getViewId();
    } else {
      viewId = gmsJoinLeave.getPreparedView().getViewId();
    }
    ViewAckMessage vack = new ViewAckMessage(gmsJoinLeaveMemberId, viewId, true);
    vack.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(vack);
    vack = new ViewAckMessage(gmsJoinLeaveMemberId, viewId, true);
    vack.setSender(mockMembers[1]);
    gmsJoinLeave.processMessage(vack);
    vack = new ViewAckMessage(gmsJoinLeaveMemberId, viewId, true);
    vack.setSender(gmsJoinLeaveMemberId);
    gmsJoinLeave.processMessage(vack);

    await("view creator finishes").until(() -> vc.waiting);
    GMSMembershipView newView = gmsJoinLeave.getView();
    System.out.println("new view is " + newView);
    assertTrue(newView.contains(mockMembers[1]));
    assertTrue(newView.getViewId() > preparedView.getViewId());
  }

  @Test
  public void testPublicKeyForNewMemberFromPreparedViewIsInstalledInNewView() throws Exception {
    initMocks(false);
    MemberIdentifier newMember = mockMembers[1];

    prepareAndInstallView(gmsJoinLeaveMemberId,
        createMemberList(gmsJoinLeaveMemberId, mockMembers[0]));
    // a new member is joining
    GMSMembershipView<MemberIdentifier> preparedView =
        new GMSMembershipView(gmsJoinLeave.getView(), gmsJoinLeave.getView().getViewId() + 5);
    for (MemberIdentifier member : preparedView.getMembers()) {
      preparedView.setPublicKey(member, member.toString());
    }
    newMember.setVmViewId(preparedView.getViewId());
    preparedView.add(newMember);
    preparedView.setPublicKey(newMember, newMember.toString());

    InstallViewMessage msg = getInstallViewMessage(preparedView, null, true);
    gmsJoinLeave.processMessage(msg);

    becomeCoordinatorForTest(gmsJoinLeave);

    Thread.sleep(2000);
    ViewCreator vc = gmsJoinLeave.getViewCreator();
    int viewId = 0;
    if (gmsJoinLeave.getPreparedView() == null) {
      viewId = gmsJoinLeave.getView().getViewId();
    } else {
      viewId = gmsJoinLeave.getPreparedView().getViewId();
    }
    ViewAckMessage vack = new ViewAckMessage(gmsJoinLeaveMemberId, viewId, true);
    vack.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(vack);
    vack = new ViewAckMessage(gmsJoinLeaveMemberId, viewId, true);
    vack.setSender(newMember);
    gmsJoinLeave.processMessage(vack);
    vack = new ViewAckMessage(gmsJoinLeaveMemberId, viewId, true);
    vack.setSender(gmsJoinLeaveMemberId);
    gmsJoinLeave.processMessage(vack);

    await("view creator finishes").until(() -> vc.waiting);
    GMSMembershipView newView = gmsJoinLeave.getView();
    System.out.println("new view is " + newView);
    assertTrue(newView.contains(newMember));
    assertNotNull(newView.getPublicKey(newMember));
  }

  private GMSMembershipView createView() {
    List<MemberIdentifier> mbrs = new LinkedList<>();
    Set<MemberIdentifier> shutdowns = new HashSet<>();
    Set<MemberIdentifier> crashes = new HashSet<>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);
    mbrs.add(gmsJoinLeaveMemberId);

    // prepare the view
    GMSMembershipView netView = new GMSMembershipView(mockMembers[0], 1, mbrs, shutdowns, crashes);
    return netView;
  }

  @Test
  public void testCoordinatorFindRequestSuccess() throws Exception {
    initMocks(false);
    mockRequestToServer(isA(HostAndPort.class));

    boolean foundCoordinator = gmsJoinLeave.findCoordinator();
    assertTrue(gmsJoinLeave.searchState.toString(), foundCoordinator);
    assertEquals(gmsJoinLeave.searchState.possibleCoordinator, mockMembers[0]);
  }

  @Test
  public void testCoordinatorFindRequestFailure() throws Exception {
    try {
      initMocks(false);
      mockRequestToServer(eq(new HostAndPort("localhost", 12346)));
      GMSMembershipView view = createView();
      JoinResponseMessage jrm = new JoinResponseMessage(mockMembers[0], view, 0);
      gmsJoinLeave.setJoinResponseMessage(jrm);

      assertThatThrownBy(gmsJoinLeave::join)
          .isInstanceOf(MembershipConfigurationException.class);
    } finally {
    }
  }

  @Test
  public void testJoinFailureWhenSleepInterrupted() throws Exception {
    initMocks(false);
    mockRequestToServer(isA(HostAndPort.class));

    when(mockConfig.getMemberTimeout()).thenReturn(100L);
    when(mockConfig.getJoinTimeout()).thenReturn(1000L);

    GMSJoinLeave spyGmsJoinLeave = spy(gmsJoinLeave);
    when(spyGmsJoinLeave.pauseIfThereIsNoCoordinator(-1, GMSJoinLeave.JOIN_RETRY_SLEEP))
        .thenThrow(new InterruptedException());

    assertThatThrownBy(spyGmsJoinLeave::join)
        .isInstanceOf(MembershipConfigurationException.class)
        .hasMessageContaining("Retry sleep interrupted");
  }

  @Test
  public void testJoinFailureWhenTimeout() throws Exception {
    initMocks(false);
    mockRequestToServer(isA(HostAndPort.class));

    assertThatThrownBy(() -> gmsJoinLeave.join())
        .isInstanceOf(MembershipConfigurationException.class)
        .hasMessageContaining("Operation timed out");
  }

  @Test
  public void testPauseIfThereIsNoCoordinator() throws InterruptedException {
    locatorClient = mock(TcpClient.class);
    gmsJoinLeave = new GMSJoinLeave(locatorClient);
    assertThat(gmsJoinLeave.pauseIfThereIsNoCoordinator(-1, GMSJoinLeave.JOIN_RETRY_SLEEP))
        .isFalse();
    assertThat(gmsJoinLeave.pauseIfThereIsNoCoordinator(1, GMSJoinLeave.JOIN_RETRY_SLEEP)).isTrue();
  }

  @Test
  public void testJoinFailureWhenNoLocator() throws Exception {
    final String locator1 = "locator1[12345]";
    final String locator2 = "locator2[54321]";
    locatorClient = mock(TcpClient.class);

    initMocks(false, false, locator1 + ',' + locator2, locator1);
    when(locatorClient.requestToServer(any(), any(), anyInt(), anyBoolean()))
        .thenThrow(IOException.class);

    assertThatThrownBy(gmsJoinLeave::join)
        .isInstanceOf(MembershipConfigurationException.class)
        .hasMessageContaining(
            "Could not contact any of the locators: [locator1:12345, locator2:54321]")
        .hasCauseInstanceOf(IOException.class);
  }

  private void mockRequestToServer(HostAndPort hostAndPort)
      throws IOException, ClassNotFoundException {
    HashSet<MemberIdentifier> registrants = new HashSet<>();
    registrants.add(mockMembers[0]);

    FindCoordinatorResponse fcr = new FindCoordinatorResponse(mockMembers[0], mockMembers[0], false,
        null, registrants, false, true, null);
    when(locatorClient.requestToServer(hostAndPort,
        isA(FindCoordinatorRequest.class), anyInt(), anyBoolean()))
            .thenReturn(fcr);
  }

  private void waitForViewAndFinalCheckInProgress(int viewId) throws InterruptedException {
    // wait for the view processing thread to collect and process the requests
    int sleeps = 0;
    while (!gmsJoinLeave.isStopping() && (gmsJoinLeave.getView().getViewId() == viewId)) {
      if (sleeps++ > 20) {
        System.out.println("view requests: " + gmsJoinLeave.getViewRequests());
        System.out.println("current view: " + gmsJoinLeave.getView());
        throw new RuntimeException("timeout waiting for view #" + viewId);
      }

      Thread.sleep(1000);
      System.out.println("Empty sleeps " + sleeps + " stopping: " + gmsJoinLeave.isStopping());
    }
  }

  class GMSJoinLeaveTest extends GMSJoinLeave {
    public GMSJoinLeaveTest(final TcpClient locatorClient) {
      super(locatorClient);
    }

    @Override
    boolean checkIfAvailable(MemberIdentifier fmbr) {
      if (removeMember != null) {
        try {
          if (removeMember.equals(fmbr)) {
            GMSJoinLeaveJUnitTest.this.processRemoveMessage(fmbr);
            Thread.sleep(1000000);
          }
        } catch (InterruptedException ignore) {
        }
        return true;
      } else if (leaveMember != null) {
        try {
          if (leaveMember.equals(fmbr)) {
            GMSJoinLeaveJUnitTest.this.processLeaveMessage(fmbr);
            Thread.sleep(1000000);
          }
        } catch (InterruptedException ignore) {
        }
        return true;
      } else {
        return super.checkIfAvailable(fmbr);
      }
    }
  }

  @Test
  public void testRemoveRequestWhileWaitingForFinalResponse() throws Exception {
    initMocks(true, true);

    becomeCoordinatorForTest(gmsJoinLeave);

    installView();

    int viewId = gmsJoinLeave.getView().getViewId();
    System.out.println("Current viewid " + viewId);

    this.removeMember = mockMembers[0];

    processJoinMessage(gmsJoinLeave.getMemberID(), mockMembers[2], 98989);

    waitForViewAndFinalCheckInProgress(viewId);

    this.removeMember = null;

    assertTrue("testFlagForRemovalRequest should be true",
        gmsJoinLeave.getViewCreator().getTestFlagForRemovalRequest());
  }

  @Test
  public void testLeaveRequestWhileWaitingForFinalResponse() throws Exception {
    initMocks(true, true);

    becomeCoordinatorForTest(gmsJoinLeave);

    installView();

    int viewId = gmsJoinLeave.getView().getViewId();
    System.out.println("Current viewid " + viewId);

    this.leaveMember = mockMembers[0];

    processJoinMessage(gmsJoinLeave.getMemberID(), mockMembers[2], 98989);

    waitForViewAndFinalCheckInProgress(viewId);

    this.leaveMember = null;

    assertTrue("testFlagForRemovalRequest should be true",
        gmsJoinLeave.getViewCreator().getTestFlagForRemovalRequest());
  }

  @Test
  public void testMulticastDiscoveryNotAllowed() {
    Services services = mock(Services.class);
    MembershipConfig membershipConfig = mock(MembershipConfig.class);
    when(membershipConfig.getLocators()).thenReturn("");
    when(membershipConfig.getMcastPort()).thenReturn(1234);
    when(membershipConfig.getMcastAddress()).thenReturn("scooby.dooby.doo");
    when(services.getConfig()).thenReturn(membershipConfig);

    GMSJoinLeave joinLeave = new GMSJoinLeave(null);
    try {
      joinLeave.init(services);
      throw new Error(
          "expected a GemFireConfigException to be thrown because no locators are configured");
    } catch (MembershipConfigurationException e) {
      // expected
    }
  }

  // GEODE-8240 could cause this member's identifier to have the wrong version so patch it up
  @Test
  public void repairWrongVersionInView() throws Exception {

    initMocks();

    List<MemberIdentifier> viewmembers =
        Arrays.asList(new MemberIdentifier[] {mockMembers[0], gmsJoinLeaveMemberId});

    final GMSMembershipView<MemberIdentifier> viewWithWrongVersion =
        new GMSMembershipView<>(mockMembers[0], 2, viewmembers);

    // clone member ID
    final MemberIdentifierImpl myMemberIDWithWrongVersion =
        new MemberIdentifierImpl(gmsJoinLeaveMemberId.getMemberData());

    // this test must live in the 1.12 and later lines so pick a pre-1.12 version
    final Version oldVersion = Version.GEODE_1_11_0;
    myMemberIDWithWrongVersion.setVersionObjectForTest(oldVersion);

    viewWithWrongVersion.remove(gmsJoinLeaveMemberId);
    viewWithWrongVersion.add(myMemberIDWithWrongVersion);

    gmsJoinLeave.installView(viewWithWrongVersion);

    assertThat(
        gmsJoinLeave.getView().getCanonicalID(gmsJoinLeaveMemberId).getVersionOrdinalObject())
            .isEqualTo(Version.getCurrentVersion());
  }

  private void becomeCoordinatorForTest(GMSJoinLeave gmsJoinLeave) {
    synchronized (gmsJoinLeave.getViewInstallationLock()) {
      gmsJoinLeave.becomeCoordinator();
    }
  }

  private void installView() throws Exception {
    final int viewInstallationTime = 15000;

    GMSMembershipView oldView = null;
    long giveup = System.currentTimeMillis() + viewInstallationTime;
    while (System.currentTimeMillis() < giveup && oldView == null) {
      Thread.sleep(500);
      oldView = gmsJoinLeave.getView();
    }
    assertTrue(oldView != null); // it should have become coordinator and installed a view

    GMSMembershipView newView = new GMSMembershipView(oldView, oldView.getViewId() + 1);
    newView.add(mockMembers[0]);
    newView.add(mockMembers[1]);
    gmsJoinLeave.installView(newView);
  }

  private void processJoinMessage(MemberIdentifier coordinator,
      MemberIdentifier newMember, int port) {
    JoinRequestMessage reqMsg = new JoinRequestMessage(coordinator, newMember, null, port, 0);
    gmsJoinLeave.processMessage(reqMsg);
  }

  private void processRemoveMessage(MemberIdentifier rMember) {
    RemoveMemberMessage msg =
        new RemoveMemberMessage(gmsJoinLeave.getMemberID(), rMember, "testing");
    msg.setSender(gmsJoinLeave.getMemberID());

    gmsJoinLeave.processMessage(msg);
  }

  private void processLeaveMessage(MemberIdentifier rMember) {
    LeaveRequestMessage msg =
        new LeaveRequestMessage(gmsJoinLeave.getMemberID(), rMember, "testing");
    msg.setSender(rMember);

    gmsJoinLeave.processMessage(msg);
  }
}
