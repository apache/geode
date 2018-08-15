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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.internal.verification.Times;
import org.mockito.verification.Timeout;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.ServiceConfig;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.Services.Stopper;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Authenticator;
import org.apache.geode.distributed.internal.membership.gms.interfaces.HealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Locator;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Messenger;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.SearchState;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.TcpClientWrapper;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.ViewCreator;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.ViewReplyProcessor;
import org.apache.geode.distributed.internal.membership.gms.messages.InstallViewMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinResponseMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.NetworkPartitionMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.RemoveMemberMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.ViewAckMessage;
import org.apache.geode.internal.Version;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class GMSJoinLeaveJUnitTest {
  private Services services;
  private ServiceConfig mockConfig;
  private DistributionConfig mockDistConfig;
  private Authenticator authenticator;
  private HealthMonitor healthMonitor;
  private InternalDistributedMember gmsJoinLeaveMemberId;
  private InternalDistributedMember[] mockMembers;
  private InternalDistributedMember mockOldMember;
  private Properties credentials = new Properties();
  private Messenger messenger;
  private GMSJoinLeave gmsJoinLeave;
  private Manager manager;
  private Stopper stopper;
  private TestLocator testLocator;
  private InternalDistributedMember removeMember = null;
  private InternalDistributedMember leaveMember = null;

  public void initMocks() {
    initMocks(false);
  }

  public void initMocks(boolean enableNetworkPartition) {
    initMocks(enableNetworkPartition, false);
  }

  public void initMocks(boolean enableNetworkPartition, boolean useTestGMSJoinLeave) {
    mockDistConfig = mock(DistributionConfig.class);
    when(mockDistConfig.getEnableNetworkPartitionDetection()).thenReturn(enableNetworkPartition);
    when(mockDistConfig.getSecurityUDPDHAlgo()).thenReturn("");
    mockConfig = mock(ServiceConfig.class);
    when(mockDistConfig.getStartLocator()).thenReturn("localhost[12345]");
    when(mockConfig.getDistributionConfig()).thenReturn(mockDistConfig);
    when(mockDistConfig.getLocators()).thenReturn("localhost[12345]");
    when(mockDistConfig.getMcastPort()).thenReturn(0);
    when(mockDistConfig.getMemberTimeout()).thenReturn(2000);

    authenticator = mock(Authenticator.class);
    gmsJoinLeaveMemberId = new InternalDistributedMember("localhost", 8887);

    messenger = mock(Messenger.class);
    when(messenger.getMemberID()).thenReturn(gmsJoinLeaveMemberId);

    stopper = mock(Stopper.class);
    when(stopper.isCancelInProgress()).thenReturn(false);

    manager = mock(Manager.class);

    healthMonitor = mock(HealthMonitor.class);
    when(healthMonitor.getFailureDetectionPort()).thenReturn(Integer.valueOf(-1));

    services = mock(Services.class);
    when(services.getAuthenticator()).thenReturn(authenticator);
    when(services.getConfig()).thenReturn(mockConfig);
    when(services.getMessenger()).thenReturn(messenger);
    when(services.getCancelCriterion()).thenReturn(stopper);
    when(services.getManager()).thenReturn(manager);
    when(services.getHealthMonitor()).thenReturn(healthMonitor);

    testLocator = new TestLocator();
    when(services.getLocator()).thenReturn(testLocator);

    Timer t = new Timer(true);
    when(services.getTimer()).thenReturn(t);

    mockMembers = new InternalDistributedMember[4];
    for (int i = 0; i < mockMembers.length; i++) {
      mockMembers[i] = new InternalDistributedMember("localhost", 8888 + i);
    }
    mockOldMember = new InternalDistributedMember("localhost", 8700, Version.GFE_56);

    if (useTestGMSJoinLeave) {
      gmsJoinLeave = new GMSJoinLeaveTest();
    } else {
      gmsJoinLeave = new GMSJoinLeave();
    }
    gmsJoinLeave.init(services);
    gmsJoinLeave.start();
    gmsJoinLeave.started();
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
    public void installView(NetView v) {}

    @Override
    public void setIsCoordinator(boolean isCoordinator) {
      this.isCoordinator = isCoordinator;
    }

    public boolean isCoordinator() {
      return isCoordinator;
    }

  }

  @Test
  public void testFindCoordinatorInView() throws Exception {
    initMocks();

    int viewId = 1;
    List<InternalDistributedMember> mbrs = new LinkedList<>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);

    when(services.getMessenger()).thenReturn(messenger);

    // prepare the view
    NetView netView = new NetView(mockMembers[0], viewId, mbrs);
    SearchState state = gmsJoinLeave.searchState;
    state.view = netView;
    state.viewId = netView.getViewId();

    InternalDistributedMember coordinator = mockMembers[2];
    coordinator.setVmViewId(viewId);

    // already tried joining using members 0 and 1
    Set<InternalDistributedMember> set = new HashSet<>();
    mockMembers[0].setVmViewId(viewId - 1);
    set.add(mockMembers[0]);
    mockMembers[1].setVmViewId(viewId - 1);
    set.add(mockMembers[1]);
    state.alreadyTried = set;
    state.hasContactedAJoinedLocator = true;

    // simulate a response being received
    InternalDistributedMember sender = mockMembers[2];
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
  public void testProcessJoinMessageRejectOldMemberVersion() throws IOException {
    initMocks();

    gmsJoinLeave.processMessage(new JoinRequestMessage(mockOldMember, mockOldMember, null, -1, 0));
    assertTrue("JoinRequest should not have been added to view request",
        gmsJoinLeave.getViewRequests().size() == 0);
    verify(messenger).send(isA(JoinResponseMessage.class));
  }

  @Test
  public void testViewWithoutMemberInitiatesForcedDisconnect() throws Exception {
    initMocks();
    GMSJoinLeaveTestHelper.becomeCoordinatorForTest(gmsJoinLeave);
    List<InternalDistributedMember> members = Arrays.asList(mockMembers);
    NetView v = new NetView(mockMembers[0], 2, members);
    InstallViewMessage message = getInstallViewMessage(v, null, false);
    gmsJoinLeave.processMessage(message);
    verify(manager).forceDisconnect(isA(String.class));
  }


  @Test
  public void testProcessJoinMessageWithBadAuthentication() throws IOException {
    initMocks();
    when(services.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(mockMembers[0], credentials))
        .thenThrow(new AuthenticationFailedException("we want to fail auth here"));
    when(services.getMessenger()).thenReturn(messenger);

    gmsJoinLeave
        .processMessage(new JoinRequestMessage(mockMembers[0], mockMembers[0], credentials, -1, 0));
    assertTrue("JoinRequest should not have been added to view request",
        gmsJoinLeave.getViewRequests().size() == 0);
    verify(messenger).send(isA(JoinResponseMessage.class));
  }

  @Test
  public void testProcessJoinMessageWithAuthenticationButNullCredentials() throws IOException {
    initMocks();
    when(services.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(mockMembers[0], null))
        .thenThrow(new AuthenticationFailedException("we want to fail auth here"));
    when(services.getMessenger()).thenReturn(messenger);

    gmsJoinLeave
        .processMessage(new JoinRequestMessage(mockMembers[0], mockMembers[0], null, -1, 0));
    assertTrue("JoinRequest should not have been added to view request",
        gmsJoinLeave.getViewRequests().size() == 0);
    verify(messenger).send(isA(JoinResponseMessage.class));
  }

  // This test does not test the actual join process but rather that the join response gets logged
  @Test
  public void testProcessJoinResponseIsRecorded() throws IOException {
    initMocks();
    when(services.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(mockMembers[0], null))
        .thenThrow(new AuthenticationFailedException("we want to fail auth here"));
    when(services.getMessenger()).thenReturn(messenger);

    JoinResponseMessage[] joinResponse = gmsJoinLeave.getJoinResponseMessage();

    JoinResponseMessage jrm = new JoinResponseMessage(mockMembers[0], new byte[9], 233);
    gmsJoinLeave.processMessage(jrm);
    // this should NOT logs, this is just to inform member succesful joining
    Assert.assertEquals(null, joinResponse[0]);

    jrm = new JoinResponseMessage("rejected...", 0);
    gmsJoinLeave.processMessage(jrm);
    // this should log..
    Assert.assertEquals(jrm, joinResponse[0]);

    gmsJoinLeave.setJoinResponseMessage(null);

    jrm = new JoinResponseMessage(mockMembers[0], new NetView(), 0);
    gmsJoinLeave.processMessage(jrm);
    // this should log..
    Assert.assertEquals(jrm, joinResponse[0]);
  }

  /**
   * prepares and install a view
   *
   */
  private void prepareAndInstallView(InternalDistributedMember coordinator,
      List<InternalDistributedMember> members) throws IOException {
    int viewId = 1;

    when(services.getMessenger()).thenReturn(messenger);

    // prepare the view
    NetView netView = new NetView(coordinator, viewId, members);
    for (InternalDistributedMember member : netView.getMembers()) {
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

  private List<InternalDistributedMember> createMemberList(InternalDistributedMember... members) {
    List<InternalDistributedMember> memberList =
        new ArrayList<InternalDistributedMember>(members.length);
    for (InternalDistributedMember member : members) {
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

    when(healthMonitor.checkIfAvailable(isA(InternalDistributedMember.class), isA(String.class),
        isA(Boolean.class))).thenReturn(true);

    gmsJoinLeave.delayViewCreationForTest(5000); // ensures multiple requests are queued for a view
                                                 // change
    GMSJoinLeaveTestHelper.becomeCoordinatorForTest(gmsJoinLeave);

    Awaitility.await().atMost(viewInstallationTime, MILLISECONDS)
        .until(() -> gmsJoinLeave.getView() != null);

    NetView oldView = gmsJoinLeave.getView();

    NetView newView = new NetView(oldView, oldView.getViewId() + 1);
    newView.add(mockMembers[1]);
    newView.add(mockMembers[2]);
    gmsJoinLeave.installView(newView);

    gmsJoinLeave.memberShutdown(mockMembers[1], "shutting down for test");
    gmsJoinLeave.remove(mockMembers[1], "removing for test");

    Awaitility.await().atMost(viewInstallationTime, MILLISECONDS)
        .until(() -> gmsJoinLeave.getView().getViewId() > newView.getViewId());
    assertFalse(gmsJoinLeave.getView().getCrashedMembers().contains(mockMembers[1]));
  }


  @Test
  public void testRejectOlderView() throws IOException {
    initMocks();
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));

    List<InternalDistributedMember> mbrs = new LinkedList<>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);

    // try to install an older view where viewId < currentView.viewId
    NetView olderNetView = new NetView(mockMembers[0], 0, mbrs);
    InstallViewMessage installViewMessage = getInstallViewMessage(olderNetView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);
    Assert.assertNotEquals(gmsJoinLeave.getView(), olderNetView);
  }

  @Test
  public void testForceDisconnectedFromNewView() throws IOException {
    initMocks(true);// enabledNetworkPartition;
    Manager mockManager = mock(Manager.class);
    when(services.getManager()).thenReturn(mockManager);
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));

    int viewId = 2;
    List<InternalDistributedMember> mbrs = new LinkedList<>();
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);
    mbrs.add(mockMembers[3]);

    // install the view
    NetView netView = new NetView(mockMembers[0], viewId, mbrs);
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
    msg.setSender(new InternalDistributedMember("localhost", 9000));
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
    GMSJoinLeaveTestHelper.becomeCoordinatorForTest(gmsJoinLeave);

    LeaveRequestMessage msg =
        new LeaveRequestMessage(gmsJoinLeave.getMemberID(), mockMembers[0], reason);
    msg.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(msg);
    msg = new LeaveRequestMessage(gmsJoinLeave.getMemberID(), mockMembers[0], reason);
    msg.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(msg);

    waitForViewAndNoRequestsInProgress(7);

    NetView view = gmsJoinLeave.getView();
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
    GMSJoinLeaveTestHelper.becomeCoordinatorForTest(gmsJoinLeave);
    RemoveMemberMessage msg =
        new RemoveMemberMessage(gmsJoinLeave.getMemberID(), mockMembers[0], reason);
    msg.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(msg);
    msg = new RemoveMemberMessage(gmsJoinLeave.getMemberID(), mockMembers[0], reason);
    msg.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(msg);

    waitForViewAndNoRequestsInProgress(7);

    NetView view = gmsJoinLeave.getView();
    assertTrue("expected member to be removed: " + mockMembers[0] + "; view: " + view,
        !view.contains(mockMembers[0]));
    assertTrue(
        "expected member to be in crashedMembers collection: " + mockMembers[0] + "; view: " + view,
        view.getCrashedMembers().contains(mockMembers[0]));
  }

  @Test
  public void testDuplicateJoinRequestDoesNotCauseNewView() throws Exception {
    initMocks();
    when(healthMonitor.checkIfAvailable(isA(InternalDistributedMember.class), isA(String.class),
        isA(Boolean.class))).thenReturn(true);
    gmsJoinLeave.unitTesting.add("noRandomViewChange");
    prepareAndInstallView(gmsJoinLeaveMemberId,
        createMemberList(gmsJoinLeaveMemberId, mockMembers[0]));
    gmsJoinLeave.getView().add(mockMembers[1]);
    GMSJoinLeaveTestHelper.becomeCoordinatorForTest(gmsJoinLeave);
    JoinRequestMessage msg =
        new JoinRequestMessage(gmsJoinLeaveMemberId, mockMembers[2], null, -1, 0);
    msg.setSender(mockMembers[2]);
    gmsJoinLeave.processMessage(msg);
    msg = new JoinRequestMessage(gmsJoinLeaveMemberId, mockMembers[2], null, -1, 0);
    msg.setSender(mockMembers[2]);
    gmsJoinLeave.processMessage(msg);

    waitForViewAndNoRequestsInProgress(7);

    NetView view = gmsJoinLeave.getView();
    assertTrue("expected member to be added: " + mockMembers[2] + "; view: " + view,
        view.contains(mockMembers[2]));
    List<InternalDistributedMember> members = view.getMembers();
    int occurrences = 0;
    for (InternalDistributedMember mbr : members) {
      if (mbr.equals(mockMembers[2])) {
        occurrences += 1;
      }
    }
    assertTrue("expected member to only be in the view once: " + mockMembers[2] + "; view: " + view,
        occurrences == 1);
    verify(healthMonitor, times(5)).checkIfAvailable(isA(InternalDistributedMember.class),
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
    synchronized (gmsJoinLeave.getViewInstallationLock()) {
      gmsJoinLeave.becomeCoordinator();
    }
    prepareAndInstallView(gmsJoinLeaveMemberId,
        createMemberList(gmsJoinLeaveMemberId, mockMembers[0], mockMembers[1]));
    reset(messenger);
    RemoveMemberMessage msg = new RemoveMemberMessage(gmsJoinLeaveMemberId,
        new InternalDistributedMember("localhost", 10000), "removing for test");
    msg.setSender(mockMembers[0]);
    gmsJoinLeave.processMessage(msg);
    verify(messenger).send(isA(RemoveMemberMessage.class));
  }

  @Test
  public void testRemoveRequestCausesForcedDisconnectInRogue() throws Exception {
    initMocks();
    // gmsJoinLeave mistakenly uses an old viewID when joining, making it a rogue member
    gmsJoinLeaveMemberId.setVmViewId(-1);
    InternalDistributedMember previousMemberId =
        new InternalDistributedMember(gmsJoinLeaveMemberId.getId(), gmsJoinLeaveMemberId.getPort());
    previousMemberId.setVmViewId(0);
    NetView view = new NetView(mockMembers[0], 1,
        createMemberList(mockMembers[0], previousMemberId, mockMembers[1]));
    InstallViewMessage viewMessage = new InstallViewMessage(view, 0, true);
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
  public void testViewWithOldIDNotAcceptedAsJoinResponse() throws Exception {
    initMocks();
    when(messenger.isOldMembershipIdentifier(any(DistributedMember.class)))
        .thenReturn(Boolean.TRUE);
    List<InternalDistributedMember> mbrs = new LinkedList<>();
    Set<InternalDistributedMember> shutdowns = new HashSet<>();
    Set<InternalDistributedMember> crashes = new HashSet<>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);
    InternalDistributedMember oldId = new InternalDistributedMember(
        gmsJoinLeaveMemberId.getInetAddress(), gmsJoinLeaveMemberId.getPort());
    oldId.setVmViewId(0);
    mbrs.add(oldId);

    // prepare the view
    NetView netView = new NetView(mockMembers[0], 1, mbrs, shutdowns, crashes);
    gmsJoinLeave.processMessage(new InstallViewMessage(netView, null, true));
    assertEquals(-1, gmsJoinLeaveMemberId.getVmViewId());
    verify(messenger).isOldMembershipIdentifier(isA(DistributedMember.class));
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
    GMSJoinLeaveTestHelper.becomeCoordinatorForTest(gmsJoinLeave);
    Awaitility.await().atMost(20, SECONDS).until(() -> gmsJoinLeave.isCoordinator());
  }

  @Test
  public void testBecomeCoordinator() throws Exception {
    String reason = "testing";
    initMocks();
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));
    NetView view = gmsJoinLeave.getView();
    view.add(gmsJoinLeaveMemberId);
    InternalDistributedMember creator = view.getCreator();
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
    NetView view = gmsJoinLeave.getView();
    view.add(gmsJoinLeaveMemberId);
    InternalDistributedMember creator = view.getCreator();
    RemoveMemberMessage msg = new RemoveMemberMessage(creator, creator, reason);
    msg.setSender(creator);
    gmsJoinLeave.processMessage(msg);
    assertTrue("Expected becomeCoordinator to be invoked", gmsJoinLeave.isCoordinator());
  }

  @Test
  public void testBecomeCoordinatorThroughViewChange() throws Exception {
    initMocks();
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));
    NetView oldView = gmsJoinLeave.getView();
    oldView.add(gmsJoinLeaveMemberId);
    NetView view = new NetView(oldView, oldView.getViewId() + 1);
    InternalDistributedMember creator = view.getCreator();
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
    NetView oldView = gmsJoinLeave.getView();
    oldView.add(gmsJoinLeaveMemberId);
    NetView view = new NetView(oldView, oldView.getViewId() + 1);
    InternalDistributedMember creator = view.getCreator();
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
    NetView oldView = gmsJoinLeave.getView();
    oldView.add(gmsJoinLeaveMemberId);
    InternalDistributedMember creator = oldView.getCreator();
    GMSJoinLeaveTestHelper.becomeCoordinatorForTest(gmsJoinLeave);
    NetView view = new NetView(2, gmsJoinLeave.getView().getViewId() + 1);
    view.setCreator(creator);
    view.add(creator);
    view.add(gmsJoinLeaveMemberId);
    InstallViewMessage msg = getInstallViewMessage(view, creator, false);
    msg.setSender(creator);
    gmsJoinLeave.processMessage(msg);
    assertTrue("Expected it to stop being coordinator", !gmsJoinLeave.isCoordinator());
  }

  private InstallViewMessage getInstallViewMessage(NetView view, Object credentials,
      boolean preparing) {
    InstallViewMessage installViewMessage = new InstallViewMessage(view, credentials, preparing);
    installViewMessage.setSender(gmsJoinLeaveMemberId);
    return installViewMessage;
  }

  @Test
  public void testNetworkPartitionDetected() throws IOException {
    initMocks(true);
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));

    // set up a view with sufficient members, then create a new view
    // where enough weight is lost to cause a network partition

    List<InternalDistributedMember> mbrs = new LinkedList<>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);
    mbrs.add(gmsJoinLeaveMemberId);

    ((GMSMember) mockMembers[1].getNetMember()).setMemberWeight((byte) 20);

    NetView newView = new NetView(mockMembers[0], gmsJoinLeave.getView().getViewId() + 1, mbrs);
    InstallViewMessage installViewMessage = getInstallViewMessage(newView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);

    Set<InternalDistributedMember> crashes = new HashSet<>();
    crashes.add(mockMembers[1]);
    crashes.add(mockMembers[2]);
    mbrs = new LinkedList<>(mbrs);
    mbrs.remove(mockMembers[1]);
    mbrs.remove(mockMembers[2]);
    NetView partitionView =
        new NetView(mockMembers[0], newView.getViewId() + 1, mbrs, Collections.emptySet(), crashes);
    installViewMessage = getInstallViewMessage(partitionView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);

    verify(manager).forceDisconnect(isA(String.class));
    verify(manager).quorumLost(crashes, newView);
  }

  // Possibly modify test to check for network partition message in the force disconnect
  @Test
  public void testNetworkPartitionMessageReceived() throws Exception {
    initMocks();
    GMSJoinLeaveTestHelper.becomeCoordinatorForTest(gmsJoinLeave);
    NetworkPartitionMessage message = new NetworkPartitionMessage();
    gmsJoinLeave.processMessage(message);
    verify(manager).forceDisconnect(isA(String.class));
  }


  @Test
  public void testQuorumLossNotificationWithNetworkPartitionDetectionDisabled() throws IOException {
    initMocks(false);
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));

    // set up a view with sufficient members, then create a new view
    // where enough weight is lost to cause a network partition

    List<InternalDistributedMember> mbrs = new LinkedList<>();
    Set<InternalDistributedMember> shutdowns = new HashSet<>();
    Set<InternalDistributedMember> crashes = new HashSet<>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);
    mbrs.add(gmsJoinLeaveMemberId);

    ((GMSMember) mockMembers[1].getNetMember()).setMemberWeight((byte) 20);

    NetView newView = new NetView(mockMembers[0], gmsJoinLeave.getView().getViewId() + 1, mbrs,
        shutdowns, crashes);
    InstallViewMessage installViewMessage = getInstallViewMessage(newView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);

    crashes = new HashSet<>(crashes);
    crashes.add(mockMembers[1]);
    crashes.add(mockMembers[2]);
    mbrs = new LinkedList<>(mbrs);
    mbrs.remove(mockMembers[1]);
    mbrs.remove(mockMembers[2]);
    NetView partitionView =
        new NetView(mockMembers[0], newView.getViewId() + 1, mbrs, shutdowns, crashes);
    installViewMessage = getInstallViewMessage(partitionView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);

    verify(manager, never()).forceDisconnect(isA(String.class));
    verify(manager).quorumLost(crashes, newView);
  }

  @Test
  public void testConflictingPrepare() throws Exception {
    initMocks(true);
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));

    NetView gmsView = gmsJoinLeave.getView();
    NetView newView = new NetView(gmsView, gmsView.getViewId() + 6);
    InstallViewMessage msg = getInstallViewMessage(newView, null, true);
    gmsJoinLeave.processMessage(msg);

    NetView alternateView = new NetView(gmsView, gmsView.getViewId() + 1);
    msg = getInstallViewMessage(alternateView, null, true);
    gmsJoinLeave.processMessage(msg);

    assertTrue(gmsJoinLeave.getPreparedView().equals(newView));
  }

  @Test
  public void testNoViewAckCausesRemovalMessage() throws Exception {
    initMocks(true);
    when(healthMonitor.checkIfAvailable(isA(InternalDistributedMember.class), isA(String.class),
        isA(Boolean.class))).thenReturn(false);
    prepareAndInstallView(mockMembers[0], createMemberList(mockMembers[0], gmsJoinLeaveMemberId));
    NetView oldView = gmsJoinLeave.getView();
    NetView newView = new NetView(oldView, oldView.getViewId() + 1);

    // the new view will remove the old coordinator (normal shutdown) and add a new member
    // who will not ack the view. This should cause it to be removed from the system
    // with a RemoveMemberMessage
    newView.add(mockMembers[2]);
    newView.remove(mockMembers[0]);

    InstallViewMessage installViewMessage = getInstallViewMessage(newView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);

    // this test's member-timeout * 3
    Awaitility.await().atMost(6, SECONDS)
        .until(() -> gmsJoinLeave.getView().getViewId() != oldView.getViewId());
    assertTrue(gmsJoinLeave.isCoordinator());
    // wait for suspect processing

    verify(healthMonitor, timeout(10000).atLeast(1)).checkIfAvailable(isA(DistributedMember.class),
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
    NetView nextView = gmsJoinLeave.getViewCreator().initialView;

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
    NetView nextView = gmsJoinLeave.getViewCreator().initialView;

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
    InternalDistributedMember otherMember = mockMembers[0];
    gmsJoinLeaveMemberId.setVmKind(ClusterDistributionManager.NORMAL_DM_TYPE);
    List<InternalDistributedMember> members = createMemberList(gmsJoinLeaveMemberId, otherMember);
    prepareAndInstallView(gmsJoinLeaveMemberId, members);
    NetView installedView = gmsJoinLeave.getView();

    gmsJoinLeave.unitTesting.add("noRandomViewChange"); // keep view numbers predictable

    // create a view coming from the locator that conflicts with the installed view
    InternalDistributedMember locatorMemberId = new InternalDistributedMember("localhost",
        mockMembers[mockMembers.length - 1].getPort() + 1);
    locatorMemberId.setVmKind(ClusterDistributionManager.LOCATOR_DM_TYPE);
    List<InternalDistributedMember> newMemberList = new ArrayList<>(members);
    newMemberList.add(locatorMemberId);
    NetView locatorView =
        new NetView(locatorMemberId, installedView.getViewId() + 10, newMemberList);

    // start the process to make our GMSJoinLeave become coordinator. It will send out a view
    // and want an ACK from
    synchronized (gmsJoinLeave.getViewInstallationLock()) {
      gmsJoinLeave.becomeCoordinator();
    }
    Awaitility.await().atMost(30, TimeUnit.SECONDS)
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

    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> gmsJoinLeave.getViewCreator() != null);
    Awaitility.await().atMost(30, TimeUnit.SECONDS)
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
    NetView netView = new NetView(mockMembers[0], 3/* new view id */,
        createMemberList(mockMembers[0], gmsJoinLeaveMemberId, mockMembers[1], mockMembers[3]));
    InstallViewMessage installViewMessage = getInstallViewMessage(netView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);

    // Now coordinator leaves
    gmsJoinLeave.memberShutdown(mockMembers[0], "Shutdown");
    NetView nextView = gmsJoinLeave.getViewCreator().initialView;

    assertTrue(gmsJoinLeave.isCoordinator());
    assertTrue(nextView.getCoordinator().equals(gmsJoinLeaveMemberId));
    Assert.assertFalse(nextView.getMembers().contains(mockMembers[1]));
    Assert.assertFalse(nextView.getMembers().contains(mockMembers[2]));
    assertTrue(nextView.getMembers().contains(mockMembers[3]));
  }

  @Test
  public void testViewBroadcaster() throws Exception {
    initMocks();
    List<InternalDistributedMember> members = new ArrayList<>(Arrays.asList(mockMembers));
    gmsJoinLeaveMemberId.setVmViewId(1);
    members.add(gmsJoinLeaveMemberId);
    prepareAndInstallView(gmsJoinLeaveMemberId, members);
    GMSJoinLeaveTestHelper.becomeCoordinatorForTest(gmsJoinLeave);
    GMSJoinLeave.ViewBroadcaster b = gmsJoinLeave.new ViewBroadcaster();
    b.run();
    verify(messenger).sendUnreliably(isA(InstallViewMessage.class));
  }

  private void installView(int viewId, InternalDistributedMember coordinator,
      List<InternalDistributedMember> members) throws IOException {
    // prepare the view
    NetView netView = new NetView(coordinator, viewId, members);
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
      Awaitility.await().atMost(2 * ServiceConfig.MEMBER_REQUEST_COLLECTION_INTERVAL, MILLISECONDS)
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
      Set<InternalDistributedMember> recips = new HashSet<>();
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
      Set<InternalDistributedMember> recips = new HashSet<>();
      recips.add(mockMembers[0]);
      recips.add(mockMembers[1]);
      recips.add(mockMembers[2]);
      recips.add(mockMembers[3]);
      ViewReplyProcessor prepareProcessor = gmsJoinLeave.getPrepareViewReplyProcessor();
      prepareProcessor.initialize(1, recips);
      assertTrue("Prepare processor should be waiting ",
          gmsJoinLeave.testPrepareProcessorWaiting());
      Set<InternalDistributedMember> pendingLeaves = new HashSet<>();
      pendingLeaves.add(mockMembers[0]);
      Set<InternalDistributedMember> pendingRemovals = new HashSet<>();
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
  // InternalDistributedMember ids = new InternalDistributedMember("localhost", 97898);
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
      installView(1, gmsJoinLeaveMemberId, createMemberList(mockMembers[0], mockMembers[1],
          mockMembers[2], gmsJoinLeaveMemberId, mockMembers[3]));
      for (int i = 1; i < 4; i++) {
        RemoveMemberMessage msg =
            new RemoveMemberMessage(gmsJoinLeaveMemberId, mockMembers[i], "crashed");
        msg.setSender(gmsJoinLeaveMemberId);
        gmsJoinLeave.processMessage(msg);
      }
      Timeout to = new Timeout(3 * ServiceConfig.MEMBER_REQUEST_COLLECTION_INTERVAL, new Times(1));
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
      Timeout to = new Timeout(2 * ServiceConfig.MEMBER_REQUEST_COLLECTION_INTERVAL, never());
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

      Awaitility.await("waiting for view creator to stop").atMost(5000, MILLISECONDS)
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
    NetView preparedView =
        new NetView(gmsJoinLeave.getView(), gmsJoinLeave.getView().getViewId() + 5);
    mockMembers[1].setVmViewId(preparedView.getViewId());
    preparedView.add(mockMembers[1]);

    InstallViewMessage msg = getInstallViewMessage(preparedView, null, true);
    gmsJoinLeave.processMessage(msg);

    GMSJoinLeaveTestHelper.becomeCoordinatorForTest(gmsJoinLeave);

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

    Awaitility.await("view creator finishes").atMost(30, SECONDS).until(() -> vc.waiting);
    NetView newView = gmsJoinLeave.getView();
    System.out.println("new view is " + newView);
    assertTrue(newView.contains(mockMembers[1]));
    assertTrue(newView.getViewId() > preparedView.getViewId());
  }

  @Test
  public void testPublicKeyForNewMemberFromPreparedViewIsInstalledInNewView() throws Exception {
    initMocks(false);
    InternalDistributedMember newMember = mockMembers[1];

    prepareAndInstallView(gmsJoinLeaveMemberId,
        createMemberList(gmsJoinLeaveMemberId, mockMembers[0]));
    // a new member is joining
    NetView preparedView =
        new NetView(gmsJoinLeave.getView(), gmsJoinLeave.getView().getViewId() + 5);
    for (InternalDistributedMember member : preparedView.getMembers()) {
      preparedView.setPublicKey(member, member.toString());
    }
    newMember.setVmViewId(preparedView.getViewId());
    preparedView.add(newMember);
    preparedView.setPublicKey(newMember, newMember.toString());

    InstallViewMessage msg = getInstallViewMessage(preparedView, null, true);
    gmsJoinLeave.processMessage(msg);

    GMSJoinLeaveTestHelper.becomeCoordinatorForTest(gmsJoinLeave);

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

    Awaitility.await("view creator finishes").atMost(30, SECONDS).until(() -> vc.waiting);
    NetView newView = gmsJoinLeave.getView();
    System.out.println("new view is " + newView);
    assertTrue(newView.contains(newMember));
    assertNotNull(newView.getPublicKey(newMember));
  }

  private NetView createView() {
    List<InternalDistributedMember> mbrs = new LinkedList<>();
    Set<InternalDistributedMember> shutdowns = new HashSet<>();
    Set<InternalDistributedMember> crashes = new HashSet<>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);
    mbrs.add(gmsJoinLeaveMemberId);

    // prepare the view
    NetView netView = new NetView(mockMembers[0], 1, mbrs, shutdowns, crashes);
    return netView;
  }

  @Test
  public void testCoordinatorFindRequestSuccess() throws Exception {
    initMocks(false);
    HashSet<InternalDistributedMember> registrants = new HashSet<>();
    registrants.add(mockMembers[0]);
    FindCoordinatorResponse fcr = new FindCoordinatorResponse(mockMembers[0], mockMembers[0], false,
        null, registrants, false, true, null);
    NetView view = createView();

    TcpClientWrapper tcpClientWrapper = mock(TcpClientWrapper.class);
    gmsJoinLeave.setTcpClientWrapper(tcpClientWrapper);

    when(tcpClientWrapper.sendCoordinatorFindRequest(isA(InetSocketAddress.class),
        isA(FindCoordinatorRequest.class), isA(Integer.class))).thenReturn(fcr);

    boolean foundCoordinator = gmsJoinLeave.findCoordinator();
    assertTrue(gmsJoinLeave.searchState.toString(), foundCoordinator);
    assertEquals(gmsJoinLeave.searchState.possibleCoordinator, mockMembers[0]);
  }

  @Test
  public void testCoordinatorFindRequestFailure() throws Exception {
    try {
      initMocks(false);
      HashSet<InternalDistributedMember> registrants = new HashSet<>();
      registrants.add(mockMembers[0]);
      FindCoordinatorResponse fcr = new FindCoordinatorResponse(mockMembers[0], mockMembers[0],
          false, null, registrants, false, true, null);
      NetView view = createView();
      JoinResponseMessage jrm = new JoinResponseMessage(mockMembers[0], view, 0);
      gmsJoinLeave.setJoinResponseMessage(jrm);

      TcpClientWrapper tcpClientWrapper = mock(TcpClientWrapper.class);
      gmsJoinLeave.setTcpClientWrapper(tcpClientWrapper);
      FindCoordinatorRequest fcreq =
          new FindCoordinatorRequest(gmsJoinLeaveMemberId, new HashSet<>(), -1, null, 0, "");
      int connectTimeout = (int) services.getConfig().getMemberTimeout() * 2;
      // passing wrong port here, so ot will fail
      when(tcpClientWrapper.sendCoordinatorFindRequest(new InetSocketAddress("localhost", 12346),
          fcreq, connectTimeout)).thenReturn(fcr);
      assertFalse("Should not be able to join ", gmsJoinLeave.join());
    } finally {

    }
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
    GMSJoinLeaveTest() {
      super();
    }

    @Override
    boolean checkIfAvailable(InternalDistributedMember fmbr) {
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

    GMSJoinLeaveTestHelper.becomeCoordinatorForTest(gmsJoinLeave);

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

    GMSJoinLeaveTestHelper.becomeCoordinatorForTest(gmsJoinLeave);

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

  private void installView() throws Exception {
    final int viewInstallationTime = 15000;

    NetView oldView = null;
    long giveup = System.currentTimeMillis() + viewInstallationTime;
    while (System.currentTimeMillis() < giveup && oldView == null) {
      Thread.sleep(500);
      oldView = gmsJoinLeave.getView();
    }
    assertTrue(oldView != null); // it should have become coordinator and installed a view

    NetView newView = new NetView(oldView, oldView.getViewId() + 1);
    newView.add(mockMembers[0]);
    newView.add(mockMembers[1]);
    gmsJoinLeave.installView(newView);
  }

  private void processJoinMessage(InternalDistributedMember coordinator,
      InternalDistributedMember newMember, int port) {
    JoinRequestMessage reqMsg = new JoinRequestMessage(coordinator, newMember, null, port, 0);
    gmsJoinLeave.processMessage(reqMsg);
  }

  private void processRemoveMessage(InternalDistributedMember rMember) {
    RemoveMemberMessage msg =
        new RemoveMemberMessage(gmsJoinLeave.getMemberID(), rMember, "testing");
    msg.setSender(gmsJoinLeave.getMemberID());

    gmsJoinLeave.processMessage(msg);
  }

  private void processLeaveMessage(InternalDistributedMember rMember) {
    LeaveRequestMessage msg =
        new LeaveRequestMessage(gmsJoinLeave.getMemberID(), rMember, "testing");
    msg.setSender(rMember);

    gmsJoinLeave.processMessage(msg);
  }
}
