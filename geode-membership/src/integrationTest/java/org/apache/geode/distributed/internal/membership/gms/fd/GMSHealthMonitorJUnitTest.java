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
package org.apache.geode.distributed.internal.membership.gms.fd;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.distributed.internal.membership.api.MemberData;
import org.apache.geode.distributed.internal.membership.api.MemberDataBuilder;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactoryImpl;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.gms.DefaultMembershipStatistics;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.MemberIdentifierImpl;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.Services.Stopper;
import org.apache.geode.distributed.internal.membership.gms.fd.GMSHealthMonitor.ClientSocketHandler;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Messenger;
import org.apache.geode.distributed.internal.membership.gms.messages.FinalCheckPassedMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.HeartbeatMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.HeartbeatRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.SuspectMembersMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.SuspectRequest;
import org.apache.geode.distributed.internal.membership.gms.util.MemberIdentifierUtil;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreatorImpl;
import org.apache.geode.internal.serialization.BufferDataOutputStream;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.serialization.internal.DSFIDSerializerImpl;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class GMSHealthMonitorJUnitTest {

  private Services services;
  private MembershipConfig mockConfig;
  private List<MemberIdentifier> mockMembers;
  private Messenger messenger;
  private JoinLeave joinLeave;
  private GMSHealthMonitor gmsHealthMonitor;
  private Manager manager;
  final long memberTimeout = 1000l;
  private int[] portRange = new int[] {0, 65535};
  private boolean useGMSHealthMonitorTestClass = false;
  private boolean simulateHeartbeatInGMSHealthMonitorTestClass = true;
  private boolean allowSelfCheckToSucceed = true;
  private final int myAddressIndex = 3;
  private DSFIDSerializerImpl dsfidSerializer;

  @Before
  public void initMocks() throws MemberStartupException {
    // ensure that Geode's serialization and version are initialized
    Version currentVersion = Version.CURRENT;
    dsfidSerializer = new DSFIDSerializerImpl();
    Services.registerSerializables(dsfidSerializer);

    // System.setProperty("gemfire.bind-address", "localhost");
    mockConfig = mock(MembershipConfig.class);
    messenger = mock(Messenger.class);
    joinLeave = mock(JoinLeave.class);
    manager = mock(Manager.class);
    services = mock(Services.class);
    Stopper stopper = mock(Stopper.class);

    when(mockConfig.getMemberTimeout()).thenReturn(memberTimeout);
    when(mockConfig.getMembershipPortRange()).thenReturn(portRange);
    when(services.getConfig()).thenReturn(mockConfig);
    when(services.getMessenger()).thenReturn(messenger);
    when(services.getJoinLeave()).thenReturn(joinLeave);
    when(services.getCancelCriterion()).thenReturn(stopper);
    when(services.getManager()).thenReturn(manager);
    when(services.getStatistics()).thenReturn(new DefaultMembershipStatistics());
    when(services.getTimer()).thenReturn(new Timer("Geode Membership Timer", true));
    when(stopper.isCancelInProgress()).thenReturn(false);
    when(services.getMemberFactory()).thenReturn(new MemberIdentifierFactoryImpl());

    if (mockMembers == null) {
      mockMembers = new ArrayList<>();
      for (int i = 0; i < 7; i++) {
        MemberIdentifier mbr = MemberIdentifierUtil.createMemberID(8888 + i);

        if (i == 0 || i == 1) {
          mbr.setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);
          mbr.setPreferredForCoordinator(true);
        }
        mockMembers.add(mbr);
      }
    }
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(myAddressIndex));
    when(messenger.getMemberID()).thenReturn(mockMembers.get(myAddressIndex));
    gmsHealthMonitor = new GMSHealthMonitorTest();
    gmsHealthMonitor.init(services);
    gmsHealthMonitor.start();
  }

  @After
  public void tearDown() {
    gmsHealthMonitor.stop();
    // System.getProperties().remove("gemfire.bind-address");
  }

  @Test
  public void testHMServiceStarted() throws Exception {

    MemberIdentifier mbr = MemberIdentifierUtil.createMemberID(12345);
    mbr.setVmViewId(1);
    when(messenger.getMemberID()).thenReturn(mbr);
    gmsHealthMonitor.started();

    gmsHealthMonitor.processMessage(new HeartbeatRequestMessage(mbr, 1));
    verify(messenger, atLeastOnce()).send(any(HeartbeatMessage.class));
    assertEquals(1, gmsHealthMonitor.getStats().getHeartbeatRequestsReceived());
    assertEquals(1, gmsHealthMonitor.getStats().getHeartbeatsSent());
  }

  @Test
  public void testHMServiceHandlesShutdownRace() throws IOException, Exception {
    // The health monitor starts a thread to monitor the tcp socket, both that thread and the
    // stopServices call will attempt to shut down the socket during a normal close. This test tries
    // to create a problematic ordering to make sure we still shutdown properly.
    ((GMSHealthMonitorTest) gmsHealthMonitor).useBlockingSocket = true;
    gmsHealthMonitor.started();

    gmsHealthMonitor.stop();
  }

  /**
   * checks who is next neighbor
   */
  @Test
  public void testHMNextNeighborVerify() throws Exception {
    installAView();
    assertEquals(mockMembers.get(myAddressIndex + 1), gmsHealthMonitor.getNextNeighbor());
  }

  @Test
  public void testHMNextNeighborAfterTimeout() throws Exception {
    System.out.println("testHMNextNeighborAfterTimeout starting");

    installAView();
    MemberIdentifier initialNeighbor = mockMembers.get(myAddressIndex + 1);

    await("wait for new neighbor")
        .until(() -> gmsHealthMonitor.getNextNeighbor() != initialNeighbor);
    MemberIdentifier neighbor = gmsHealthMonitor.getNextNeighbor();

    // neighbor should change. In order to not be a flaky test we don't demand
    // that it be myAddressIndex+2 but just require that the neighbor being
    // monitored has changed
    System.out.println("testHMNextNeighborAfterTimeout ending");
    Assert.assertNotNull(gmsHealthMonitor.getView());
    Assert.assertNotEquals("neighbor to not be " + neighbor + "; my ID is "
        + mockMembers.get(myAddressIndex) + ";  view=" + gmsHealthMonitor.getView(),
        initialNeighbor, neighbor);
  }

  /**
   * it checks neighbor before member-timeout, it should be same
   */

  @Test
  public void testHMNextNeighborBeforeTimeout() throws Exception {
    long startTime = System.currentTimeMillis();
    installAView();
    final MemberIdentifier neighbor = gmsHealthMonitor.getNextNeighbor();
    System.out.println("next neighbor is " + neighbor + "\nmy address is "
        + mockMembers.get(myAddressIndex) + "\nview is " + joinLeave.getView());
    assertEquals(mockMembers.get(myAddressIndex + 1), neighbor);
    await("wait for new neighbor")
        .until(() -> gmsHealthMonitor.getNextNeighbor() != neighbor);
    long endTime = System.currentTimeMillis();

    // member-timeout is 1000 ms. We initiate a check and choose
    // a new neighbor at 500 ms
    assertThat(memberTimeout / GMSHealthMonitor.LOGICAL_INTERVAL)
        .isLessThanOrEqualTo(endTime - startTime);
  }

  /***
   * checks whether member-check thread sends suspectMembers message
   */
  @Test
  public void testSuspectMembersCalledThroughMemberCheckThread() throws Exception {
    installAView();

    // when the view is installed we start a heartbeat timeout. After
    // that expires we request a heartbeat
    await().until(() -> (gmsHealthMonitor.isSuspectMember(mockMembers.get(myAddressIndex + 1))) &&
        (gmsHealthMonitor.getStats().getHeartbeatRequestsSent() > 0) &&
        (gmsHealthMonitor.getStats().getSuspectsSent() > 0));

    System.out.println("testSuspectMembersCalledThroughMemberCheckThread ending");
  }

  private GMSMembershipView installAView() throws Exception {
    GMSMembershipView v = new GMSMembershipView(mockMembers.get(0), 2, mockMembers);

    // 3rd is current member
    when(messenger.getMemberID()).thenReturn(mockMembers.get(myAddressIndex));
    gmsHealthMonitor.started();


    gmsHealthMonitor.installView(v);

    return v;
  }

  private void setFailureDetectionPorts(GMSMembershipView v) {
    java.util.Iterator<MemberIdentifier> itr = mockMembers.iterator();

    int port = 7899;
    while (itr.hasNext()) {
      v.setFailureDetectionPort(itr.next(), port++);
    }
  }

  /***
   * checks ping thread didn't sends suspectMembers message before timeout
   */
  @Test
  public void testSuspectMembersNotCalledThroughPingThreadBeforeTimeout() throws Exception {
    long startTime = System.currentTimeMillis();
    installAView();
    MemberIdentifier neighbor = gmsHealthMonitor.getNextNeighbor();

    await().until(() -> gmsHealthMonitor.isSuspectMember(neighbor));
    long endTime = System.currentTimeMillis();

    // member-timeout is 1000 ms
    // plus 100 ms for ack
    assertThat(memberTimeout + 100).isLessThanOrEqualTo(endTime - startTime);
  }

  /***
   * Checks whether suspect thread sends suspectMembers message
   */
  @Test
  public void testSuspectMembersCalledThroughSuspectThread() throws Exception {
    installAView();

    gmsHealthMonitor.suspect(mockMembers.get(1), "Not responding");

    await().untilAsserted(
        () -> verify(messenger, atLeastOnce()).send(any(SuspectMembersMessage.class)));

    Assert.assertTrue(gmsHealthMonitor.getStats().getSuspectsSent() > 0);
  }

  /***
   * Checks suspect thread doesn't sends suspectMembers message before timeout
   */
  @Test
  public void testSuspectMembersNotCalledThroughSuspectThreadBeforeTimeout() throws Exception {
    installAView();

    gmsHealthMonitor.suspect(mockMembers.get(1), "Not responding");

    verify(messenger, atLeastOnce()).send(isA(SuspectMembersMessage.class));
    Assert.assertTrue(gmsHealthMonitor.getStats().getSuspectsSent() > 0);
  }

  /***
   * Send remove member message after doing final check, ping Timeout
   */
  @Test
  public void testRemoveMemberCalled() throws Exception {
    System.out.println("testRemoveMemberCalled starting");
    GMSMembershipView v = new GMSMembershipView(mockMembers.get(0), 2, mockMembers);

    // 3rd is current member
    when(messenger.getMemberID()).thenReturn(mockMembers.get(0)); // coordinator and local member
    gmsHealthMonitor.started();

    gmsHealthMonitor.installView(v);

    ArrayList<MemberIdentifier> recipient = new ArrayList<MemberIdentifier>();
    recipient.add(mockMembers.get(0));
    ArrayList<SuspectRequest> as = new ArrayList<SuspectRequest>();
    SuspectRequest sr = new SuspectRequest(mockMembers.get(1), "Not Responding");// removing member
                                                                                 // 1
    as.add(sr);
    SuspectMembersMessage sm = new SuspectMembersMessage(recipient, as);
    sm.setSender(mockMembers.get(0));

    gmsHealthMonitor.processMessage(sm);

    await("waiting for remove(member) to be invoked").untilAsserted(() -> {
      verify(joinLeave, atLeastOnce()).remove(any(MemberIdentifier.class),
          any(String.class));
    });
    Assert.assertTrue(gmsHealthMonitor.getStats().getSuspectsReceived() > 0);
  }

  /***
   * Shouldn't send remove member message before doing final check, or before ping Timeout
   */
  @Test
  public void testRemoveMemberNotCalledBeforeTimeout() throws Exception {
    System.out.println("testRemoveMemberNotCalledBeforeTimeout starting");
    GMSMembershipView v = new GMSMembershipView(mockMembers.get(0), 2, mockMembers);

    // 3rd is current member
    when(messenger.getMemberID()).thenReturn(mockMembers.get(0)); // coordinator and local member
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(0)); // coordinator and local member
    gmsHealthMonitor.started();

    gmsHealthMonitor.installView(v);

    ArrayList<MemberIdentifier> recipient = new ArrayList<MemberIdentifier>();
    recipient.add(mockMembers.get(0));
    ArrayList<SuspectRequest> as = new ArrayList<SuspectRequest>();
    SuspectRequest sr = new SuspectRequest(mockMembers.get(1), "Not Responding");// removing member
                                                                                 // 1
    as.add(sr);
    SuspectMembersMessage sm = new SuspectMembersMessage(recipient, as);
    sm.setSender(mockMembers.get(0));

    long preProcess = System.currentTimeMillis();
    gmsHealthMonitor.processMessage(sm);

    await("waiting for remove(member) to be invoked")
        .untilAsserted(
            () -> verify(joinLeave, atLeastOnce()).remove(any(MemberIdentifier.class),
                any(String.class)));
    long postRemove = System.currentTimeMillis();

    assertThat(memberTimeout).isLessThanOrEqualTo(postRemove - preProcess);
  }

  /***
   * Send remove member message after doing final check for coordinator, ping timeout This test
   * trying to remove coordinator
   */
  @Test
  public void testRemoveMemberCalledAfterDoingFinalCheckOnCoordinator() throws Exception {

    GMSMembershipView v = new GMSMembershipView(mockMembers.get(0), 2, mockMembers);

    // preferred coordinators are 0 and 1
    when(messenger.getMemberID()).thenReturn(mockMembers.get(1));// next preferred coordinator
    gmsHealthMonitor.started();

    gmsHealthMonitor.installView(v);

    ArrayList<MemberIdentifier> recipient = new ArrayList<MemberIdentifier>();
    recipient.add(mockMembers.get(0));
    recipient.add(mockMembers.get(1));
    ArrayList<SuspectRequest> as = new ArrayList<SuspectRequest>();
    SuspectRequest sr = new SuspectRequest(mockMembers.get(0), "Not Responding");// removing
                                                                                 // coordinator
    as.add(sr);
    SuspectMembersMessage sm = new SuspectMembersMessage(recipient, as);
    sm.setSender(mockMembers.get(myAddressIndex + 1));// member 4 sends suspect message

    gmsHealthMonitor.processMessage(sm);

    await("waiting for remove(member) to be invoked").untilAsserted(
        () -> verify(joinLeave, atLeastOnce()).remove(any(MemberIdentifier.class),
            any(String.class)));

    Assert.assertTrue(gmsHealthMonitor.getStats().getSuspectsReceived() > 0);
  }

  @Test
  public void testCheckIfAvailableWithSimulatedHeartBeat() throws Exception {
    GMSMembershipView v = installAView();

    MemberIdentifier memberToCheck = mockMembers.get(1);
    HeartbeatMessage fakeHeartbeat = new HeartbeatMessage();
    fakeHeartbeat.setSender(memberToCheck);
    when(messenger.send(any(HeartbeatRequestMessage.class))).then(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        gmsHealthMonitor.processMessage(fakeHeartbeat);
        return null;
      }
    });

    boolean retVal = gmsHealthMonitor.checkIfAvailable(memberToCheck, "Not responding", true);
    assertTrue("CheckIfAvailable should have return true", retVal);
  }

  @Test
  public void testCheckIfAvailableWithSimulatedHeartBeatWithTcpCheck() throws Exception {
    System.out.println("testCheckIfAvailableWithSimulatedHeartBeatWithTcpCheck");
    useGMSHealthMonitorTestClass = true;

    try {
      GMSMembershipView v = installAView();

      setFailureDetectionPorts(v);

      MemberIdentifier memberToCheck = mockMembers.get(1);

      boolean retVal = gmsHealthMonitor.checkIfAvailable(memberToCheck, "Not responding", true);
      assertTrue("CheckIfAvailable should have return true", retVal);
    } finally {
      useGMSHealthMonitorTestClass = false;
    }
  }

  @Test
  public void testMemberIsExaminedAgainAfterPassingAvailabilityCheck() throws Exception {
    // use the test health monitor's availability check for the first round of suspect processing
    // but then turn it off so that a subsequent round is performed and fails to get a heartbeat
    useGMSHealthMonitorTestClass = true;

    try {
      GMSMembershipView v = installAView();

      setFailureDetectionPorts(v);

      MemberIdentifier memberToCheck = mockMembers.get(1);

      boolean retVal = gmsHealthMonitor.checkIfAvailable(memberToCheck, "Not responding", true);
      assertTrue("CheckIfAvailable should have return true", retVal);

      // memberToCheck should be suspected again since it's not sending heartbeats and then
      // it should fail an availability check and cause removal of the member
      useGMSHealthMonitorTestClass = false;
      await().untilAsserted(() -> verify(joinLeave, atLeastOnce()).remove(memberToCheck,
          "Member isn't responding to heartbeat requests"));
    } finally {
      useGMSHealthMonitorTestClass = false;
    }
  }

  @Test
  public void testNeighborRemainsSameAfterSuccessfulFinalCheck() throws Exception {
    useGMSHealthMonitorTestClass = true;

    try {
      GMSMembershipView v = installAView();

      setFailureDetectionPorts(v);

      MemberIdentifier memberToCheck = gmsHealthMonitor.getNextNeighbor();

      gmsHealthMonitor.setNextNeighbor(v, memberToCheck);
      assertNotEquals(memberToCheck, gmsHealthMonitor.getNextNeighbor());

      boolean retVal = gmsHealthMonitor.checkIfAvailable(memberToCheck, "Not responding", true);

      assertTrue("CheckIfAvailable should have return true", retVal);
      // we should now be watching the same member
      assertEquals(memberToCheck, gmsHealthMonitor.getNextNeighbor());

    } finally {
      useGMSHealthMonitorTestClass = false;
    }
  }


  @Test
  public void testNeighborChangesAfterFailedFinalCheck() throws Exception {
    useGMSHealthMonitorTestClass = true;
    simulateHeartbeatInGMSHealthMonitorTestClass = false;

    try {
      GMSMembershipView v = installAView();

      setFailureDetectionPorts(v);

      MemberIdentifier memberToCheck = gmsHealthMonitor.getNextNeighbor();

      boolean retVal = gmsHealthMonitor.checkIfAvailable(memberToCheck, "Not responding", true);

      assertFalse("checkIfAvailable should have return false", retVal);
      // we should now be watching the same member
      int failedIndex = v.getMembers().indexOf(memberToCheck);
      assertEquals("neighbor was " + gmsHealthMonitor.getNextNeighbor() + " but expected "
          + mockMembers.get(failedIndex + 1), mockMembers.get(failedIndex + 1),
          gmsHealthMonitor.getNextNeighbor());

    } finally {
      useGMSHealthMonitorTestClass = false;
      simulateHeartbeatInGMSHealthMonitorTestClass = true;
    }
  }


  @Test
  public void testExonerationMessageIsSentAfterSuccessfulFinalCheck() throws Exception {
    useGMSHealthMonitorTestClass = true;

    try {
      GMSMembershipView v = installAView();

      setFailureDetectionPorts(v);

      MemberIdentifier memberToCheck = gmsHealthMonitor.getNextNeighbor();

      gmsHealthMonitor.setNextNeighbor(v, memberToCheck);
      assertNotEquals(memberToCheck, gmsHealthMonitor.getNextNeighbor());

      boolean retVal = gmsHealthMonitor.inlineCheckIfAvailable(mockMembers.get(0), v, true,
          memberToCheck, "Not responding");

      assertTrue("CheckIfAvailable should have return true", retVal);
      verify(messenger, atLeastOnce()).send(isA(FinalCheckPassedMessage.class));

    } finally {
      useGMSHealthMonitorTestClass = false;
    }
  }

  @Test
  public void testExonerationMessageIsNotSentToVersion_1_3() throws Exception {
    // versions older than 1.4 don't know about the FinalCheckPassedMessage class
    useGMSHealthMonitorTestClass = true;

    try {
      GMSMembershipView v = installAView();

      setFailureDetectionPorts(v);

      MemberIdentifier memberToCheck = gmsHealthMonitor.getNextNeighbor();

      gmsHealthMonitor.setNextNeighbor(v, memberToCheck);
      assertNotEquals(memberToCheck, gmsHealthMonitor.getNextNeighbor());

      ((MemberIdentifierImpl) mockMembers.get(0)).setVersionObjectForTest(Version.GEODE_1_3_0);
      boolean retVal = gmsHealthMonitor.inlineCheckIfAvailable(mockMembers.get(0), v, true,
          memberToCheck, "Not responding");

      assertTrue("CheckIfAvailable should have return true", retVal);
      verify(messenger, never()).send(isA(FinalCheckPassedMessage.class));

    } finally {
      useGMSHealthMonitorTestClass = false;
    }
  }

  @Test
  public void testFinalCheckPassedMessageCanBeSerializedAndDeserialized()
      throws IOException, ClassNotFoundException {
    BufferDataOutputStream BufferDataOutputStream =
        new BufferDataOutputStream(500, Version.CURRENT);
    FinalCheckPassedMessage message =
        new FinalCheckPassedMessage(mockMembers.get(0), mockMembers.get(1));
    dsfidSerializer.getObjectSerializer().writeObject(message, BufferDataOutputStream);
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(BufferDataOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    message = dsfidSerializer.getObjectDeserializer().readObject(dataInputStream);
    assertEquals(mockMembers.get(1), message.getSuspect());
  }



  @Test
  public void testInitiatorRewatchesSuspectAfterSuccessfulFinalCheck() throws Exception {
    GMSMembershipView v = installAView();

    setFailureDetectionPorts(v);

    MemberIdentifier memberToCheck = gmsHealthMonitor.getNextNeighbor();
    gmsHealthMonitor.memberSuspected(mockMembers.get(0), memberToCheck, "Not responding");
    assertTrue(gmsHealthMonitor.isSuspectMember(memberToCheck));
    gmsHealthMonitor.processMessage(new FinalCheckPassedMessage(mockMembers.get(0), memberToCheck));
    assertFalse(gmsHealthMonitor.isSuspectMember(memberToCheck));
  }


  @Test
  public void testFinalCheckFailureLeavesMemberAsSuspect() throws Exception {
    useGMSHealthMonitorTestClass = true;
    simulateHeartbeatInGMSHealthMonitorTestClass = false;

    GMSMembershipView v = installAView();

    setFailureDetectionPorts(v);

    MemberIdentifier memberToCheck = gmsHealthMonitor.getNextNeighbor();
    boolean available = gmsHealthMonitor.checkIfAvailable(memberToCheck, "Not responding", true);
    assertFalse(available);
    verify(joinLeave).remove(isA(MemberIdentifier.class), isA(String.class));
    assertTrue(gmsHealthMonitor.isSuspectMember(memberToCheck));
  }

  @Test
  public void testFailedSelfCheckRemovesMemberAsSuspect() throws Exception {
    useGMSHealthMonitorTestClass = true;
    simulateHeartbeatInGMSHealthMonitorTestClass = false;
    allowSelfCheckToSucceed = false;

    GMSMembershipView v = installAView();

    setFailureDetectionPorts(v);

    MemberIdentifier memberToCheck = gmsHealthMonitor.getNextNeighbor();
    gmsHealthMonitor.stopServer();
    boolean available = gmsHealthMonitor.checkIfAvailable(memberToCheck, "Not responding", false);
    assertTrue(available);
    verify(joinLeave, never()).remove(isA(MemberIdentifier.class), isA(String.class));
    assertTrue(((GMSHealthMonitorTest) gmsHealthMonitor).availabilityCheckedMembers
        .contains(memberToCheck));
    assertTrue(((GMSHealthMonitorTest) gmsHealthMonitor).availabilityCheckedMembers
        .contains(joinLeave.getMemberID()));
  }

  /**
   * a failed availablility check should initiate suspect processing
   */
  @Test
  public void testFailedCheckIfAvailableDoesNotRemoveMember() throws Exception {
    useGMSHealthMonitorTestClass = true;
    simulateHeartbeatInGMSHealthMonitorTestClass = false;

    GMSMembershipView v = installAView();

    setFailureDetectionPorts(v);

    MemberIdentifier memberToCheck = gmsHealthMonitor.getNextNeighbor();
    boolean available = gmsHealthMonitor.checkIfAvailable(memberToCheck, "Not responding", false);
    assertFalse(available);
    verify(joinLeave, never()).remove(isA(MemberIdentifier.class), isA(String.class));
    assertTrue(gmsHealthMonitor.isSuspectMember(memberToCheck));
  }


  /**
   * Same test as above but with request to initiate removal
   */
  @Test
  public void testFailedCheckIfAvailableRemovesMember() throws Exception {
    useGMSHealthMonitorTestClass = true;
    simulateHeartbeatInGMSHealthMonitorTestClass = false;

    GMSMembershipView v = installAView();

    setFailureDetectionPorts(v);

    MemberIdentifier memberToCheck = gmsHealthMonitor.getNextNeighbor();
    boolean available = gmsHealthMonitor.checkIfAvailable(memberToCheck, "Not responding", true);
    assertFalse(available);
    verify(joinLeave).remove(isA(MemberIdentifier.class), isA(String.class));
  }

  /*
   * The next two tests are like the previous two except the next two do not set up failure
   * detection ports.
   */

  @Test
  public void testFailedCheckIfAvailableWithoutFailureDetectionPortDoesNotRemoveMember()
      throws Exception {
    useGMSHealthMonitorTestClass = true;
    simulateHeartbeatInGMSHealthMonitorTestClass = false;

    final long checkRequestsSentBefore = services.getStatistics().getUdpFinalCheckRequestsSent();

    GMSMembershipView v = installAView();

    MemberIdentifier memberToCheck = gmsHealthMonitor.getNextNeighbor();
    boolean available = gmsHealthMonitor.checkIfAvailable(memberToCheck, "Not responding", false);
    assertFalse(available);
    verify(joinLeave, never()).remove(isA(MemberIdentifier.class), isA(String.class));
    assertTrue(gmsHealthMonitor.isSuspectMember(memberToCheck));

    final long checkRequestsSentAfter = services.getStatistics().getUdpFinalCheckRequestsSent();
    assertThat(checkRequestsSentAfter).isGreaterThan(checkRequestsSentBefore);
  }

  @Test
  public void testFailedCheckIfAvailableWithoutFailureDetectionPortRemovesMember()
      throws Exception {
    useGMSHealthMonitorTestClass = true;
    simulateHeartbeatInGMSHealthMonitorTestClass = false;

    final long checkRequestsSentBefore = services.getStatistics().getUdpFinalCheckRequestsSent();

    GMSMembershipView v = installAView();

    MemberIdentifier memberToCheck = gmsHealthMonitor.getNextNeighbor();
    boolean available = gmsHealthMonitor.checkIfAvailable(memberToCheck, "Not responding", true);

    assertFalse(available);
    verify(joinLeave).remove(isA(MemberIdentifier.class), isA(String.class));

    final long checkRequestsSentAfter = services.getStatistics().getUdpFinalCheckRequestsSent();
    assertThat(checkRequestsSentAfter).isGreaterThan(checkRequestsSentBefore);
  }

  @Test
  public void testShutdown() throws Exception {

    installAView();

    gmsHealthMonitor.stop();
    await().until(() -> gmsHealthMonitor.isShutdown());

  }

  @Test
  public void testCreateServerSocket() throws Exception {
    try (ServerSocket socket =
        gmsHealthMonitor.createServerSocket(InetAddress.getLocalHost(), portRange)) {
      Assert.assertTrue(
          portRange[0] <= socket.getLocalPort() && socket.getLocalPort() <= portRange[1]);
    }
  }

  @Test
  public void testCreateServerSocketPortRangeInvalid() throws Exception {
    try (ServerSocket socket =
        gmsHealthMonitor.createServerSocket(InetAddress.getLocalHost(), new int[] {-1, -1})) {
      Assert.fail("socket was created with invalid port range");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test
  public void testClientSocketHandler() throws Exception {
    int viewId = 2;
    long msb = 3;
    long lsb = 4;
    MemberIdentifier otherMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb, lsb);
    MemberIdentifier gmsMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb, lsb);
    executeTestClientSocketHandler(gmsMember, otherMember, GMSHealthMonitor.OK);
  }

  @Test
  public void testClientSocketHandlerWhenMsbDoNotMatch() throws Exception {
    int viewId = 2;
    long msb = 3;
    long lsb = 4;
    MemberIdentifier otherMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb + 1, lsb);
    MemberIdentifier gmsMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb, lsb);
    executeTestClientSocketHandler(gmsMember, otherMember, GMSHealthMonitor.ERROR);
  }

  @Test
  public void testClientSocketHandlerWhenLsbDoNotMatch() throws Exception {
    int viewId = 2;
    long msb = 3;
    long lsb = 4;
    MemberIdentifier otherMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb, lsb + 1);
    MemberIdentifier gmsMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb, lsb);
    executeTestClientSocketHandler(gmsMember, otherMember, GMSHealthMonitor.ERROR);
  }

  @Test
  public void testClientSocketHandlerWhenViewIdDoNotMatch() throws Exception {
    int viewId = 2;
    long msb = 3;
    long lsb = 4;
    MemberIdentifier otherMember = createGMSMember(Version.CURRENT_ORDINAL, viewId + 1, msb, lsb);
    MemberIdentifier gmsMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb, lsb);
    executeTestClientSocketHandler(gmsMember, otherMember, GMSHealthMonitor.ERROR);
  }

  public void executeTestClientSocketHandler(MemberIdentifier gmsMember,
      MemberIdentifier otherMember,
      int expectedResult) throws Exception {
    // We have already set the view id in the member but when creating the IDM it resets it to -1
    // for some reason
    int viewId = gmsMember.getVmViewId();

    MemberIdentifier testMember =
        createGMSMember(Version.CURRENT_ORDINAL, viewId,
            gmsMember.getUuidMostSignificantBits(),
            gmsMember.getUuidLeastSignificantBits());
    testMember.setUdpPort(9000);

    // We set to our expected test viewId in the IDM as well as resetting the gms member
    gmsMember.setVmViewId(viewId);


    // Set up the incoming/received bytes. We just wrap output streams and write out the gms member
    // information
    byte[] receivedBytes = writeMemberToBytes(otherMember);
    InputStream mockInputStream = new ByteArrayInputStream(receivedBytes);

    // configure the mock to return the mocked incoming bytes and provide an outputstream that we
    // will check
    Socket fakeSocket = mock(Socket.class);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    when(fakeSocket.getInputStream()).thenReturn(mockInputStream);
    when(fakeSocket.getOutputStream()).thenReturn(outputStream);

    // run the socket handler
    gmsHealthMonitor.setLocalAddress(testMember);
    ClientSocketHandler handler = gmsHealthMonitor.new ClientSocketHandler(fakeSocket);
    handler.run();

    // verify the written bytes are as expected
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
    int byteReply = dis.read();
    assertEquals(expectedResult, byteReply);

    Assert.assertTrue(gmsHealthMonitor.getStats().getFinalCheckResponsesSent() > 0);
    Assert.assertTrue(gmsHealthMonitor.getStats().getTcpFinalCheckResponsesSent() > 0);
  }

  @Test
  public void testBeSickAndPlayDead() {
    GMSMembershipView v = new GMSMembershipView(mockMembers.get(0), 2, mockMembers);
    gmsHealthMonitor.installView(v);
    gmsHealthMonitor.beSick();

    // a sick member will not respond to a heartbeat request
    HeartbeatRequestMessage req = new HeartbeatRequestMessage(mockMembers.get(0), 10);
    req.setSender(mockMembers.get(0));
    gmsHealthMonitor.processMessage(req);
    verify(messenger, never()).send(isA(HeartbeatMessage.class));

    // a sick member will not record a heartbeat from another member
    HeartbeatMessage hb = new HeartbeatMessage(-1);
    hb.setSender(mockMembers.get(0));
    gmsHealthMonitor.processMessage(hb);
    assertTrue(gmsHealthMonitor.memberTimeStamps.get(hb.getSender()) == null);

    // a sick member will not take action on a Suspect message from another member
    SuspectMembersMessage smm = mock(SuspectMembersMessage.class);
    Error err = new AssertionError("expected suspect message to be ignored");
    when(smm.getMembers()).thenThrow(err);
    when(smm.getSender()).thenThrow(err);
    when(smm.getDSFID()).thenCallRealMethod();
    gmsHealthMonitor.processMessage(smm);
  }

  @Test
  public void testTcpCheckMemberTriesUntilTimeout() throws Exception {
    ServerSocket mySocket = new ServerSocket(0);
    Thread serverThread = new Thread() {
      public void run() {
        long giveupTime = System.currentTimeMillis() + (5 * memberTimeout);
        while (System.currentTimeMillis() < giveupTime) {
          try {
            Socket acceptedSocket = mySocket.accept();
            try {
              Thread.sleep(200);
            } catch (InterruptedException e) {
              e.printStackTrace();
              return;
            }
            acceptedSocket.close();
          } catch (IOException e) {
            if (!mySocket.isClosed()) {
              System.err.println("Test failed with unexpected IOException");
              e.printStackTrace(System.err);
            }
            return;
          }
        }
      }
    };
    serverThread.setDaemon(true);
    serverThread.start();
    MemberIdentifier otherMember =
        createGMSMember(Version.CURRENT_ORDINAL, 0, 1, 1);
    long startTime = System.currentTimeMillis();
    gmsHealthMonitor.doTCPCheckMember(otherMember, mySocket.getLocalPort(), true);
    mySocket.close();
    serverThread.interrupt();
    serverThread.join(getTimeout().toMillis());
    assertThat(System.currentTimeMillis()).isGreaterThanOrEqualTo(startTime + memberTimeout);
  }

  @Test
  public void testDoTCPCheckMemberWithOkStatus() throws Exception {
    executeTestDoTCPCheck(GMSHealthMonitor.OK, true);
  }

  @Test
  public void testDoTCPCheckMemberWithErrorStatus() throws Exception {
    executeTestDoTCPCheck(GMSHealthMonitor.ERROR, false);
  }

  @Test
  public void testDoTCPCheckMemberWithUnkownStatus() throws Exception {
    executeTestDoTCPCheck(GMSHealthMonitor.ERROR + 100, false);
  }

  private void executeTestDoTCPCheck(int receivedStatus, boolean expectedResult) throws Exception {
    MemberIdentifier otherMember =
        createGMSMember(Version.CURRENT_ORDINAL, 0, 1, 1);
    MemberIdentifier gmsMember =
        createGMSMember(Version.CURRENT_ORDINAL, 0, 1, 1);

    // Set up the incoming/received bytes. We just wrap output streams and write out the gms member
    // information
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(receivedStatus);

    byte[] receivedBytes = baos.toByteArray();
    InputStream mockInputStream = new ByteArrayInputStream(receivedBytes);

    Socket fakeSocket = mock(Socket.class);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    when(fakeSocket.getInputStream()).thenReturn(mockInputStream);
    when(fakeSocket.getOutputStream()).thenReturn(outputStream);
    when(fakeSocket.isConnected()).thenReturn(true);

    assertEquals(expectedResult, gmsHealthMonitor.doTCPCheckMember(otherMember, fakeSocket));
    Assert.assertTrue(gmsHealthMonitor.getStats().getFinalCheckRequestsSent() > 0);
    Assert.assertTrue(gmsHealthMonitor.getStats().getTcpFinalCheckRequestsSent() > 0);
    Assert.assertTrue(gmsHealthMonitor.getStats().getFinalCheckResponsesReceived() > 0);
    Assert.assertTrue(gmsHealthMonitor.getStats().getTcpFinalCheckResponsesReceived() > 0);

    // we can check to see if the gms member information was written out by the tcp check
    byte[] bytesWritten = outputStream.toByteArray();
    Assert.assertArrayEquals(writeMemberToBytes(gmsMember),
        bytesWritten);
  }

  private MemberIdentifier createGMSMember(short version, int viewId, long msb, long lsb)
      throws UnknownHostException {
    MemberData memberData = MemberDataBuilder.newBuilderForLocalHost("localhost")
        .setVersionOrdinal(version)
        .setVmViewId(viewId)
        .setUuidMostSignificantBits(msb)
        .setUuidLeastSignificantBits(lsb)
        .build();
    MemberIdentifier gmsMember = services.getMemberFactory().create(memberData);
    return gmsMember;
  }

  private byte[] writeMemberToBytes(MemberIdentifier gmsMember) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dataReceive = new DataOutputStream(baos);
    gmsHealthMonitor.writeMemberToStream(gmsMember, dataReceive);
    return baos.toByteArray();
  }

  public class GMSHealthMonitorTest extends GMSHealthMonitor {
    public boolean useBlockingSocket = false;
    public Set<MemberIdentifier> availabilityCheckedMembers = new HashSet<>();

    public GMSHealthMonitorTest() {
      super(new TcpSocketCreatorImpl());
    }

    @Override
    boolean doTCPCheckMember(MemberIdentifier suspectMember, int port,
        boolean retryIfConnectFails) {
      availabilityCheckedMembers.add(suspectMember);
      if (useGMSHealthMonitorTestClass) {
        if (simulateHeartbeatInGMSHealthMonitorTestClass) {
          HeartbeatMessage fakeHeartbeat = new HeartbeatMessage();
          fakeHeartbeat.setSender(suspectMember);
          gmsHealthMonitor.processMessage(fakeHeartbeat);
        }
        if (allowSelfCheckToSucceed && suspectMember.equals(joinLeave.getMemberID())) {
          return true;
        }
        return false;
      }
      return super.doTCPCheckMember(suspectMember, port, retryIfConnectFails);
    }

    @Override
    ServerSocket createServerSocket(InetAddress socketAddress, int[] portRange) throws IOException {
      final ServerSocket serverSocket = super.createServerSocket(socketAddress, portRange);
      if (useBlockingSocket) {
        try {
          return new TrickySocket(serverSocket);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        return serverSocket;
      }
    }
  }

  public class TrickySocket extends ServerSocket {
    ServerSocket wrappedSocket;
    final Lock lock = new ReentrantLock();
    boolean firstWait = true;
    final Condition block = lock.newCondition();

    public TrickySocket(ServerSocket wrappee) throws IOException {
      wrappedSocket = wrappee;
    }

    @Override
    public void bind(SocketAddress endpoint) throws IOException {
      wrappedSocket.bind(endpoint);
    }

    @Override
    public void bind(SocketAddress endpoint, int backlog) throws IOException {
      wrappedSocket.bind(endpoint, backlog);
    }

    @Override
    public InetAddress getInetAddress() {
      return wrappedSocket.getInetAddress();
    }

    @Override
    public int getLocalPort() {
      return wrappedSocket.getLocalPort();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
      return wrappedSocket.getLocalSocketAddress();
    }

    @Override
    public Socket accept() throws IOException {
      // It's expected that the GMSHealthMonitor will start shut down prior to calling this accept,
      // but to prevent hanging on the rare race where that doesn't happen, throw this exception.
      throw new IOException("Unable to handle accept call.");
    }

    @Override
    public void close() throws IOException {
      wrappedSocket.close();
      lock.lock();
      block.signal();
      lock.unlock();
    }

    @Override
    public boolean isClosed() {
      final boolean closed = wrappedSocket.isClosed();
      lock.lock();
      if (firstWait) {
        firstWait = false;
        try {
          block.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      lock.unlock();
      return closed;
    }
  }
}
