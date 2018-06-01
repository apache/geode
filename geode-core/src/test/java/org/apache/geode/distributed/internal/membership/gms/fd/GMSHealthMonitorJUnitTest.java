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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.awaitility.Awaitility;
import org.jgroups.util.UUID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.ServiceConfig;
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
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({IntegrationTest.class, MembershipTest.class})
public class GMSHealthMonitorJUnitTest {

  private Services services;
  private ServiceConfig mockConfig;
  private DistributionConfig mockDistConfig;
  private List<InternalDistributedMember> mockMembers;
  private Messenger messenger;
  private JoinLeave joinLeave;
  private GMSHealthMonitor gmsHealthMonitor;
  private Manager manager;
  private long statsId = 123;
  final long memberTimeout = 1000l;
  private int[] portRange = new int[] {0, 65535};
  private boolean useGMSHealthMonitorTestClass = false;
  private final int myAddressIndex = 3;

  @Before
  public void initMocks() throws UnknownHostException {
    // System.setProperty("gemfire.bind-address", "localhost");
    mockDistConfig = mock(DistributionConfig.class);
    mockConfig = mock(ServiceConfig.class);
    messenger = mock(Messenger.class);
    joinLeave = mock(JoinLeave.class);
    manager = mock(Manager.class);
    services = mock(Services.class);
    Stopper stopper = mock(Stopper.class);

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
    DistributionManager dm = mock(DistributionManager.class);
    SocketCreatorFactory.setDistributionConfig(new DistributionConfigImpl(new Properties()));
    InternalDistributedSystem system =
        InternalDistributedSystem.newInstanceForTesting(dm, nonDefault);

    when(mockConfig.getDistributionConfig()).thenReturn(mockDistConfig);
    when(mockConfig.getMemberTimeout()).thenReturn(memberTimeout);
    when(mockConfig.getMembershipPortRange()).thenReturn(portRange);
    when(services.getConfig()).thenReturn(mockConfig);
    when(services.getMessenger()).thenReturn(messenger);
    when(services.getJoinLeave()).thenReturn(joinLeave);
    when(services.getCancelCriterion()).thenReturn(stopper);
    when(services.getManager()).thenReturn(manager);
    when(services.getStatistics()).thenReturn(new DistributionStats(system, statsId));
    when(stopper.isCancelInProgress()).thenReturn(false);

    if (mockMembers == null) {
      mockMembers = new ArrayList<InternalDistributedMember>();
      for (int i = 0; i < 7; i++) {
        InternalDistributedMember mbr = new InternalDistributedMember("localhost", 8888 + i);

        if (i == 0 || i == 1) {
          mbr.setVmKind(ClusterDistributionManager.LOCATOR_DM_TYPE);
          mbr.getNetMember().setPreferredForCoordinator(true);
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
    SocketCreatorFactory.close();
    // System.getProperties().remove("gemfire.bind-address");
  }

  @Test
  public void testHMServiceStarted() throws IOException {

    InternalDistributedMember mbr =
        new InternalDistributedMember(SocketCreator.getLocalHost(), 12345);
    mbr.setVmViewId(1);
    when(messenger.getMemberID()).thenReturn(mbr);
    gmsHealthMonitor.started();

    NetView v = new NetView(mbr, 1, mockMembers);

    gmsHealthMonitor.processMessage(new HeartbeatRequestMessage(mbr, 1));
    verify(messenger, atLeastOnce()).send(any(HeartbeatMessage.class));
    assertEquals(1, gmsHealthMonitor.getStats().getHeartbeatRequestsReceived());
    assertEquals(1, gmsHealthMonitor.getStats().getHeartbeatsSent());
  }

  @Test
  public void testHMServiceHandlesShutdownRace() throws IOException {
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
  public void testHMNextNeighborVerify() throws IOException {
    installAView();
    assertEquals(mockMembers.get(myAddressIndex + 1), gmsHealthMonitor.getNextNeighbor());
  }

  @Test
  public void testHMNextNeighborAfterTimeout() throws Exception {
    System.out.println("testHMNextNeighborAfterTimeout starting");

    installAView();
    InternalDistributedMember initialNeighbor = mockMembers.get(myAddressIndex + 1);

    // allow the monitor to give up on the initial "next neighbor" and
    // move on to the one after it
    long giveup = System.currentTimeMillis() + (2 * memberTimeout) + 1500;
    InternalDistributedMember neighbor = gmsHealthMonitor.getNextNeighbor();
    while (System.currentTimeMillis() < giveup && neighbor == initialNeighbor) {
      Thread.sleep(50);
      neighbor = gmsHealthMonitor.getNextNeighbor();
    }

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
  public void testHMNextNeighborBeforeTimeout() throws IOException {
    installAView();

    // Should we remove these sleeps and force the checkmember directly instead of waiting?
    try {
      // member-timeout is 1000 ms. We initiate a check and choose
      // a new neighbor at 500 ms
      Thread.sleep(memberTimeout / GMSHealthMonitor.LOGICAL_INTERVAL - 100);
    } catch (InterruptedException e) {
    }
    // neighbor should be same
    System.out.println("next neighbor is " + gmsHealthMonitor.getNextNeighbor() + "\nmy address is "
        + mockMembers.get(myAddressIndex) + "\nview is " + joinLeave.getView());

    assertEquals(mockMembers.get(myAddressIndex + 1), gmsHealthMonitor.getNextNeighbor());
  }

  /***
   * checks whether member-check thread sends suspectMembers message
   */
  @Test
  public void testSuspectMembersCalledThroughMemberCheckThread() throws Exception {
    installAView();

    // when the view is installed we start a heartbeat timeout. After
    // that expires we request a heartbeat
    Thread.sleep(3 * memberTimeout + 100);

    System.out.println("testSuspectMembersCalledThroughMemberCheckThread ending");
    assertTrue(gmsHealthMonitor.isSuspectMember(mockMembers.get(myAddressIndex + 1)));
    Assert.assertTrue(gmsHealthMonitor.getStats().getHeartbeatRequestsSent() > 0);
    Assert.assertTrue(gmsHealthMonitor.getStats().getSuspectsSent() > 0);
  }

  private NetView installAView() {
    System.out.println("installAView starting");
    NetView v = new NetView(mockMembers.get(0), 2, mockMembers);

    // 3rd is current member
    when(messenger.getMemberID()).thenReturn(mockMembers.get(myAddressIndex));
    gmsHealthMonitor.started();

    gmsHealthMonitor.installView(v);

    return v;
  }

  private void setFailureDetectionPorts(NetView v) {
    java.util.Iterator<InternalDistributedMember> itr = mockMembers.iterator();

    int port = 7899;
    while (itr.hasNext()) {
      v.setFailureDetectionPort(itr.next(), port++);
    }
  }

  /***
   * checks ping thread didn't sends suspectMembers message before timeout
   */
  @Test
  public void testSuspectMembersNotCalledThroughPingThreadBeforeTimeout() {
    installAView();
    InternalDistributedMember neighbor = gmsHealthMonitor.getNextNeighbor();

    try {
      // member-timeout is 1000 ms
      // plus 100 ms for ack
      Thread.sleep(memberTimeout - 200);
    } catch (InterruptedException e) {
    }

    assertFalse(gmsHealthMonitor.isSuspectMember(neighbor));
  }

  /***
   * Checks whether suspect thread sends suspectMembers message
   */
  @Test
  public void testSuspectMembersCalledThroughSuspectThread() throws Exception {
    installAView();

    gmsHealthMonitor.suspect(mockMembers.get(1), "Not responding");

    Thread.sleep(GMSHealthMonitor.MEMBER_SUSPECT_COLLECTION_INTERVAL + 1000);

    verify(messenger, atLeastOnce()).send(any(SuspectMembersMessage.class));

    Assert.assertTrue(gmsHealthMonitor.getStats().getSuspectsSent() > 0);
  }

  /***
   * Checks suspect thread doesn't sends suspectMembers message before timeout
   */
  @Test
  public void testSuspectMembersNotCalledThroughSuspectThreadBeforeTimeout() {

    installAView();

    gmsHealthMonitor.suspect(mockMembers.get(1), "Not responding");

    try {
      // suspect thread timeout is 200 ms
      Thread.sleep(100l);
    } catch (InterruptedException e) {
    }

    verify(messenger, atLeastOnce()).send(isA(SuspectMembersMessage.class));

    Assert.assertTrue(gmsHealthMonitor.getStats().getSuspectsSent() > 0);
  }

  /***
   * Send remove member message after doing final check, ping Timeout
   */
  @Test
  public void testRemoveMemberCalled() throws Exception {
    System.out.println("testRemoveMemberCalled starting");
    NetView v = new NetView(mockMembers.get(0), 2, mockMembers);

    // 3rd is current member
    when(messenger.getMemberID()).thenReturn(mockMembers.get(0)); // coordinator and local member
    gmsHealthMonitor.started();

    gmsHealthMonitor.installView(v);

    Thread.sleep(memberTimeout / GMSHealthMonitor.LOGICAL_INTERVAL);

    ArrayList<InternalDistributedMember> recipient = new ArrayList<InternalDistributedMember>();
    recipient.add(mockMembers.get(0));
    ArrayList<SuspectRequest> as = new ArrayList<SuspectRequest>();
    SuspectRequest sr = new SuspectRequest(mockMembers.get(1), "Not Responding");// removing member
                                                                                 // 1
    as.add(sr);
    SuspectMembersMessage sm = new SuspectMembersMessage(recipient, as);
    sm.setSender(mockMembers.get(0));

    gmsHealthMonitor.processMessage(sm);

    Awaitility.await("waiting for remove(member) to be invoked")
        .atMost(3 * memberTimeout, TimeUnit.SECONDS).until(() -> {
          verify(joinLeave, atLeastOnce()).remove(any(InternalDistributedMember.class),
              any(String.class));
        });
    Assert.assertTrue(gmsHealthMonitor.getStats().getSuspectsReceived() > 0);
  }

  /***
   * Shouldn't send remove member message before doing final check, or before ping Timeout
   */
  @Test
  public void testRemoveMemberNotCalledBeforeTimeout() {
    System.out.println("testRemoveMemberNotCalledBeforeTimeout starting");
    NetView v = new NetView(mockMembers.get(0), 2, mockMembers);

    // 3rd is current member
    when(messenger.getMemberID()).thenReturn(mockMembers.get(0)); // coordinator and local member
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(0)); // coordinator and local member
    gmsHealthMonitor.started();

    gmsHealthMonitor.installView(v);

    ArrayList<InternalDistributedMember> recipient = new ArrayList<InternalDistributedMember>();
    recipient.add(mockMembers.get(0));
    ArrayList<SuspectRequest> as = new ArrayList<SuspectRequest>();
    SuspectRequest sr = new SuspectRequest(mockMembers.get(1), "Not Responding");// removing member
                                                                                 // 1
    as.add(sr);
    SuspectMembersMessage sm = new SuspectMembersMessage(recipient, as);
    sm.setSender(mockMembers.get(0));

    gmsHealthMonitor.processMessage(sm);

    try {
      // this happens after final check, ping timeout
      Thread.sleep(memberTimeout - 100);
    } catch (InterruptedException e) {
    }

    System.out.println("testRemoveMemberNotCalledBeforeTimeout ending");
    verify(joinLeave, never()).remove(any(InternalDistributedMember.class), any(String.class));
    Assert.assertTrue(gmsHealthMonitor.getStats().getSuspectsReceived() > 0);
  }

  /***
   * Send remove member message after doing final check for coordinator, ping timeout This test
   * trying to remove coordinator
   */
  @Test
  public void testRemoveMemberCalledAfterDoingFinalCheckOnCoordinator() throws Exception {

    NetView v = new NetView(mockMembers.get(0), 2, mockMembers);

    // preferred coordinators are 0 and 1
    when(messenger.getMemberID()).thenReturn(mockMembers.get(1));// next preferred coordinator
    gmsHealthMonitor.started();

    gmsHealthMonitor.installView(v);

    Thread.sleep(memberTimeout / GMSHealthMonitor.LOGICAL_INTERVAL);

    ArrayList<InternalDistributedMember> recipient = new ArrayList<InternalDistributedMember>();
    recipient.add(mockMembers.get(0));
    recipient.add(mockMembers.get(1));
    ArrayList<SuspectRequest> as = new ArrayList<SuspectRequest>();
    SuspectRequest sr = new SuspectRequest(mockMembers.get(0), "Not Responding");// removing
                                                                                 // coordinator
    as.add(sr);
    SuspectMembersMessage sm = new SuspectMembersMessage(recipient, as);
    sm.setSender(mockMembers.get(myAddressIndex + 1));// member 4 sends suspect message

    gmsHealthMonitor.processMessage(sm);

    // this happens after final check, ping timeout = 1000 ms
    Thread.sleep(memberTimeout + 200);

    verify(joinLeave, atLeastOnce()).remove(any(InternalDistributedMember.class),
        any(String.class));
    Assert.assertTrue(gmsHealthMonitor.getStats().getSuspectsReceived() > 0);
  }

  /***
   * validates HealthMonitor.CheckIfAvailable api
   */
  @Test
  public void testCheckIfAvailableNoHeartBeatDontRemoveMember() {
    installAView();
    long startTime = System.currentTimeMillis();
    boolean retVal = gmsHealthMonitor.checkIfAvailable(mockMembers.get(1), "Not responding", true);
    long timeTaken = System.currentTimeMillis() - startTime;

    assertFalse("CheckIfAvailable should have return false", retVal);
    assertTrue("This should have taken member ping timeout 100ms but it took " + timeTaken,
        timeTaken >= gmsHealthMonitor.memberTimeout);
  }

  @Test
  public void testCheckIfAvailableWithSimulatedHeartBeat() {
    NetView v = installAView();

    InternalDistributedMember memberToCheck = mockMembers.get(1);
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
  public void testCheckIfAvailableWithSimulatedHeartBeatWithTcpCheck() {
    System.out.println("testCheckIfAvailableWithSimulatedHeartBeatWithTcpCheck");
    useGMSHealthMonitorTestClass = true;

    try {
      NetView v = installAView();

      setFailureDetectionPorts(v);

      InternalDistributedMember memberToCheck = mockMembers.get(1);

      boolean retVal = gmsHealthMonitor.checkIfAvailable(memberToCheck, "Not responding", true);
      assertTrue("CheckIfAvailable should have return true", retVal);
    } finally {
      useGMSHealthMonitorTestClass = false;
    }
  }


  @Test
  public void testNeighborRemainsSameAfterSuccessfulFinalCheck() {
    useGMSHealthMonitorTestClass = true;

    try {
      NetView v = installAView();

      setFailureDetectionPorts(v);

      InternalDistributedMember memberToCheck = gmsHealthMonitor.getNextNeighbor();

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
  public void testExonerationMessageIsSentAfterSuccessfulFinalCheck() {
    useGMSHealthMonitorTestClass = true;

    try {
      NetView v = installAView();

      setFailureDetectionPorts(v);

      InternalDistributedMember memberToCheck = gmsHealthMonitor.getNextNeighbor();

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
  public void testFinalCheckPassedMessageCanBeSerializedAndDeserialized()
      throws IOException, ClassNotFoundException {
    HeapDataOutputStream heapDataOutputStream = new HeapDataOutputStream(500, Version.CURRENT);
    FinalCheckPassedMessage message =
        new FinalCheckPassedMessage(mockMembers.get(0), mockMembers.get(1));
    DataSerializer.writeObject(message, heapDataOutputStream);
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(heapDataOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    message = DataSerializer.readObject(dataInputStream);
    assertEquals(mockMembers.get(1), message.getSuspect());
  }



  @Test
  public void testInitiatorRewatchesSuspectAfterSuccessfulFinalCheck() {
    NetView v = installAView();

    setFailureDetectionPorts(v);

    InternalDistributedMember memberToCheck = gmsHealthMonitor.getNextNeighbor();
    gmsHealthMonitor.memberSuspected(mockMembers.get(0), memberToCheck, "Not responding");
    assertTrue(gmsHealthMonitor.isSuspectMember(memberToCheck));
    gmsHealthMonitor.processMessage(new FinalCheckPassedMessage(mockMembers.get(0), memberToCheck));
    assertFalse(gmsHealthMonitor.isSuspectMember(memberToCheck));
  }


  @Test
  public void testShutdown() {

    installAView();

    gmsHealthMonitor.stop();

    try {
      // this happens after final check, membertimeout = 1000
      Thread.sleep(100l);
    } catch (InterruptedException e) {
    }

    assertTrue("HeathMonitor should have shutdown", gmsHealthMonitor.isShutdown());

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
    GMSMember otherMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb, lsb);
    GMSMember gmsMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb, lsb);
    executeTestClientSocketHandler(gmsMember, otherMember, GMSHealthMonitor.OK);
  }

  @Test
  public void testClientSocketHandlerWhenMsbDoNotMatch() throws Exception {
    int viewId = 2;
    long msb = 3;
    long lsb = 4;
    GMSMember otherMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb + 1, lsb);
    GMSMember gmsMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb, lsb);
    executeTestClientSocketHandler(gmsMember, otherMember, GMSHealthMonitor.ERROR);
  }

  @Test
  public void testClientSocketHandlerWhenLsbDoNotMatch() throws Exception {
    int viewId = 2;
    long msb = 3;
    long lsb = 4;
    GMSMember otherMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb, lsb + 1);
    GMSMember gmsMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb, lsb);
    executeTestClientSocketHandler(gmsMember, otherMember, GMSHealthMonitor.ERROR);
  }

  @Test
  public void testClientSocketHandlerWhenViewIdDoNotMatch() throws Exception {
    int viewId = 2;
    long msb = 3;
    long lsb = 4;
    GMSMember otherMember = createGMSMember(Version.CURRENT_ORDINAL, viewId + 1, msb, lsb);
    GMSMember gmsMember = createGMSMember(Version.CURRENT_ORDINAL, viewId, msb, lsb);
    executeTestClientSocketHandler(gmsMember, otherMember, GMSHealthMonitor.ERROR);
  }

  public void executeTestClientSocketHandler(GMSMember gmsMember, GMSMember otherMember,
      int expectedResult) throws Exception {
    // We have already set the view id in the member but when creating the IDM it resets it to -1
    // for some reason
    int viewId = gmsMember.getVmViewId();

    InternalDistributedMember testMember =
        new InternalDistributedMember("localhost", 9000, Version.CURRENT, gmsMember);
    // We set to our expected test viewId in the IDM as well as reseting the gms member
    testMember.setVmViewId(viewId);
    gmsMember.setBirthViewId(viewId);


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
  public void testBeSickAndPlayDead() throws Exception {
    NetView v = new NetView(mockMembers.get(0), 2, mockMembers);
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
    InternalDistributedMember otherMember =
        createInternalDistributedMember(Version.CURRENT_ORDINAL, 0, 1, 1);
    InternalDistributedMember gmsMember =
        createInternalDistributedMember(Version.CURRENT_ORDINAL, 0, 1, 1);

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
    Assert.assertArrayEquals(writeMemberToBytes((GMSMember) gmsMember.getNetMember()),
        bytesWritten);
  }

  private InternalDistributedMember createInternalDistributedMember(short version, int viewId,
      long msb, long lsb) throws UnknownHostException {
    GMSMember gmsMember = createGMSMember(version, viewId, msb, lsb);
    InternalDistributedMember idm =
        new InternalDistributedMember("localhost", 9000, Version.CURRENT, gmsMember);
    // We set to our expected test viewId in the IDM as well as reseting the gms member
    idm.setVmViewId(viewId);
    gmsMember.setBirthViewId(viewId);
    return idm;
  }

  private GMSMember createGMSMember(short version, int viewId, long msb, long lsb)
      throws UnknownHostException {
    GMSMember gmsMember = new GMSMember();
    gmsMember.setVersionOrdinal(version);
    gmsMember.setBirthViewId(viewId);
    gmsMember.setUUID(new UUID(msb, lsb));
    gmsMember.setInetAddr(InetAddress.getLocalHost());
    return gmsMember;
  }

  private byte[] writeMemberToBytes(GMSMember gmsMember) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dataReceive = new DataOutputStream(baos);
    gmsHealthMonitor.writeMemberToStream(gmsMember, dataReceive);
    return baos.toByteArray();
  }

  public class GMSHealthMonitorTest extends GMSHealthMonitor {
    public boolean useBlockingSocket = false;

    @Override
    boolean doTCPCheckMember(InternalDistributedMember suspectMember, int port) {
      if (useGMSHealthMonitorTestClass) {
        HeartbeatMessage fakeHeartbeat = new HeartbeatMessage();
        fakeHeartbeat.setSender(suspectMember);
        gmsHealthMonitor.processMessage(fakeHeartbeat);
        return false;
      }
      return super.doTCPCheckMember(suspectMember, port);
    }

    @Override
    ServerSocket createServerSocket(InetAddress socketAddress, int[] portRange) {
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
      return wrappedSocket.accept();
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
