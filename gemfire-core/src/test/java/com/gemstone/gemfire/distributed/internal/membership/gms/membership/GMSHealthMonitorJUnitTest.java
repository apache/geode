package com.gemstone.gemfire.distributed.internal.membership.gms.membership;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.ServiceConfig;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.fd.GMSHealthMonitor;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Messenger;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.PingRequestMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.PingResponseMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.RemoveMemberMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.SuspectMembersMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.SuspectRequest;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GMSHealthMonitorJUnitTest {

  private Services services;
  private ServiceConfig mockConfig;
  private DistributionConfig mockDistConfig;
  private List<InternalDistributedMember> mockMembers;
  private Messenger messenger;
  private GMSJoinLeave joinLeave;
  private GMSHealthMonitor gmsHealthMonitor;
  final long memberTimeout = 1000l;

  @Before
  public void initMocks() throws UnknownHostException {
    mockDistConfig = mock(DistributionConfig.class);
    mockConfig = mock(ServiceConfig.class);
    messenger = mock(Messenger.class);
    joinLeave = mock(GMSJoinLeave.class);
    services = mock(Services.class);

    when(mockConfig.getDistributionConfig()).thenReturn(mockDistConfig);
    when(mockConfig.getMemberTimeout()).thenReturn(memberTimeout);
    when(services.getConfig()).thenReturn(mockConfig);
    when(services.getMessenger()).thenReturn(messenger);
    when(services.getJoinLeave()).thenReturn(joinLeave);   

    mockMembers = new ArrayList<InternalDistributedMember>();
    for (int i = 0; i < 7; i++) {
      InternalDistributedMember mbr = new InternalDistributedMember("localhost", 8888 + i);

      if (i == 0 || i == 1) {
        mbr.setVmKind(DistributionManager.LOCATOR_DM_TYPE);
        mbr.getNetMember().setPreferredForCoordinator(true);
      }
      mockMembers.add(mbr);
    }

    gmsHealthMonitor = new GMSHealthMonitor();
    gmsHealthMonitor.init(services);
    gmsHealthMonitor.start();
  }

  @After
  public void tearDown() {
    gmsHealthMonitor.stop();
  }

  @Test
  public void testHMServiceStarted() throws IOException {

    MethodExecuted messageSent = new MethodExecuted();
    InternalDistributedMember mbr = new InternalDistributedMember(SocketCreator.getLocalHost(), 12345);
    when(messenger.getMemberID()).thenReturn(mbr);
    when(messenger.send(any(PingResponseMessage.class))).thenAnswer(messageSent);

    gmsHealthMonitor.processMessage(new PingRequestMessage(mbr, 1));
    Assert.assertTrue("Ping Response should have been sent", messageSent.isMethodExecuted());
  }

  /**
   * checks whether we get local member id or not to set next neighbour
   */
  @Test
  public void testHMNextNeighbour() throws IOException {

    NetView v = new NetView(mockMembers.get(0), 2, mockMembers, new HashSet<InternalDistributedMember>(), new HashSet<InternalDistributedMember>());

    MethodExecuted messageSent = new MethodExecuted();
    when(services.getJoinLeave().getMemberID()).thenAnswer(messageSent);

    gmsHealthMonitor.installView(v);

    Assert.assertTrue("It should have got memberID from services.getJoinLeave().getMemberID()", messageSent.isMethodExecuted());
  }

  /**
   * checks who is next neighbour
   */
  @Test
  public void testHMNextNeighbourVerify() throws IOException {

    NetView v = new NetView(mockMembers.get(0), 2, mockMembers, new HashSet<InternalDistributedMember>(), new HashSet<InternalDistributedMember>());

    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(3));

    gmsHealthMonitor.installView(v);

    Assert.assertEquals(mockMembers.get(4), gmsHealthMonitor.getNextNeighbour());

  }

  /**
   * it checks neighbour after membertimeout, it should be different
   */

  @Test
  public void testHMNextNeighbourAfterTimeout() throws IOException {

    NetView v = new NetView(mockMembers.get(0), 2, mockMembers, new HashSet<InternalDistributedMember>(), new HashSet<InternalDistributedMember>());

    // 3rd is current member
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(3));

    gmsHealthMonitor.installView(v);

    try {
      // member-timeout is 1000 ms
      Thread.sleep(memberTimeout + 5);
    } catch (InterruptedException e) {
    }
    // neighbour should change to 5th
    Assert.assertEquals(mockMembers.get(5), gmsHealthMonitor.getNextNeighbour());
  }

  /**
   * it checks neighbour before membertiemout, it should be same
   */

  @Test
  public void testHMNextNeighbourBeforeTimeout() throws IOException {

    NetView v = new NetView(mockMembers.get(0), 2, mockMembers, new HashSet<InternalDistributedMember>(), new HashSet<InternalDistributedMember>());

    // 3rd is current member
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(3));

    gmsHealthMonitor.installView(v);

    try {
      // member-timeout is 1000 ms, so next neighbour should be same
      Thread.sleep(memberTimeout - 200);
    } catch (InterruptedException e) {
    }
    // neighbour should be same
    Assert.assertEquals(mockMembers.get(4), gmsHealthMonitor.getNextNeighbour());
  }

  /***
   * checks whether member-check thread sends suspectMembers message
   */
  @Test
  public void testSuspectMembersCalledThroughMemberCheckThread() {
    NetView v = new NetView(mockMembers.get(0), 2, mockMembers, new HashSet<InternalDistributedMember>(), new HashSet<InternalDistributedMember>());

    MethodExecuted messageSent = new MethodExecuted();
    // 3rd is current member
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(3));

    gmsHealthMonitor.installView(v);

    when(messenger.send(any(SuspectMembersMessage.class))).thenAnswer(messageSent);

    try {
      // member-timeout is 1000 ms + ping timeout 100ms
      // plus wait 100 ms for ack
      Thread.sleep(memberTimeout + 100);
    } catch (InterruptedException e) {
    }

    Assert.assertTrue("SuspectMembersMessage should have sent", messageSent.isMethodExecuted());
  }

  /***
   * checks ping thread didn't sends suspectMembers message before timeout
   */
  @Test
  public void testSuspectMembersNotCalledThroughPingThreadBeforeTimeout() {

    NetView v = new NetView(mockMembers.get(0), 2, mockMembers, new HashSet<InternalDistributedMember>(), new HashSet<InternalDistributedMember>());

    MethodExecuted messageSent = new MethodExecuted();
    // 3rd is current member
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(3));

    gmsHealthMonitor.installView(v);

    when(messenger.send(any(SuspectMembersMessage.class))).thenAnswer(messageSent);

    try {
      // member-timeout is 1000 ms
      // plus 100 ms for ack
      Thread.sleep(memberTimeout - 200);
    } catch (InterruptedException e) {
    }

    Assert.assertTrue("SuspectMembersMessage shouldn't have sent", !messageSent.isMethodExecuted());
  }

  /***
   * Checks whether suspect thread sends suspectMembers message
   */
  @Test
  public void testSuspectMembersCalledThroughSuspectThread() throws Exception {
    NetView v = new NetView(mockMembers.get(0), 2, mockMembers, new HashSet<InternalDistributedMember>(), new HashSet<InternalDistributedMember>());
    
    MethodExecuted messageSent = new MethodExecuted();
    // 3rd is current member
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(3));
    
    gmsHealthMonitor.installView(v);

    gmsHealthMonitor.suspect(mockMembers.get(1), "Not responding");

    when(messenger.send(any(SuspectMembersMessage.class))).thenAnswer(messageSent);

    Thread.sleep(GMSHealthMonitor.MEMBER_SUSPECT_COLLECTION_INTERVAL + 1000);

    Assert.assertTrue("SuspectMembersMessage should have sent", messageSent.isMethodExecuted());
  }

  /***
   * Checks suspect thread doesn't sends suspectMembers message before timeout
   */
  @Test
  public void testSuspectMembersNotCalledThroughSuspectThreadBeforeTimeout() {

    NetView v = new NetView(mockMembers.get(0), 2, mockMembers, new HashSet<InternalDistributedMember>(), new HashSet<InternalDistributedMember>());

    MethodExecuted messageSent = new MethodExecuted();
    // 3rd is current member
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(3));

    gmsHealthMonitor.installView(v);

    gmsHealthMonitor.suspect(mockMembers.get(1), "Not responding");

    when(messenger.send(any(SuspectMembersMessage.class))).thenAnswer(messageSent);

    try {
      // suspect thread timeout is 200 ms
      Thread.sleep(100l);
    } catch (InterruptedException e) {
    }

    Assert.assertTrue("SuspectMembersMessage shouldn't have sent", !messageSent.isMethodExecuted());
  }

  /***
   * Send remove member message after doing final check, ping Timeout
   */
  @Test
  public void testRemoveMemberCalled() {

    NetView v = new NetView(mockMembers.get(0), 2, mockMembers, new HashSet<InternalDistributedMember>(), new HashSet<InternalDistributedMember>());

    MethodExecuted messageSent = new MethodExecuted();
    // 3rd is current member
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(0)); // coordinator and local member

    gmsHealthMonitor.installView(v);

    ArrayList<InternalDistributedMember> recipient = new ArrayList<InternalDistributedMember>();
    recipient.add(mockMembers.get(0));
    ArrayList<SuspectRequest> as = new ArrayList<SuspectRequest>();
    SuspectRequest sr = new SuspectRequest(mockMembers.get(1), "Not Responding");// removing member 1
    as.add(sr);
    SuspectMembersMessage sm = new SuspectMembersMessage(recipient, as);
    sm.setSender(mockMembers.get(0));

    when(messenger.send(any(RemoveMemberMessage.class))).thenAnswer(messageSent);

    gmsHealthMonitor.processMessage(sm);

    try {
      // this happens after final check, ping timeout
      Thread.sleep(150l);
    } catch (InterruptedException e) {
    }

    Assert.assertTrue("RemoveMemberMessage should have sent", messageSent.isMethodExecuted());
  }

  /***
   * Shouldn't send remove member message before doing final check, or before ping Timeout
   */
  @Test
  public void testRemoveMemberNotCalledBeforeTimeout() {

    NetView v = new NetView(mockMembers.get(0), 2, mockMembers, new HashSet<InternalDistributedMember>(), new HashSet<InternalDistributedMember>());

    MethodExecuted messageSent = new MethodExecuted();
    // 3rd is current member
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(0)); // coordinator and local member

    gmsHealthMonitor.installView(v);

    ArrayList<InternalDistributedMember> recipient = new ArrayList<InternalDistributedMember>();
    recipient.add(mockMembers.get(0));
    ArrayList<SuspectRequest> as = new ArrayList<SuspectRequest>();
    SuspectRequest sr = new SuspectRequest(mockMembers.get(1), "Not Responding");// removing member 1
    as.add(sr);
    SuspectMembersMessage sm = new SuspectMembersMessage(recipient, as);
    sm.setSender(mockMembers.get(0));

    when(messenger.send(any(RemoveMemberMessage.class))).thenAnswer(messageSent);

    gmsHealthMonitor.processMessage(sm);

    try {
      // this happens after final check, ping timeout
      Thread.sleep(90l);
    } catch (InterruptedException e) {
    }

    Assert.assertTrue("RemoveMemberMessage should have sent", messageSent.isMethodExecuted());
  }

  /***
   * Send remove member message after doing final check for coordinator, ping timeout
   * This test trying to remove coordinator
   */
  @Test
  public void testRemoveMemberCalledAfterDoingFinalCheckOnCoordinator() {

    NetView v = new NetView(mockMembers.get(0), 2, mockMembers, new HashSet<InternalDistributedMember>(), new HashSet<InternalDistributedMember>());

    MethodExecuted messageSent = new MethodExecuted();
    // preferred coordinators are 0 and 1
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(1));// next preferred coordinator

    gmsHealthMonitor.installView(v);

    ArrayList<InternalDistributedMember> recipient = new ArrayList<InternalDistributedMember>();
    recipient.add(mockMembers.get(0));
    recipient.add(mockMembers.get(1));
    ArrayList<SuspectRequest> as = new ArrayList<SuspectRequest>();
    SuspectRequest sr = new SuspectRequest(mockMembers.get(0), "Not Responding");// removing coordinator
    as.add(sr);
    SuspectMembersMessage sm = new SuspectMembersMessage(recipient, as);
    sm.setSender(mockMembers.get(4));// member 4 sends suspect message

    when(messenger.send(any(RemoveMemberMessage.class))).thenAnswer(messageSent);// member 1 will process

    gmsHealthMonitor.processMessage(sm);

    try {
      // this happens after final check, ping timeout = 100 ms
      Thread.sleep(110l);
    } catch (InterruptedException e) {
    }

    Assert.assertTrue("RemoveMemberMessage should have sent.", messageSent.isMethodExecuted());
  }

  /***
   * validates HealthMonitor.CheckIfAvailable api
   */
  @Test
  public void testCheckIfAvailable() {

    NetView v = new NetView(mockMembers.get(0), 2, mockMembers, new HashSet<InternalDistributedMember>(), new HashSet<InternalDistributedMember>());

    // 3rd is current member
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(3));

    gmsHealthMonitor.installView(v);

    long startTime = System.currentTimeMillis();

    boolean retVal = gmsHealthMonitor.checkIfAvailable(mockMembers.get(1), "Not responding", false);

    long timeTaken = System.currentTimeMillis() - startTime;

    Assert.assertTrue("This should have taken member ping timeout 100ms ", timeTaken > 90);
    Assert.assertTrue("CheckIfAvailable should have return false", !retVal);
  }

  @Test
  public void testShutdown() {

    NetView v = new NetView(mockMembers.get(0), 2, mockMembers, new HashSet<InternalDistributedMember>(), new HashSet<InternalDistributedMember>());

    MethodExecuted messageSent = new MethodExecuted();
    // 3rd is current member
    when(joinLeave.getMemberID()).thenReturn(mockMembers.get(3));

    gmsHealthMonitor.installView(v);

    gmsHealthMonitor.stop();

    try {
      // this happens after final check, membertimeout = 1000
      Thread.sleep(100l);
    } catch (InterruptedException e) {
    }

    Assert.assertTrue("HeathMonitor should have shutdown", gmsHealthMonitor.isShutdown());

  }

  private class MethodExecuted implements Answer {
    private boolean methodExecuted = false;

    public boolean isMethodExecuted() {
      return methodExecuted;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      methodExecuted = true;
      return null;
    }
  }
}
