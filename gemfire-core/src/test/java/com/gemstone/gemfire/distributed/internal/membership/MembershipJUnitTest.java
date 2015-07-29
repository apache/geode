/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.LonerDistributionManager.DummyDMStats;
import com.gemstone.gemfire.distributed.internal.PoolStatHelper;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.distributed.internal.membership.gms.NetLocator;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpHandler;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpServer;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.tcp.Stub;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class MembershipJUnitTest extends TestCase {

  public MembershipJUnitTest(String name) {
    super(name);
  }

  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }
  
  /**
   * This test creates a locator with a colocated
   * membership manager and then creates a second
   * manager that joins the system of the first.
   * 
   * It then makes assertions about the state of
   * the membership view, closes one of the managers
   * and makes more assertions.
   */
  @Test
  public void testJoinLeave() throws Exception {
    
    MembershipManager m1=null, m2=null;
    Locator l = null;
    
    try {
      LogService.initialize();
      
      MemberAttributes.setDefaults(MemberAttributes.DEFAULT.getPort(),
          MemberAttributes.DEFAULT.getVmPid(),
          DistributionManager.NORMAL_DM_TYPE,
          MemberAttributes.DEFAULT.getVmViewId(),
          MemberAttributes.DEFAULT.getName(),
          MemberAttributes.DEFAULT.getGroups(), MemberAttributes.DEFAULT.getDurableClientAttributes());
      
      // boot up a locator
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      InetAddress localHost = SocketCreator.getLocalHost();
      l = Locator.startLocator(port, new File("testJoinLeave.dat"), localHost);

      // create configuration objects
      Properties nonDefault = new Properties();
      nonDefault.put(DistributionConfig.DISABLE_TCP_NAME, "true");
      nonDefault.put(DistributionConfig.MCAST_PORT_NAME, "0");
      nonDefault.put(DistributionConfig.LOG_FILE_NAME, "");
      nonDefault.put(DistributionConfig.LOG_LEVEL_NAME, "fine");
      nonDefault.put(DistributionConfig.LOCATORS_NAME, localHost.getHostName()+'['+port+']');
      DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
      RemoteTransportConfig transport = new RemoteTransportConfig(config);

      // start the first membership manager
      MembershipListener listener1 = new MembershipListener();
      DMStats stats1 = new MyStats();
      m1 = MemberFactory.newMembershipManager(listener1, config, transport, stats1);

      // start the second membership manager
      MembershipListener listener2 = new MembershipListener();
      DMStats stats2 = new MyStats();
      m2 = MemberFactory.newMembershipManager(listener2, config, transport, stats2);
      
      assert m2.getView().size() == 2;
      assert m1.getView().size() == 2;
      assert m1.getView().getViewId() == m2.getView().getViewId();
      
      m2.shutdown();
      assert !m2.isConnected();
      
      assert m1.getView().size() == 1;
    }
    finally {
      if (m2 != null) {
        m2.shutdown();
      }
      if (m1 != null) {
        m1.shutdown();
      }
      if (l != null) {
        l.stop();
      }
    }
    
  }
  
  /**
   * Test that failed weight calculations are correctly performed.  See bug #47342
   * @throws Exception
   */
  public void _testFailedWeight() throws Exception {
//    // in #47342 a new view was created that contained a member that was joining but
//    // was no longer reachable.  The member was included in the failed-weight and not
//    // in the previous view-weight, causing a spurious network partition to be declared
//    IpAddress members[] = new IpAddress[] {
//        new IpAddress("localhost", 1), new IpAddress("localhost", 2), new IpAddress("localhost", 3),
//        new IpAddress("localhost", 4), new IpAddress("localhost", 5), new IpAddress("localhost", 6)};
//    int i = 0;
//    // weight 3
//    members[i].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
//    members[i++].shouldntBeCoordinator(false);
//    // weight 3
//    members[i].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
//    members[i++].shouldntBeCoordinator(false);
//    // weight 15 (cache+leader)
//    members[i].setVmKind(DistributionManager.NORMAL_DM_TYPE);
//    members[i++].shouldntBeCoordinator(true);
//    // weight 0
//    members[i].setVmKind(DistributionManager.ADMIN_ONLY_DM_TYPE);
//    members[i++].shouldntBeCoordinator(true);
//    // weight 0
//    members[i].setVmKind(DistributionManager.ADMIN_ONLY_DM_TYPE);
//    members[i++].shouldntBeCoordinator(true);
//    // weight 10
//    members[i].setVmKind(DistributionManager.NORMAL_DM_TYPE);
//    members[i++].shouldntBeCoordinator(true);
//    
//    ViewId vid = new ViewId(members[0], 4);
//    Vector<IpAddress> vmbrs = new Vector<IpAddress>();
//    for (i=0; i<members.length; i++) {
//      vmbrs.add(members[i]);
//    }
//    View lastView = new View(vid, vmbrs);
//    IpAddress leader = members[2];
//    assertTrue(!leader.preferredForCoordinator());
//    
//    IpAddress joiningMember = new IpAddress("localhost", 7);
//    joiningMember.setVmKind(DistributionManager.NORMAL_DM_TYPE);
//    joiningMember.shouldntBeCoordinator(true);
//    
//    // have the joining member and another cache process (weight 10) in the failed members
//    // collection and check to make sure that the joining member is not included in failed
//    // weight calcs.
//    Set<IpAddress> failedMembers = new HashSet<IpAddress>();
//    failedMembers.add(joiningMember);
//    failedMembers.add(members[members.length-1]); // cache
//    failedMembers.add(members[members.length-2]); // admin
//    int failedWeight = GMS.processFailuresAndGetWeight(lastView, leader, failedMembers);
////    System.out.println("last view = " + lastView);
////    System.out.println("failed mbrs = " + failedMembers);
////    System.out.println("failed weight = " + failedWeight);
//    assertEquals("failure weight calculation is incorrect", 10, failedWeight);
//    assertTrue(!failedMembers.contains(members[members.length-2]));
  }
  
  static class MembershipListener implements DistributedMembershipListener {

    @Override
    public void viewInstalled(NetView view) {
    }

    @Override
    public void quorumLost(Set<InternalDistributedMember> failures,
        List<InternalDistributedMember> remainingMembers) {
    }

    @Override
    public void newMemberConnected(InternalDistributedMember m, Stub stub) {
    }

    @Override
    public void memberDeparted(InternalDistributedMember id, boolean crashed,
        String reason) {
    }

    @Override
    public void memberSuspect(InternalDistributedMember suspect,
        InternalDistributedMember whoSuspected) {
    }

    @Override
    public void messageReceived(DistributionMessage o) {
    }

    @Override
    public boolean isShutdownMsgSent() {
      return false;
    }

    @Override
    public void membershipFailure(String reason, Throwable t) {
    }

    @Override
    public DistributionManager getDM() {
      return null;
    }
    
  }
  
  static class MyStats extends DummyDMStats {
    
  }

  public static class DummyPoolStatHelper implements PoolStatHelper {
    public void startJob() {}
    public void endJob(){}
  }
  
}
