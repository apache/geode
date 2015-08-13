/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.LonerDistributionManager.DummyDMStats;
import com.gemstone.gemfire.distributed.internal.MembershipListener;
import com.gemstone.gemfire.distributed.internal.PoolStatHelper;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.tcp.Stub;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class MembershipJUnitTest extends TestCase {
  static Level baseLogLevel;
  
  public MembershipJUnitTest(String name) {
    super(name);
  }

  @BeforeClass
  public static void setupClass() {
    baseLogLevel = LogService.getBaseLogLevel();
    LogService.setBaseLogLevel(Level.DEBUG);
  }
  
  @AfterClass
  protected void tearDown() throws Exception {
    LogService.setBaseLogLevel(baseLogLevel);
  }
  
  /**
   * Test that failed weight calculations are correctly performed.  See bug #47342
   * @throws Exception
   */
  public void testFailedWeight() throws Exception {
    // in #47342 a new view was created that contained a member that was joining but
    // was no longer reachable.  The member was included in the failed-weight and not
    // in the previous view-weight, causing a spurious network partition to be declared
    InternalDistributedMember members[] = new InternalDistributedMember[] {
        new InternalDistributedMember("localhost", 1), new InternalDistributedMember("localhost", 2), new InternalDistributedMember("localhost", 3),
        new InternalDistributedMember("localhost", 4), new InternalDistributedMember("localhost", 5), new InternalDistributedMember("localhost", 6)};
    int i = 0;
    // weight 3
    members[i].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    members[i++].getNetMember().setPreferredForCoordinator(true);
    // weight 3
    members[i].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    members[i++].getNetMember().setPreferredForCoordinator(true);
    // weight 15 (cache+leader)
    members[i].setVmKind(DistributionManager.NORMAL_DM_TYPE);
    members[i++].getNetMember().setPreferredForCoordinator(false);
    // weight 0
    members[i].setVmKind(DistributionManager.ADMIN_ONLY_DM_TYPE);
    members[i++].getNetMember().setPreferredForCoordinator(false);
    // weight 0
    members[i].setVmKind(DistributionManager.ADMIN_ONLY_DM_TYPE);
    members[i++].getNetMember().setPreferredForCoordinator(false);
    // weight 10
    members[i].setVmKind(DistributionManager.NORMAL_DM_TYPE);
    members[i++].getNetMember().setPreferredForCoordinator(false);
    
    List<InternalDistributedMember> vmbrs = new ArrayList(members.length);
    for (i=0; i<members.length; i++) {
      vmbrs.add(members[i]);
    }
    NetView lastView = new NetView(members[0], 4, vmbrs, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    InternalDistributedMember leader = members[2];
    assertTrue(!leader.getNetMember().preferredForCoordinator());
    
    InternalDistributedMember joiningMember = new InternalDistributedMember("localhost", 7);
    joiningMember.setVmKind(DistributionManager.NORMAL_DM_TYPE);
    joiningMember.getNetMember().setPreferredForCoordinator(false);
    
    // have the joining member and another cache process (weight 10) in the failed members
    // collection and check to make sure that the joining member is not included in failed
    // weight calcs.
    List<InternalDistributedMember> failedMembers = new ArrayList<InternalDistributedMember>(3);
    failedMembers.add(joiningMember);
    failedMembers.add(members[members.length-1]); // cache
    failedMembers.add(members[members.length-2]); // admin
    List<InternalDistributedMember> newMbrs = new ArrayList<InternalDistributedMember>(lastView.getMembers());
    newMbrs.removeAll(failedMembers);
    NetView newView = new NetView(members[0], 5, newMbrs, Collections.EMPTY_LIST, failedMembers);
    
    int failedWeight = newView.getCrashedMemberWeight(lastView);
//    System.out.println("last view = " + lastView);
//    System.out.println("failed mbrs = " + failedMembers);
//    System.out.println("failed weight = " + failedWeight);
    assertEquals("failure weight calculation is incorrect", 10, failedWeight);
    List<InternalDistributedMember> actual = newView.getActualCrashedMembers(lastView);
    assertTrue(!actual.contains(members[members.length-2]));
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
      
      // boot up a locator
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      InetAddress localHost = SocketCreator.getLocalHost();
      
      // this locator will hook itself up with the first MembershipManager
      // to be created
      l = Locator.startLocator(port, new File(""), localHost);

      // create configuration objects
      Properties nonDefault = new Properties();
      nonDefault.put(DistributionConfig.DISABLE_TCP_NAME, "true");
      nonDefault.put(DistributionConfig.MCAST_PORT_NAME, "0");
      nonDefault.put(DistributionConfig.LOG_FILE_NAME, "");
      nonDefault.put(DistributionConfig.LOG_LEVEL_NAME, "fine");
      nonDefault.put(DistributionConfig.LOCATORS_NAME, localHost.getHostName()+'['+port+']');
      DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
      RemoteTransportConfig transport = new RemoteTransportConfig(config,
          DistributionManager.NORMAL_DM_TYPE);

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
      System.getProperties().remove(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
      LogService.reconfigure();
      
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
