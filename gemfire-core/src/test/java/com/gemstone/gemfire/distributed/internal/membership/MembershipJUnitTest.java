/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.membership.gms.ServiceConfig;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.membership.GMSJoinLeave;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class MembershipJUnitTest extends TestCase {
  static Level baseLogLevel;
  
  public MembershipJUnitTest(String name) {
    super(name);
  }

  @BeforeClass
  public static void setupClass() {
//    baseLogLevel = LogService.getBaseLogLevel();
//    LogService.setBaseLogLevel(Level.DEBUG);
  }
  
  @AfterClass
  protected void tearDown() throws Exception {
//    LogService.setBaseLogLevel(baseLogLevel);
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
    
    List<InternalDistributedMember> vmbrs = new ArrayList<>(members.length);
    for (i=0; i<members.length; i++) {
      vmbrs.add(members[i]);
    }
    List<InternalDistributedMember> empty = Collections.emptyList();
    NetView lastView = new NetView(members[0], 4, vmbrs, empty, empty);
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
    NetView newView = new NetView(members[0], 5, newMbrs, empty, failedMembers);
    
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
  public void testMultipleManagersInSameProcess() throws Exception {
    
    MembershipManager m1=null, m2=null;
    Locator l = null;
    
    try {
      
      // boot up a locator
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      InetAddress localHost = SocketCreator.getLocalHost();
      
      // this locator will hook itself up with the first MembershipManager
      // to be created
//      l = Locator.startLocator(port, new File(""), localHost);
      l = InternalLocator.startLocator(port, new File(""), null,
          null, null, localHost, false, new Properties(), true, false, null,
          false);

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
      try {
        System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY, "true");
        DistributedMembershipListener listener1 = mock(DistributedMembershipListener.class);
        DMStats stats1 = mock(DMStats.class);
        m1 = MemberFactory.newMembershipManager(listener1, config, transport, stats1);
      } finally {
        System.getProperties().remove(GMSJoinLeave.BYPASS_DISCOVERY);
      }
      
      // start the second membership manager
      DistributedMembershipListener listener2 = mock(DistributedMembershipListener.class);
      DMStats stats2 = mock(DMStats.class);
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

  @Test
  public void testMulticastDiscoveryNotAllowed() {
    Properties nonDefault = new Properties();
    nonDefault.put(DistributionConfig.DISABLE_TCP_NAME, "true");
    nonDefault.put(DistributionConfig.MCAST_PORT_NAME, "12345");
    nonDefault.put(DistributionConfig.LOG_FILE_NAME, "");
    nonDefault.put(DistributionConfig.LOG_LEVEL_NAME, "fine");
    nonDefault.put(DistributionConfig.LOCATORS_NAME, "");
    DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
    RemoteTransportConfig transport = new RemoteTransportConfig(config,
        DistributionManager.NORMAL_DM_TYPE);

    ServiceConfig serviceConfig = mock(ServiceConfig.class);
    when(serviceConfig.getDistributionConfig()).thenReturn(config);
    when(serviceConfig.getTransport()).thenReturn(transport);
    
    Services services = mock(Services.class);
    when(services.getConfig()).thenReturn(serviceConfig);
    
    GMSJoinLeave joinLeave = new GMSJoinLeave();
    try {
      joinLeave.init(services);
      fail("expected a GemFireConfigException to be thrown because no locators are configured");
    } catch (GemFireConfigException e) {
      // expected
    }
  }
  
  
}
