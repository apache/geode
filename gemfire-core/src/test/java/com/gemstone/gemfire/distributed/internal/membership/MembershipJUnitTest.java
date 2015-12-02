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
package com.gemstone.gemfire.distributed.internal.membership;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.logging.log4j.Level;
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
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSUtil;
import com.gemstone.gemfire.distributed.internal.membership.gms.ServiceConfig;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.JoinLeave;
import com.gemstone.gemfire.distributed.internal.membership.gms.membership.GMSJoinLeave;
import com.gemstone.gemfire.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class MembershipJUnitTest {
  static Level baseLogLevel;
  
  @BeforeClass
  public static void setupClass() {
//    baseLogLevel = LogService.getBaseLogLevel();
//    LogService.setBaseLogLevel(Level.DEBUG);
  }
  
  @AfterClass
  public static void tearDown() throws Exception {
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
    Set<InternalDistributedMember> empty = Collections.emptySet();
    NetView lastView = new NetView(members[0], 4, vmbrs, empty, empty);
    InternalDistributedMember leader = members[2];
    assertTrue(!leader.getNetMember().preferredForCoordinator());
    
    InternalDistributedMember joiningMember = new InternalDistributedMember("localhost", 7);
    joiningMember.setVmKind(DistributionManager.NORMAL_DM_TYPE);
    joiningMember.getNetMember().setPreferredForCoordinator(false);
    
    // have the joining member and another cache process (weight 10) in the failed members
    // collection and check to make sure that the joining member is not included in failed
    // weight calcs.
    Set<InternalDistributedMember> failedMembers = new HashSet<>(3);
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
    Set<InternalDistributedMember> actual = newView.getActualCrashedMembers(lastView);
    assertTrue(!actual.contains(members[members.length-2]));
  }
  
//  @Test
//  public void testRepeat() throws Exception {
//    for (int i=0; i<50; i++) {
//      System.out.println("--------------------run #" + i);
//      testMultipleManagersInSameProcess();
//    }
//  }
  
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
      l = InternalLocator.startLocator(port, new File(""), null,
          null, null, localHost, false, new Properties(), true, false, null,
          false);

      // create configuration objects
      Properties nonDefault = new Properties();
      nonDefault.put(DistributionConfig.DISABLE_TCP_NAME, "true");
      nonDefault.put(DistributionConfig.MCAST_PORT_NAME, "0");
      nonDefault.put(DistributionConfig.LOG_FILE_NAME, "");
//      nonDefault.put(DistributionConfig.LOG_LEVEL_NAME, "finest");
      nonDefault.put(DistributionConfig.LOCATORS_NAME, localHost.getHostName()+'['+port+']');
      DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
      RemoteTransportConfig transport = new RemoteTransportConfig(config,
          DistributionManager.NORMAL_DM_TYPE);

      // start the first membership manager
      try {
        System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
        DistributedMembershipListener listener1 = mock(DistributedMembershipListener.class);
        DMStats stats1 = mock(DMStats.class);
        System.out.println("creating 1st membership manager");
        m1 = MemberFactory.newMembershipManager(listener1, config, transport, stats1);
        m1.startEventProcessing();
      } finally {
        System.getProperties().remove(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
      }
      
      // start the second membership manager
      DistributedMembershipListener listener2 = mock(DistributedMembershipListener.class);
      DMStats stats2 = mock(DMStats.class);
      System.out.println("creating 2nd membership manager");
      m2 = MemberFactory.newMembershipManager(listener2, config, transport, stats2);
      m2.startEventProcessing();
      
      // we have to check the views with JoinLeave because the membership
      // manager queues new views for processing through the DM listener,
      // which is a mock object in this test
      System.out.println("waiting for views to stabilize");
      JoinLeave jl1 = ((GMSMembershipManager)m1).getServices().getJoinLeave();
      JoinLeave jl2 = ((GMSMembershipManager)m2).getServices().getJoinLeave();
      long giveUp = System.currentTimeMillis() + 15000;
      for (;;) {
        try {
          assertTrue("view = " + jl2.getView(), jl2.getView().size() == 2);
          assertTrue("view = " + jl1.getView(), jl1.getView().size() == 2);
          assertTrue(jl1.getView().getCreator().equals(jl2.getView().getCreator()));
          assertTrue(jl1.getView().getViewId() == jl2.getView().getViewId());
          break;
        } catch  (AssertionError e) {
          if (System.currentTimeMillis() > giveUp) {
            throw e;
          }
        }
      }
        
      m2.shutdown();
      assertTrue(!m2.isConnected());
      
      assertTrue(m1.getView().size() == 1);
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
      throw new Error("expected a GemFireConfigException to be thrown because no locators are configured");
    } catch (GemFireConfigException e) {
      // expected
    }
  }
  
  /**
   * test the GMSUtil.formatBytes() method
   */
  @Test
  public void testFormatBytes() throws Exception {
    byte[] bytes = new byte[200];
    for (int i=0; i<bytes.length; i++) {
      bytes[i] = (byte)(i%255);
    }
    String str = GMSUtil.formatBytes(bytes, 0, bytes.length);
    System.out.println(str);
    assertEquals(600+4, str.length());
  }
  
  
}
