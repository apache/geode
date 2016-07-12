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

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSUtil;
import com.gemstone.gemfire.distributed.internal.membership.gms.ServiceConfig;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.JoinLeave;
import com.gemstone.gemfire.distributed.internal.membership.gms.membership.GMSJoinLeave;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.*;
import com.gemstone.gemfire.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.apache.logging.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.net.InetAddress;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

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
    int mcastPort = AvailablePortHelper.getRandomAvailableUDPPort();
    
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
      nonDefault.put(DISABLE_TCP, "true");
      nonDefault.put(MCAST_PORT, String.valueOf(mcastPort));
      nonDefault.put(LOG_FILE, "");
      nonDefault.put(LOG_LEVEL, "fine");
      nonDefault.put(GROUPS, "red, blue");
      nonDefault.put(MEMBER_TIMEOUT, "2000");
      nonDefault.put(LOCATORS, localHost.getHostName() + '[' + port + ']');
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
      
      System.out.println("testing multicast availability");
      assertTrue(m1.testMulticast());
      
      System.out.println("multicasting SerialAckedMessage from m1 to m2");
      SerialAckedMessage msg = new SerialAckedMessage();
      msg.setRecipient(m2.getLocalMember());
      msg.setMulticast(true);
      m1.send(new InternalDistributedMember[] {m2.getLocalMember()}, msg, null);
      giveUp = System.currentTimeMillis() + 5000;
      boolean verified = false;
      Throwable problem = null;
      while (giveUp > System.currentTimeMillis()) {
        try {
          verify(listener2).messageReceived(isA(SerialAckedMessage.class));
          verified = true;
          break;
        } catch (Error e) {
          problem = e;
          Thread.sleep(500);
        }
      }
      if (!verified) {
        if (problem != null) {
          problem.printStackTrace();
        }
        fail("Expected a multicast message to be received");
      }
      
      // let the managers idle for a while and get used to each other
      Thread.sleep(4000l);
      
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
  public void testJoinTimeoutSetting() throws Exception {
    long timeout = 30000;
    Properties nonDefault = new Properties();
    nonDefault.put(MEMBER_TIMEOUT, ""+timeout);
    DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
    RemoteTransportConfig transport = new RemoteTransportConfig(config,
        DistributionManager.NORMAL_DM_TYPE);
    ServiceConfig sc = new ServiceConfig(transport, config);
    assertEquals(2 * timeout + ServiceConfig.MEMBER_REQUEST_COLLECTION_INTERVAL, sc.getJoinTimeout());
    
    nonDefault.clear();
    config = new DistributionConfigImpl(nonDefault);
    transport = new RemoteTransportConfig(config,
        DistributionManager.NORMAL_DM_TYPE);
    sc = new ServiceConfig(transport, config);
    assertEquals(24000, sc.getJoinTimeout());
    

    nonDefault.clear();
    nonDefault.put(LOCATORS, SocketCreator.getLocalHost().getHostAddress() + "[" + 12345 + "]");
    config = new DistributionConfigImpl(nonDefault);
    transport = new RemoteTransportConfig(config,
        DistributionManager.NORMAL_DM_TYPE);
    sc = new ServiceConfig(transport, config);
    assertEquals(60000, sc.getJoinTimeout());
    

    timeout = 2000;
    System.setProperty("p2p.joinTimeout", ""+timeout);
    try {
      config = new DistributionConfigImpl(nonDefault);
      transport = new RemoteTransportConfig(config,
          DistributionManager.NORMAL_DM_TYPE);
      sc = new ServiceConfig(transport, config);
      assertEquals(timeout, sc.getJoinTimeout());
    } finally {
      System.getProperties().remove("p2p.joinTimeout");
    }
    
  }

  @Test
  public void testMulticastDiscoveryNotAllowed() {
    Properties nonDefault = new Properties();
    nonDefault.put(DISABLE_TCP, "true");
    nonDefault.put(MCAST_PORT, "12345");
    nonDefault.put(LOG_FILE, "");
    nonDefault.put(LOG_LEVEL, "fine");
    nonDefault.put(LOCATORS, "");
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
  
  @Test
  public void testMessagesThrowExceptionIfProcessed() throws Exception {
    DistributionManager dm = null;
    try {
      new HeartbeatMessage().process(dm);
      fail("expected an exception to be thrown");
    } catch (Exception e) {
      // okay
    }
    try {
      new HeartbeatRequestMessage().process(dm);
      fail("expected an exception to be thrown");
    } catch (Exception e) {
      // okay
    }
    try {
      new InstallViewMessage().process(dm);
      fail("expected an exception to be thrown");
    } catch (Exception e) {
      // okay
    }
    try {
      new JoinRequestMessage().process(dm);
      fail("expected an exception to be thrown");
    } catch (Exception e) {
      // okay
    }
    try {
      new JoinResponseMessage().process(dm);
      fail("expected an exception to be thrown");
    } catch (Exception e) {
      // okay
    }
    try {
      new LeaveRequestMessage().process(dm);
      fail("expected an exception to be thrown");
    } catch (Exception e) {
      // okay
    }
    try {
      new RemoveMemberMessage().process(dm);
      fail("expected an exception to be thrown");
    } catch (Exception e) {
      // okay
    }
    try {
      new SuspectMembersMessage().process(dm);
      fail("expected an exception to be thrown");
    } catch (Exception e) {
      // okay
    }
    try {
      new ViewAckMessage().process(dm);
      fail("expected an exception to be thrown");
    } catch (Exception e) {
      // okay
    }
  }
  
  
}
