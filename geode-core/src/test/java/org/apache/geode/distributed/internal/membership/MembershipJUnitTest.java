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
package org.apache.geode.distributed.internal.membership;

import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_TCP;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.SerialAckedMessage;
import org.apache.geode.distributed.internal.membership.gms.GMSUtil;
import org.apache.geode.distributed.internal.membership.gms.ServiceConfig;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.distributed.internal.membership.gms.messages.HeartbeatMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.HeartbeatRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.InstallViewMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinResponseMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.RemoveMemberMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.SuspectMembersMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.ViewAckMessage;
import org.apache.geode.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.logging.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.net.InetAddress;
import java.util.List;
import java.util.Properties;

@Category({IntegrationTest.class, MembershipJUnitTest.class})
public class MembershipJUnitTest {

  /**
   * This test creates a locator with a colocated membership manager and then creates a second
   * manager that joins the system of the first.
   *
   * It then makes assertions about the state of the membership view, closes one of the managers and
   * makes more assertions. It also ensures that a cache message can be sent from one manager to the
   * other.
   */
  @Test
  public void testMultipleManagersInSameProcess() throws Exception {
    doTestMultipleManagersInSameProcessWithGroups("red, blue");
  }

  /**
   * Ensure that a large membership group doesn't cause communication issues
   */
  @Test
  public void testManagersWithLargeGroups() throws Exception {
    StringBuilder stringBuilder = new StringBuilder(80000);
    boolean first = true;
    // create 8000 10-byte group names
    for (int thousands = 1; thousands < 9; thousands++) {
      for (int group = 0; group < 1000; group++) {
        if (!first) {
          stringBuilder.append(',');
        }
        first = false;
        stringBuilder.append(String.format("%1$02d%2$08d", thousands, group));
      }
    }
    List<String> result = doTestMultipleManagersInSameProcessWithGroups(stringBuilder.toString());
    assertEquals(8000, result.size());
    for (String group : result) {
      assertEquals(10, group.length());
    }
  }

  /**
   * this runs the test with a given set of member groups. Returns the groups of the member that was
   * not the coordinator for verification that they were correctly transmitted
   */
  private List<String> doTestMultipleManagersInSameProcessWithGroups(String groups)
      throws Exception {

    MembershipManager m1 = null, m2 = null;
    Locator l = null;
    // int mcastPort = AvailablePortHelper.getRandomAvailableUDPPort();

    try {

      // boot up a locator
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      InetAddress localHost = SocketCreator.getLocalHost();

      // this locator will hook itself up with the first MembershipManager
      // to be created
      l = InternalLocator.startLocator(port, new File(""), null, null, null, localHost, false,
          new Properties(), null);

      // create configuration objects
      Properties nonDefault = new Properties();
      nonDefault.put(DISABLE_TCP, "true");
      nonDefault.put(MCAST_PORT, "0");
      nonDefault.put(LOG_FILE, "");
      nonDefault.put(LOG_LEVEL, "fine");
      nonDefault.put(GROUPS, groups);
      nonDefault.put(MEMBER_TIMEOUT, "2000");
      nonDefault.put(LOCATORS, localHost.getHostName() + '[' + port + ']');
      DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
      RemoteTransportConfig transport =
          new RemoteTransportConfig(config, DistributionManager.NORMAL_DM_TYPE);

      // start the first membership manager
      try {
        System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
        DistributedMembershipListener listener1 = mock(DistributedMembershipListener.class);
        DMStats stats1 = mock(DMStats.class);
        System.out.println("creating 1st membership manager");
        m1 = MemberFactory.newMembershipManager(listener1, config, transport, stats1,
            SecurityServiceFactory.create());
        m1.startEventProcessing();
      } finally {
        System.getProperties().remove(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
      }

      // start the second membership manager
      DistributedMembershipListener listener2 = mock(DistributedMembershipListener.class);
      DMStats stats2 = mock(DMStats.class);
      System.out.println("creating 2nd membership manager");
      m2 = MemberFactory.newMembershipManager(listener2, config, transport, stats2,
          SecurityServiceFactory.create());
      m2.startEventProcessing();

      // we have to check the views with JoinLeave because the membership
      // manager queues new views for processing through the DM listener,
      // which is a mock object in this test
      System.out.println("waiting for views to stabilize");
      JoinLeave jl1 = ((GMSMembershipManager) m1).getServices().getJoinLeave();
      JoinLeave jl2 = ((GMSMembershipManager) m2).getServices().getJoinLeave();
      long giveUp = System.currentTimeMillis() + 15000;
      for (;;) {
        try {
          assertTrue("view = " + jl2.getView(), jl2.getView().size() == 2);
          assertTrue("view = " + jl1.getView(), jl1.getView().size() == 2);
          assertTrue(jl1.getView().getCreator().equals(jl2.getView().getCreator()));
          assertTrue(jl1.getView().getViewId() == jl2.getView().getViewId());
          break;
        } catch (AssertionError e) {
          if (System.currentTimeMillis() > giveUp) {
            throw e;
          }
        }
      }

      NetView view = jl1.getView();
      InternalDistributedMember notCreator;
      if (view.getCreator().equals(jl1.getMemberID())) {
        notCreator = view.getMembers().get(1);
      } else {
        notCreator = view.getMembers().get(0);
      }
      List<String> result = notCreator.getGroups();

      System.out.println("sending SerialAckedMessage from m1 to m2");
      SerialAckedMessage msg = new SerialAckedMessage();
      msg.setRecipient(m2.getLocalMember());
      msg.setMulticast(false);
      m1.send(new InternalDistributedMember[] {m2.getLocalMember()}, msg, null);
      giveUp = System.currentTimeMillis() + 15000;
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
        AssertionError error = new AssertionError("Expected a message to be received");
        if (problem != null) {
          error.initCause(error);
        }
        throw error;
      }

      // let the managers idle for a while and get used to each other
      // Thread.sleep(4000l);

      m2.shutdown();
      assertTrue(!m2.isConnected());

      assertTrue(m1.getView().size() == 1);

      return result;
    } finally {

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
   * This test ensures that secure communications are enabled.
   *
   * This test creates a locator with a colocated membership manager and then creates a second
   * manager that joins the system of the first.
   *
   * It then makes assertions about the state of the membership view, closes one of the managers and
   * makes more assertions.
   */
  @Test
  public void testLocatorAndTwoServersJoinUsingDiffeHellman() throws Exception {

    MembershipManager m1 = null, m2 = null;
    Locator l = null;
    int mcastPort = AvailablePortHelper.getRandomAvailableUDPPort();

    try {

      // boot up a locator
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      InetAddress localHost = SocketCreator.getLocalHost();
      Properties p = new Properties();
      p.setProperty(ConfigurationProperties.SECURITY_UDP_DHALGO, "AES:128");
      // this locator will hook itself up with the first MembershipManager
      // to be created
      l = InternalLocator.startLocator(port, new File(""), null, null, null, localHost, false, p,
          null);

      // create configuration objects
      Properties nonDefault = new Properties();
      nonDefault.put(DistributionConfig.DISABLE_TCP_NAME, "true");
      nonDefault.put(DistributionConfig.MCAST_PORT_NAME, String.valueOf(mcastPort));
      nonDefault.put(DistributionConfig.LOG_FILE_NAME, "");
      nonDefault.put(DistributionConfig.LOG_LEVEL_NAME, "fine");
      nonDefault.put(DistributionConfig.GROUPS_NAME, "red, blue");
      nonDefault.put(DistributionConfig.MEMBER_TIMEOUT_NAME, "2000");
      nonDefault.put(DistributionConfig.LOCATORS_NAME, localHost.getHostName() + '[' + port + ']');
      nonDefault.put(ConfigurationProperties.SECURITY_UDP_DHALGO, "AES:128");
      DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
      RemoteTransportConfig transport =
          new RemoteTransportConfig(config, DistributionManager.NORMAL_DM_TYPE);

      // start the first membership manager
      try {
        System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
        DistributedMembershipListener listener1 = mock(DistributedMembershipListener.class);
        DMStats stats1 = mock(DMStats.class);
        System.out.println("creating 1st membership manager");
        m1 = MemberFactory.newMembershipManager(listener1, config, transport, stats1,
            SecurityServiceFactory.create());
        m1.startEventProcessing();
      } finally {
        System.getProperties().remove(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
      }

      // start the second membership manager
      DistributedMembershipListener listener2 = mock(DistributedMembershipListener.class);
      DMStats stats2 = mock(DMStats.class);
      System.out.println("creating 2nd membership manager");
      m2 = MemberFactory.newMembershipManager(listener2, config, transport, stats2,
          SecurityServiceFactory.create());
      m2.startEventProcessing();

      // we have to check the views with JoinLeave because the membership
      // manager queues new views for processing through the DM listener,
      // which is a mock object in this test
      System.out.println("waiting for views to stabilize");
      JoinLeave jl1 = ((GMSMembershipManager) m1).getServices().getJoinLeave();
      JoinLeave jl2 = ((GMSMembershipManager) m2).getServices().getJoinLeave();
      long giveUp = System.currentTimeMillis() + 15000;
      for (;;) {
        try {
          assertTrue("view = " + jl2.getView(), jl2.getView().size() == 2);
          assertTrue("view = " + jl1.getView(), jl1.getView().size() == 2);
          assertTrue(jl1.getView().getCreator().equals(jl2.getView().getCreator()));
          assertTrue(jl1.getView().getViewId() == jl2.getView().getViewId());
          break;
        } catch (AssertionError e) {
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

    } finally {

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
    nonDefault.put(MEMBER_TIMEOUT, "" + timeout);
    DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
    RemoteTransportConfig transport =
        new RemoteTransportConfig(config, DistributionManager.NORMAL_DM_TYPE);
    ServiceConfig sc = new ServiceConfig(transport, config);
    assertEquals(2 * timeout + ServiceConfig.MEMBER_REQUEST_COLLECTION_INTERVAL,
        sc.getJoinTimeout());

    nonDefault.clear();
    config = new DistributionConfigImpl(nonDefault);
    transport = new RemoteTransportConfig(config, DistributionManager.NORMAL_DM_TYPE);
    sc = new ServiceConfig(transport, config);
    assertEquals(24000, sc.getJoinTimeout());

    nonDefault.clear();
    nonDefault.put(LOCATORS, SocketCreator.getLocalHost().getHostAddress() + "[" + 12345 + "]");
    config = new DistributionConfigImpl(nonDefault);
    transport = new RemoteTransportConfig(config, DistributionManager.NORMAL_DM_TYPE);
    sc = new ServiceConfig(transport, config);
    assertEquals(60000, sc.getJoinTimeout());

    timeout = 2000;
    System.setProperty("p2p.joinTimeout", "" + timeout);
    try {
      config = new DistributionConfigImpl(nonDefault);
      transport = new RemoteTransportConfig(config, DistributionManager.NORMAL_DM_TYPE);
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
    RemoteTransportConfig transport =
        new RemoteTransportConfig(config, DistributionManager.NORMAL_DM_TYPE);

    ServiceConfig serviceConfig = mock(ServiceConfig.class);
    when(serviceConfig.getDistributionConfig()).thenReturn(config);
    when(serviceConfig.getTransport()).thenReturn(transport);

    Services services = mock(Services.class);
    when(services.getConfig()).thenReturn(serviceConfig);

    GMSJoinLeave joinLeave = new GMSJoinLeave();
    try {
      joinLeave.init(services);
      throw new Error(
          "expected a GemFireConfigException to be thrown because no locators are configured");
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
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (i % 255);
    }
    String str = GMSUtil.formatBytes(bytes, 0, bytes.length);
    System.out.println(str);
    assertEquals(600 + 4, str.length());
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
