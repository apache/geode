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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.InetAddress;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.SerialAckedMessage;
import org.apache.geode.distributed.internal.membership.adapter.ServiceConfig;
import org.apache.geode.distributed.internal.membership.adapter.auth.GMSAuthenticator;
import org.apache.geode.distributed.internal.membership.api.LifecycleListener;
import org.apache.geode.distributed.internal.membership.api.MemberData;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactory;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MembershipBuilder;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipListener;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipView;
import org.apache.geode.distributed.internal.membership.api.MessageListener;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.internal.serialization.DSFIDSerializer;

@Category({MembershipJUnitTest.class})
public class MembershipJUnitTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

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

    Membership<InternalDistributedMember> m1 = null, m2 = null;
    InternalLocator internalLocator = null;
    // int mcastPort = AvailablePortHelper.getRandomAvailableUDPPort();

    try {

      // boot up a locator
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      InetAddress localHost = LocalHostUtil.getLocalHost();

      // this locator will hook itself up with the first Membership
      // to be created
      internalLocator =
          InternalLocator.startLocator(port, new File(""), null, null, localHost, false,
              new Properties(), null, temporaryFolder.getRoot().toPath());

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
          new RemoteTransportConfig(config, ClusterDistributionManager.LOCATOR_DM_TYPE);

      // start the first membership manager
      final MembershipLocator<InternalDistributedMember> membershipLocator =
          internalLocator.getMembershipLocator();

      m1 = createMembershipManager(config, transport, membershipLocator).getLeft();

      // start the second membership manager
      final Pair<Membership, MessageListener> pair =
          createMembershipManager(config, transport, membershipLocator);
      m2 = pair.getLeft();
      final MessageListener listener2 = pair.getRight();

      // we have to check the views with JoinLeave because the membership
      // manager queues new views for processing through the DM listener,
      // which is a mock object in this test
      System.out.println("waiting for views to stabilize");
      final Membership mgr1 = m1;
      final Membership mgr2 = m2;
      long giveUp = System.currentTimeMillis() + 15000;
      for (;;) {
        try {
          assertTrue("view = " + mgr2.getView(), mgr2.getView().size() == 2);
          assertTrue("view = " + mgr1.getView(), mgr1.getView().size() == 2);
          assertTrue(mgr1.getView().getCreator().equals(mgr2.getView().getCreator()));
          assertTrue(mgr1.getView().getViewId() == mgr2.getView().getViewId());
          break;
        } catch (AssertionError e) {
          if (System.currentTimeMillis() > giveUp) {
            throw e;
          }
        }
      }

      MembershipView<InternalDistributedMember> view = mgr1.getView();
      MemberIdentifier notCreator;
      if (view.getCreator().equals(mgr1.getLocalMember())) {
        notCreator = view.getMembers().get(1);
      } else {
        notCreator = view.getMembers().get(0);
      }
      List<String> result = notCreator.getGroups();

      System.out.println("sending SerialAckedMessage from m1 to m2");
      SerialAckedMessage msg = new SerialAckedMessage();
      msg.setRecipient(m2.getLocalMember());
      msg.setMulticast(false);
      m1.send(new InternalDistributedMember[] {m2.getLocalMember()}, msg);
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
          error.initCause(problem);
        }
        throw error;
      }

      // let the managers idle for a while and get used to each other
      // Thread.sleep(4000l);

      m2.disconnect(false);
      assertTrue(!m2.isConnected());

      Membership waitingMember = m1;
      await().untilAsserted(() -> assertTrue(waitingMember.getView().size() == 1));

      return result;
    } finally {

      if (m2 != null) {
        m2.shutdown();
      }
      if (m1 != null) {
        m1.shutdown();
      }
      if (internalLocator != null) {
        internalLocator.stop();
      }
    }
  }

  private Pair<Membership, MessageListener> createMembershipManager(
      final DistributionConfigImpl config,
      final RemoteTransportConfig transport,
      final MembershipLocator<InternalDistributedMember> locator) throws MemberStartupException {
    final MembershipListener<InternalDistributedMember> listener = mock(MembershipListener.class);
    final MessageListener<InternalDistributedMember> messageListener = mock(MessageListener.class);
    final DMStats stats1 = mock(DMStats.class);
    final InternalDistributedSystem mockSystem = mock(InternalDistributedSystem.class);
    final SecurityService securityService = SecurityServiceFactory.create();
    DSFIDSerializer serializer = InternalDataSerializer.getDSFIDSerializer();
    final MemberIdentifierFactory memberFactory = mock(MemberIdentifierFactory.class);
    when(memberFactory.create(isA(MemberData.class))).thenAnswer(new Answer<MemberIdentifier>() {
      @Override
      public MemberIdentifier answer(InvocationOnMock invocation) throws Throwable {
        return new InternalDistributedMember((MemberData) invocation.getArgument(0));
      }
    });
    LifecycleListener<InternalDistributedMember> lifeCycleListener = mock(LifecycleListener.class);

    final MemberIdentifierFactory<InternalDistributedMember> memberIdentifierFactory =
        new MemberIdentifierFactory<InternalDistributedMember>() {
          @Override
          public InternalDistributedMember create(MemberData memberInfo) {
            return new InternalDistributedMember(memberInfo);
          }

          @Override
          public Comparator<InternalDistributedMember> getComparator() {
            return Comparator.naturalOrder();
          }
        };

    final TcpClient locatorClient = new TcpClient(SocketCreatorFactory
        .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        TcpSocketFactory.DEFAULT);
    final TcpSocketCreator socketCreator = SocketCreatorFactory
        .getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER);
    final GMSAuthenticator authenticator =
        new GMSAuthenticator(config.getSecurityProps(), securityService,
            mockSystem.getSecurityLogWriter(), mockSystem.getInternalLogWriter());
    final Membership<InternalDistributedMember> m1 =
        MembershipBuilder.<InternalDistributedMember>newMembershipBuilder(
            socketCreator, locatorClient, serializer, memberIdentifierFactory)
            .setMembershipLocator(locator)
            .setAuthenticator(authenticator)
            .setStatistics(stats1)
            .setMessageListener(messageListener)
            .setMembershipListener(listener)
            .setConfig(new ServiceConfig(transport, config))
            .setLifecycleListener(lifeCycleListener)
            .create();
    m1.start();
    m1.startEventProcessing();
    return Pair.of(m1, messageListener);
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

    Membership<InternalDistributedMember> m1 = null, m2 = null;
    InternalLocator internalLocator = null;
    int mcastPort = AvailablePortHelper.getRandomAvailableUDPPort();

    try {

      // boot up a locator
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      InetAddress localHost = LocalHostUtil.getLocalHost();
      Properties p = new Properties();
      p.setProperty(ConfigurationProperties.SECURITY_UDP_DHALGO, "AES:128");
      // this locator will hook itself up with the first Membership
      // to be created
      internalLocator =
          InternalLocator.startLocator(port, new File(""), null, null, localHost, false, p, null,
              temporaryFolder.getRoot().toPath());

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
          new RemoteTransportConfig(config, ClusterDistributionManager.LOCATOR_DM_TYPE);

      // start the first membership manager
      final MembershipLocator<InternalDistributedMember> membershipLocator =
          internalLocator.getMembershipLocator();

      m1 = createMembershipManager(config, transport, membershipLocator).getLeft();

      // start the second membership manager
      final Pair<Membership, MessageListener> pair =
          createMembershipManager(config, transport, membershipLocator);
      m2 = pair.getLeft();
      final MessageListener listener2 = pair.getRight();

      // we have to check the views with JoinLeave because the membership
      // manager queues new views for processing through the DM listener,
      // which is a mock object in this test
      System.out.println("waiting for views to stabilize");
      final Membership mgr1 = m1;
      final Membership mgr2 = m2;
      long giveUp = System.currentTimeMillis() + 15000;
      for (;;) {
        try {
          assertTrue("view = " + mgr2.getView(), mgr2.getView().size() == 2);
          assertTrue("view = " + mgr1.getView(), mgr1.getView().size() == 2);
          assertTrue(mgr1.getView().getCreator().equals(mgr2.getView().getCreator()));
          assertTrue(mgr1.getView().getViewId() == mgr2.getView().getViewId());
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
      m1.send(new InternalDistributedMember[] {m2.getLocalMember()}, msg);
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

      m2.disconnect(false);
      assertTrue(!m2.isConnected());

      Membership waitingMember = m1;
      await().untilAsserted(() -> assertTrue(waitingMember.getView().size() == 1));

    } finally {

      if (m2 != null) {
        m2.disconnect(false);
      }
      if (m1 != null) {
        m1.disconnect(false);
      }
      if (internalLocator != null) {
        internalLocator.stop();
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
        new RemoteTransportConfig(config, ClusterDistributionManager.NORMAL_DM_TYPE);
    MembershipConfig sc = new ServiceConfig(transport, config);
    assertEquals(2 * timeout + MembershipConfig.MEMBER_REQUEST_COLLECTION_INTERVAL,
        sc.getJoinTimeout());

    nonDefault.clear();
    config = new DistributionConfigImpl(nonDefault);
    transport = new RemoteTransportConfig(config, ClusterDistributionManager.NORMAL_DM_TYPE);
    sc = new ServiceConfig(transport, config);
    assertEquals(24000, sc.getJoinTimeout());

    nonDefault.clear();
    nonDefault.put(LOCATORS, LocalHostUtil.getLocalHost().getHostAddress() + "[" + 12345 + "]");
    config = new DistributionConfigImpl(nonDefault);
    transport = new RemoteTransportConfig(config, ClusterDistributionManager.NORMAL_DM_TYPE);
    sc = new ServiceConfig(transport, config);
    assertEquals(60000, sc.getJoinTimeout());

    timeout = 2000;
    System.setProperty("p2p.joinTimeout", "" + timeout);
    try {
      config = new DistributionConfigImpl(nonDefault);
      transport = new RemoteTransportConfig(config, ClusterDistributionManager.NORMAL_DM_TYPE);
      sc = new ServiceConfig(transport, config);
      assertEquals(timeout, sc.getJoinTimeout());
    } finally {
      System.getProperties().remove("p2p.joinTimeout");
    }

  }

}
