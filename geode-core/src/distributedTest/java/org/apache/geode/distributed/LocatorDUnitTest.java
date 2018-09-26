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
package org.apache.geode.distributed;

import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_LOCATOR_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityAckedMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.distributed.internal.membership.MembershipTestHook;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeaveTestHelper;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LocalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.tcp.Connection;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.util.test.TestUtil;

/**
 * Tests the ability of the {@link Locator} API to start and stop locators running in remote VMs.
 * @since GemFire 4.0
 */
@Category({MembershipTest.class})
public class LocatorDUnitTest extends JUnit4DistributedTestCase {
  private static final Logger logger = LogService.getLogger();
  private static TestHook hook;
  static volatile InternalDistributedSystem system = null;
  protected int port1;
  private int port2;

  private static void expectSystemToContainThisManyMembers(final int expectedMembers) {
    InternalDistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    assertThat(sys).isNotNull();
    assertEquals(expectedMembers, sys.getDM().getViewMembers().size());
  }

  private static boolean isSystemConnected() {
    DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    return sys != null && sys.isConnected();
  }

  private static void disconnectDistributedSystem() {
    DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    if (sys != null && sys.isConnected()) {
      sys.disconnect();
    }
    MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
  }

  /**
   * return the distributed member id for the ds on this vm
   */
  private static DistributedMember getDistributedMember(Properties props) {
    props.put("name", "vm_" + VM.getCurrentVMNum());
    DistributedSystem sys = getConnectedDistributedSystem(props);
    sys.getLogWriter().info("<ExpectedException action=add>service failure</ExpectedException>");
    sys.getLogWriter().info(
        "<ExpectedException action=add>org.apache.geode.ConnectException</ExpectedException>");
    sys.getLogWriter().info(
        "<ExpectedException action=add>org.apache.geode.ForcedDisconnectException</ExpectedException>");
    return sys.getDistributedMember();
  }

  /**
   * find a running locator and return its distributed member id
   */
  private static DistributedMember getLocatorDistributedMember() {
    return (Locator.getLocator()).getDistributedSystem().getDistributedMember();
  }

  /**
   * find the lead member and return its id
   */
  private static DistributedMember getLeadMember() {
    DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    return MembershipManagerHelper.getLeadMember(sys);
  }

  protected static void stopLocator() {
    MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
    Locator loc = Locator.getLocator();
    if (loc != null) {
      loc.stop();
      assertThat(Locator.hasLocator()).isFalse();
    }
  }

  //////// Test Methods

  /**
   * This tests that the locator can resume control as coordinator after all locators have been shut
   * down and one is restarted. It's necessary to have a lock service start so elder failover is
   * forced to happen. Prior to fixing how this worked it hung with the restarted locator trying to
   * become elder again because it put its address at the beginning of the new view it sent out.
   */
  @Test
  public void testCollocatedLocatorWithSecurity() {
    disconnectAllFromDS();

    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM vm3 = VM.getVM(3);

    port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);

    final String locators = NetworkUtils.getServerHostName() + "[" + port1 + "]";
    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(START_LOCATOR, locators);
    properties.put(LOG_LEVEL, logger.getLevel().name());
    properties.put(SECURITY_PEER_AUTH_INIT, "org.apache.geode.distributed.AuthInitializer.create");
    properties.put(SECURITY_PEER_AUTHENTICATOR,
        "org.apache.geode.distributed.MyAuthenticator.create");
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.put(USE_CLUSTER_CONFIGURATION, "false");
    addDSProps(properties);
    system = getConnectedDistributedSystem(properties);
    assertThat(system.getDistributedMember().getVmKind())
        .describedAs("expected the VM to have NORMAL vmKind")
        .isEqualTo(ClusterDistributionManager.NORMAL_DM_TYPE);

    properties.remove(START_LOCATOR);
    properties.put(LOCATORS, locators);
    SerializableRunnable startSystem = new SerializableRunnable("start system") {
      public void run() {
        system = getConnectedDistributedSystem(properties);
      }
    };
    vm1.invoke(startSystem);
    vm2.invoke(startSystem);

    // ensure that I, as a collocated locator owner, can create a cache region
    Cache cache = CacheFactory.create(system);
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("test-region");
    assertThat(region).describedAs("expected to create a region").isNotNull();

    // create a lock service and have every vm get a lock
    DistributedLockService service = DistributedLockService.create("test service", system);
    service.becomeLockGrantor();
    service.lock("foo0", 0, 0);

    vm1.invoke("get the lock service and lock something",
        () -> DistributedLockService.create("test service", system).lock("foo1", 0, 0));

    vm2.invoke("get the lock service and lock something",
        () -> DistributedLockService.create("test service", system).lock("foo2", 0, 0));

    // cause elder failover. vm1 will become the lock grantor
    system.disconnect();

    try {
      vm1.invoke("ensure grantor failover", () -> {
        DistributedLockService serviceNamed =
            DistributedLockService.getServiceNamed("test service");
        serviceNamed.lock("foo3", 0, 0);
        Awaitility.waitAtMost(10000, TimeUnit.MILLISECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
            .until(serviceNamed::isLockGrantor);
        assertThat(serviceNamed.isLockGrantor()).isTrue();
      });

      properties.put(START_LOCATOR, locators);
      system = getConnectedDistributedSystem(properties);
      System.out.println("done connecting distributed system.  Membership view is "
          + MembershipManagerHelper.getMembershipManager(system).getView());

      assertThat(MembershipManagerHelper.getCoordinator(system))
          .describedAs("should be the coordinator").isEqualTo(system.getDistributedMember());
      NetView view = MembershipManagerHelper.getMembershipManager(system).getView();
      logger.info("view after becoming coordinator is " + view);
      assertThat(system.getDistributedMember())
          .describedAs("should not be the first member in the view (" + view + ")")
          .isNotSameAs(view.get(0));

      service = DistributedLockService.create("test service", system);

      // now force a non-elder VM to get a lock. This will hang if the bug is not fixed
      vm2.invoke("get the lock service and lock something", () -> {
        DistributedLockService.getServiceNamed("test service").lock("foo4", 0, 0);
      });

      assertThat(service.isLockGrantor()).describedAs("should not have become lock grantor")
          .isFalse();

      // Now demonstrate that a new member can join and use the lock service
      properties.remove(START_LOCATOR);
      vm3.invoke(startSystem);
      vm3.invoke("get the lock service and lock something(2)",
          () -> DistributedLockService.create("test service", system).lock("foo5", 0, 0));

    } finally {
      disconnectAllFromDS();
    }
  }


  /**
   * Bug 30341 concerns race conditions in JGroups that allow two locators to start up in a
   * split-brain configuration. To work around this we have always told customers that they need to
   * stagger the starting of locators. This test configures two locators to start up simultaneously
   * and shows that they find each other and form a single system.
   */
  @Test
  public void testStartTwoLocators() throws Exception {
    disconnectAllFromDS();
    VM loc1 = VM.getVM(1);
    VM loc2 = VM.getVM(2);

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    this.port2 = port2; // for cleanup in tearDown2
    DistributedTestUtils.deleteLocatorStateFile(port1);
    DistributedTestUtils.deleteLocatorStateFile(port2);
    final String host0 = NetworkUtils.getServerHostName();
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = getClusterProperties(locators, "false");
    addDSProps(properties);

    startVerifyAndStopLocator(loc1, loc2, port1, port2, properties);
    startVerifyAndStopLocator(loc1, loc2, port1, port2, properties);
    startVerifyAndStopLocator(loc1, loc2, port1, port2, properties);
  }


  @Test
  public void testStartTwoLocatorsWithSingleKeystoreSSL() throws Exception {
    disconnectAllFromDS();

    VM loc1 = VM.getVM(1);
    VM loc2 = VM.getVM(2);

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    this.port2 = port2; // for cleanup in tearDown2
    DistributedTestUtils.deleteLocatorStateFile(port1);
    DistributedTestUtils.deleteLocatorStateFile(port2);
    final String host0 = NetworkUtils.getServerHostName();
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = getClusterProperties(locators, "false");
    properties.put(SSL_CIPHERS, "any");
    properties.put(SSL_PROTOCOLS, "TLSv1,TLSv1.1,TLSv1.2");
    properties.put(SSL_KEYSTORE, getSingleKeyKeystore());
    properties.put(SSL_KEYSTORE_PASSWORD, "password");
    properties.put(SSL_KEYSTORE_TYPE, "JKS");
    properties.put(SSL_TRUSTSTORE, getSingleKeyKeystore());
    properties.put(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.put(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.LOCATOR.getConstant());

    startVerifyAndStopLocator(loc1, loc2, port1, port2, properties);
  }

  @Test
  public void testStartTwoLocatorsWithMultiKeystoreSSL() throws Exception {
    disconnectAllFromDS();

    VM loc1 = VM.getVM(1);
    VM loc2 = VM.getVM(2);

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    this.port2 = port2; // for cleanup in tearDown2
    DistributedTestUtils.deleteLocatorStateFile(port1);
    DistributedTestUtils.deleteLocatorStateFile(port2);
    final String host0 = NetworkUtils.getServerHostName();
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = getClusterProperties(locators, "false");
    properties.put(SSL_CIPHERS, "any");
    properties.put(SSL_PROTOCOLS, "any");
    properties.put(SSL_KEYSTORE, getMultiKeyKeystore());
    properties.put(SSL_KEYSTORE_PASSWORD, "password");
    properties.put(SSL_KEYSTORE_TYPE, "JKS");
    properties.put(SSL_TRUSTSTORE, getMultiKeyTruststore());
    properties.put(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.put(SSL_LOCATOR_ALIAS, "locatorkey");
    properties.put(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.LOCATOR.getConstant());

    startVerifyAndStopLocator(loc1, loc2, port1, port2, properties);
  }

  @Test
  public void testNonSSLLocatorDiesWhenConnectingToSSLLocator() {
    IgnoredException.addIgnoredException("Unrecognized SSL message, plaintext connection");
    IgnoredException.addIgnoredException("LocatorCancelException");
    disconnectAllFromDS();

    final String hostname = NetworkUtils.getServerHostName();
    VM loc1 = VM.getVM(1);
    VM loc2 = VM.getVM(2);

    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    properties.put(DISABLE_AUTO_RECONNECT, "true");
    properties.put(MEMBER_TIMEOUT, "2000");
    properties.put(LOG_LEVEL, logger.getLevel().name());
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.put(SSL_CIPHERS, "any");
    properties.put(SSL_PROTOCOLS, "any");
    properties.put(SSL_KEYSTORE, getSingleKeyKeystore());
    properties.put(SSL_KEYSTORE_PASSWORD, "password");
    properties.put(SSL_KEYSTORE_TYPE, "JKS");
    properties.put(SSL_TRUSTSTORE, getSingleKeyKeystore());
    properties.put(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.put(SSL_REQUIRE_AUTHENTICATION, "true");
    properties.put(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.LOCATOR.getConstant());

    try {
      // we set port1 so that the state file gets cleaned up later.
      port1 = startLocatorGetPort(loc1, properties, 0);

      loc1.invoke("expect only one member in system",
          () -> expectSystemToContainThisManyMembers(1));

      properties.remove(SSL_ENABLED_COMPONENTS);
      properties.put(LOCATORS, hostname + "[" + port1 + "]");
      // we set port2 so that the state file gets cleaned up later.
      port2 = startLocatorGetPort(loc2, properties, 0);
      loc1.invoke("expect only one member in system",
          () -> expectSystemToContainThisManyMembers(1));

    } finally {
      loc1.invoke("stop locator", LocatorDUnitTest::stopLocator);
      // loc2 should die from inability to connect.
      loc2.invoke(() -> Awaitility.await("locator2 dies").atMost(15, TimeUnit.SECONDS)
          .until(() -> Locator.getLocator() == null));
    }
  }

  @Test
  public void testSSLEnabledLocatorDiesWhenConnectingToNonSSLLocator() {
    IgnoredException.addIgnoredException("Remote host closed connection during handshake");
    IgnoredException.addIgnoredException("Unrecognized SSL message, plaintext connection");
    IgnoredException.addIgnoredException("LocatorCancelException");

    disconnectAllFromDS();

    VM loc1 = VM.getVM(1);
    VM loc2 = VM.getVM(2);

    final String hostname = NetworkUtils.getServerHostName();
    final Properties properties = getClusterProperties("", "false");
    properties.remove(LOCATORS);
    properties.put(SSL_CIPHERS, "any");
    properties.put(SSL_PROTOCOLS, "any");

    try {
      // we set port1 so that the state file gets cleaned up later.
      port1 = startLocatorGetPort(loc1, properties, 0);
      loc1.invoke("expectSystemToContainThisManyMembers",
          () -> expectSystemToContainThisManyMembers(1));

      properties.put(SSL_KEYSTORE, getSingleKeyKeystore());
      properties.put(SSL_KEYSTORE_PASSWORD, "password");
      properties.put(SSL_KEYSTORE_TYPE, "JKS");
      properties.put(SSL_TRUSTSTORE, getSingleKeyKeystore());
      properties.put(SSL_TRUSTSTORE_PASSWORD, "password");
      properties.put(SSL_REQUIRE_AUTHENTICATION, "true");
      properties.put(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.LOCATOR.getConstant());

      final String locators = hostname + "[" + port1 + "]";
      properties.put(LOCATORS, locators);

      // we set port2 so that the state file gets cleaned up later.
      port2 = startLocatorGetPort(loc2, properties, 0);
      loc1.invoke("expectSystemToContainThisManyMembers",
          () -> expectSystemToContainThisManyMembers(1));
    } finally {
      // loc2 should die from inability to connect.
      loc2.invoke(() -> Awaitility.await("locator2 dies").atMost(30, TimeUnit.SECONDS)
          .until(() -> Locator.getLocator() == null));
      loc1.invoke("expectSystemToContainThisManyMembers",
          () -> expectSystemToContainThisManyMembers(1));
      loc1.invoke("stop locator", LocatorDUnitTest::stopLocator);
    }
  }

  @Test
  public void testStartTwoLocatorsWithDifferentSSLCertificates() {
    IgnoredException.addIgnoredException("Remote host closed connection during handshake");
    IgnoredException
        .addIgnoredException("unable to find valid certification path to requested target");
    IgnoredException.addIgnoredException("Received fatal alert: certificate_unknown");
    IgnoredException.addIgnoredException("LocatorCancelException");
    disconnectAllFromDS();
    IgnoredException.addIgnoredException("Unrecognized SSL message, plaintext connection");
    disconnectAllFromDS();

    VM loc1 = VM.getVM(1);
    VM loc2 = VM.getVM(2);

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    this.port2 = port2; // for cleanup in tearDown2
    DistributedTestUtils.deleteLocatorStateFile(port1);
    DistributedTestUtils.deleteLocatorStateFile(port2);
    final String host0 = NetworkUtils.getServerHostName();
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = getClusterProperties(locators, "false");
    properties.put(SSL_CIPHERS, "any");
    properties.put(SSL_PROTOCOLS, "any");
    properties.put(SSL_KEYSTORE, getSingleKeyKeystore());
    properties.put(SSL_KEYSTORE_PASSWORD, "password");
    properties.put(SSL_KEYSTORE_TYPE, "JKS");
    properties.put(SSL_TRUSTSTORE, getSingleKeyKeystore());
    properties.put(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.put(SSL_REQUIRE_AUTHENTICATION, "true");
    properties.put(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.LOCATOR.getConstant());

    try {
      startLocator(loc1, properties, port1);
      loc1.invoke("expectSystemToContainThisManyMembers",
          () -> expectSystemToContainThisManyMembers(1));

      properties.put(SSL_KEYSTORE, getMultiKeyKeystore());
      properties.put(SSL_TRUSTSTORE, getMultiKeyTruststore());
      properties.put(SSL_LOCATOR_ALIAS, "locatorkey");

      startLocator(loc2, properties, port2);

    } finally {
      try {
        loc1.invoke("expectSystemToContainThisManyMembers",
            () -> expectSystemToContainThisManyMembers(1));
      } finally {
        loc1.invoke("stop locator", LocatorDUnitTest::stopLocator);
      }
    }
  }

  /**
   * test lead member selection
   */
  @Test
  public void testLeadMemberSelection() throws Exception {
    disconnectAllFromDS();

    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM vm3 = VM.getVM(3);

    port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    final String locators = NetworkUtils.getServerHostName() + "[" + port1 + "]";
    final Properties properties = getBasicProperties(locators);
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, "true");
    properties.put(DISABLE_AUTO_RECONNECT, "true");

    addDSProps(properties);
    File logFile = new File("");
    if (logFile.exists()) {
      logFile.delete();
    }
    Locator locator = Locator.startLocatorAndDS(port1, logFile, properties);
    try {
      DistributedSystem sys = locator.getDistributedSystem();

      assertThat(MembershipManagerHelper.getLeadMember(sys)).isNull();

      // connect three vms and then watch the lead member selection as they
      // are disconnected/reconnected
      properties.put("name", "vm1");

      DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));

      assertLeadMember(mem1, sys);

      properties.put("name", "vm2");
      DistributedMember mem2 = vm2.invoke(() -> getDistributedMember(properties));
      assertLeadMember(mem1, sys);

      properties.put("name", "vm3");
      DistributedMember mem3 = vm3.invoke(() -> getDistributedMember(properties));
      assertLeadMember(mem1, sys);

      // after disconnecting the first vm, the second one should become the leader
      vm1.invoke(LocatorDUnitTest::disconnectDistributedSystem);
      MembershipManagerHelper.getMembershipManager(sys).waitForDeparture(mem1);
      assertLeadMember(mem2, sys);

      properties.put("name", "vm1");
      mem1 = vm1.invoke(() -> getDistributedMember(properties));
      assertLeadMember(mem2, sys);

      vm2.invoke(LocatorDUnitTest::disconnectDistributedSystem);
      MembershipManagerHelper.getMembershipManager(sys).waitForDeparture(mem2);
      assertLeadMember(mem3, sys);

      vm1.invoke(LocatorDUnitTest::disconnectDistributedSystem);
      MembershipManagerHelper.getMembershipManager(sys).waitForDeparture(mem1);
      assertLeadMember(mem3, sys);

      vm3.invoke(LocatorDUnitTest::disconnectDistributedSystem);
      MembershipManagerHelper.getMembershipManager(sys).waitForDeparture(mem3);
      assertLeadMember(null, sys);

    } finally {
      locator.stop();
    }
  }


  /**
   * test lead member and coordinator failure with network partition detection enabled. It would be
   * nice for this test to have more than two "server" vms, to demonstrate that they all exit when
   * the leader and potential- coordinator both disappear in the loss-correlation-window, but there
   * are only four vms available for dunit testing.
   * <p>
   * So, we start two locators with admin distributed systems, then start two regular distributed
   * members.
   * <p>
   * We kill the second locator (which is not the view coordinator) and then kill the non-lead
   * member. That should be okay - the lead and remaining locator continue to run.
   * <p>
   * We then kill the lead member and demonstrate that the original locator (which is now the sole
   * remaining member) shuts itself down.
   */
  @Test
  public void testLeadAndCoordFailure() throws Exception {
    IgnoredException.addIgnoredException("Possible loss of quorum due");
    disconnectAllFromDS();

    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM locatorVM = VM.getVM(3);
    Locator locator = null;

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    DistributedTestUtils.deleteLocatorStateFile(port1, port2);
    final String host0 = NetworkUtils.getServerHostName();
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = getClusterProperties(locators, "true");

    addDSProps(properties);
    try {
      File logFile = new File("");
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
      final DistributedSystem sys = locator.getDistributedSystem();
      sys.getLogWriter()
          .info("<ExpectedException action=add>java.net.ConnectException</ExpectedException>");
      MembershipManagerHelper.inhibitForcedDisconnectLogging(true);
      startLocator(locatorVM, properties, port2);

      assertThat(MembershipManagerHelper.getLeadMember(sys)).isNull();

      // properties.put("log-level", getDUnitLogLevel());

      DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));
      vm2.invoke(() -> getDistributedMember(properties));
      assertLeadMember(mem1, sys);

      assertThat(sys.getDistributedMember()).isEqualTo(MembershipManagerHelper.getCoordinator(sys));

      // crash the second vm and the locator. Should be okay
      DistributedTestUtils.crashDistributedSystem(vm2);
      locatorVM.invoke(() -> {
        Locator loc = Locator.getLocator();
        DistributedSystem msys = loc.getDistributedSystem();
        MembershipManagerHelper.crashDistributedSystem(msys);
        loc.stop();
      });

      assertThat(vm1.invoke(LocatorDUnitTest::isSystemConnected))
          .describedAs("Distributed system should not have disconnected").isTrue();

      // ensure quorumLost is properly invoked
      ClusterDistributionManager dm =
          (ClusterDistributionManager) ((InternalDistributedSystem) sys).getDistributionManager();
      MyMembershipListener listener = new MyMembershipListener();
      dm.addMembershipListener(listener);
      // ensure there is an unordered reader thread for the member
      new HighPriorityAckedMessage().send(Collections.singleton(mem1), false);

      // disconnect the first vm and demonstrate that the third vm and the
      // locator notice the failure and exit
      DistributedTestUtils.crashDistributedSystem(vm1);

      /*
       * This vm is watching vm1, which is watching vm2 which is watching locatorVM. It will take 3 * (3
       * * member-timeout) milliseconds to detect the full failure and eject the lost members from
       * the view.
       */

      logger.info("waiting for my distributed system to disconnect due to partition detection");

      Awaitility.waitAtMost(24000, TimeUnit.MILLISECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
          .until(() -> !sys.isConnected());

      if (sys.isConnected()) {
        fail(
            "Distributed system did not disconnect as expected - network partition detection is broken");
      }
      // quorumLost should be invoked if we get a ForcedDisconnect in this situation
      assertThat(listener.quorumLostInvoked).describedAs("expected quorumLost to be invoked")
          .isTrue();
      assertThat(listener.suspectReasons.contains(Connection.INITIATING_SUSPECT_PROCESSING))
          .describedAs("expected suspect processing initiated by TCPConduit").isTrue();
    } finally {
      if (locator != null) {
        locator.stop();
      }
      LogWriter bLogger = new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);
      bLogger.info("<ExpectedException action=remove>service failure</ExpectedException>");
      bLogger
          .info("<ExpectedException action=remove>java.net.ConnectException</ExpectedException>");
      bLogger.info(
          "<ExpectedException action=remove>org.apache.geode.ForcedDisconnectException</ExpectedException>");
      disconnectAllFromDS();
    }
  }

  /**
   * test lead member failure and normal coordinator shutdown with network partition detection
   * enabled.
   * <p>
   * Start two locators with admin distributed systems, then start two regular distributed members.
   * <p>
   * We kill the lead member and demonstrate that the other members continue to operate normally.
   * <p>
   * We then shut down the group coordinator and observe the second locator pick up the job and the
   * remaining member continues to operate normally.
   */
  @Test
  public void testLeadFailureAndCoordShutdown() throws Exception {
    disconnectAllFromDS();

    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM locatorVM = VM.getVM(3);
    Locator locator = null;

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    this.port2 = port2;
    DistributedTestUtils.deleteLocatorStateFile(port1, port2);
    final String host0 = NetworkUtils.getServerHostName();
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = getClusterProperties(locators, "true");

    addDSProps(properties);

    try {
      File logFile = new File("");
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
      DistributedSystem sys = locator.getDistributedSystem();
      locatorVM.invoke(() -> {
        File lf = new File("");
        try {
          Locator.startLocatorAndDS(port2, lf, properties);
          MembershipManagerHelper.inhibitForcedDisconnectLogging(true);
        } catch (IOException ios) {
          throw new RuntimeException("Unable to start locator2", ios);
        }
      });

      assertThat(MembershipManagerHelper.getLeadMember(sys)).isNull();

      DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));
      DistributedMember mem2 = vm2.invoke(() -> getDistributedMember(properties));

      assertThat(mem1).isEqualTo(MembershipManagerHelper.getLeadMember(sys));

      assertThat(sys.getDistributedMember()).isEqualTo(MembershipManagerHelper.getCoordinator(sys));

      MembershipManagerHelper.inhibitForcedDisconnectLogging(true);

      // crash the lead vm. Should be okay
      vm1.invoke(() -> {
        DistributedSystem msys = InternalDistributedSystem.getAnyInstance();
        msys.getLogWriter()
            .info("<ExpectedException action=add>service failure</ExpectedException>");
        msys.getLogWriter().info(
            "<ExpectedException action=add>org.apache.geode.ConnectException</ExpectedException>");
        msys.getLogWriter().info(
            "<ExpectedException action=add>org.apache.geode.ForcedDisconnectException</ExpectedException>");
        MembershipManagerHelper.crashDistributedSystem(msys);
      });

      checkSystemConnected(vm2, locatorVM);
      // stop the locator normally. This should also be okay
      locator.stop();

      Awaitility.waitAtMost(5, TimeUnit.MINUTES).pollInterval(200, TimeUnit.MILLISECONDS)
          .until(() -> {
            assertThat(Locator.getLocator()).describedAs("locator is not stopped").isNull();
            return true;
          });

      checkSystemConnectedInVMs(vm2, locatorVM);

      // the remaining non-locator member should now be the lead member
      assertEquals(
          "This test sometimes fails.  If the log contains "
              + "'failed to collect all ACKs' it is a false failure.",
          mem2, vm2.invoke(LocatorDUnitTest::getLeadMember));

      // disconnect the first vm and demonstrate that the third vm and the
      // locator notice the failure and exit
      vm2.invoke(LocatorDUnitTest::disconnectDistributedSystem);
      locatorVM.invoke(LocatorDUnitTest::stopLocator);
    } finally {
      MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
      if (locator != null) {
        locator.stop();
      }
      try {
        locatorVM.invoke(LocatorDUnitTest::stopLocator);
      } catch (Exception e) {
        logger.error("failed to stop locator in vm 3", e);
      }
    }
  }

  /**
   * test lead member failure and normal coordinator shutdown with network partition detection
   * enabled.
   * <p>
   * Start one locators with admin distributed systems, then start two regular distributed members.
   * <p>
   * We kill the lead member and demonstrate that the other members continue to operate normally.
   * <p>
   * We then shut down the group coordinator and observe the second locator pick up the job and the
   * remaining member continues to operate normally.
   */
  // disabled on trunk - should be reenabled on cedar_dev_Oct12
  // this test leaves a CloserThread around forever that logs "pausing" messages every 500 ms
  @Test
  public void testForceDisconnectAndPeerShutdownCause() throws Exception {
    disconnectAllFromDS();

    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM locatorVM = VM.getVM(3);
    Locator locator = null;

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    DistributedTestUtils.deleteLocatorStateFile(port1, port2);
    final String host0 = NetworkUtils.getServerHostName();
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";

    final Properties properties = getClusterProperties(locators, "true");

    addDSProps(properties);

    try {
      File logFile = new File("");
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
      DistributedSystem sys = locator.getDistributedSystem();
      startLocator(locatorVM, properties, port2);

      assertNull(MembershipManagerHelper.getLeadMember(sys));

      final DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));
      vm2.invoke(() -> getDistributedMember(properties));

      assertThat(mem1).isEqualTo(MembershipManagerHelper.getLeadMember(sys));

      assertThat(sys.getDistributedMember()).isEqualTo(MembershipManagerHelper.getCoordinator(sys));

      checkSystemConnected(vm2, locatorVM);

      vm2.invokeAsync(() -> {
        DistributedSystem msys = InternalDistributedSystem.getAnyInstance();
        msys.getLogWriter()
            .info("<ExpectedException action=add>service failure</ExpectedException>");
        msys.getLogWriter().info(
            "<ExpectedException action=add>org.apache.geode.ConnectException</ExpectedException>");
        msys.getLogWriter().info(
            "<ExpectedException action=add>org.apache.geode.ForcedDisconnectException</ExpectedException>");
        msys.getLogWriter()
            .info("<ExpectedException action=add>Possible loss of quorum</ExpectedException>");
        hook = new TestHook();
        MembershipManagerHelper.getMembershipManager(msys).registerTestHook(hook);
        try {
          MembershipManagerHelper.crashDistributedSystem(msys);
        } finally {
          hook.reset();
        }
      });

      Wait.pause(1000); // 4 x the member-timeout

      // request member removal for first peer from second peer.
      vm2.invoke(() -> {
        DistributedSystem msys = InternalDistributedSystem.getAnyInstance();
        MembershipManager mmgr = MembershipManagerHelper.getMembershipManager(msys);

        // check for shutdown cause in MembershipManager. Following call should
        // throw DistributedSystemDisconnectedException which should have cause as
        // ForceDisconnectException.
        try {
          msys.getLogWriter().info(
              "<ExpectedException action=add>Membership: requesting removal of </ExpectedException>");
          mmgr.requestMemberRemoval(mem1, "test reasons");
          msys.getLogWriter().info(
              "<ExpectedException action=remove>Membership: requesting removal of </ExpectedException>");
          fail("It should have thrown exception in requestMemberRemoval");
        } catch (DistributedSystemDisconnectedException e) {
          Throwable cause = e.getCause();
          assertThat(cause instanceof ForcedDisconnectException)
              .describedAs("This should have been ForceDisconnectException but found " + cause)
              .isTrue();
        } finally {
          hook.reset();
        }
      });

    } finally {
      if (locator != null) {
        locator.stop();
      }
      locatorVM.invoke(LocatorDUnitTest::stopLocator);
      assertThat(Locator.getLocator()).describedAs("locator is not stopped").isNull();
    }
  }

  /**
   * test lead member shutdown and coordinator crashing with network partition detection enabled.
   * <p>
   * Start two locators with admin distributed systems, then start two regular distributed members.
   * <p>
   * We kill the coordinator and shut down the lead member and observe the second locator pick up
   * the job and the remaining member continue to operate normally.
   */
  @Test
  public void testLeadShutdownAndCoordFailure() throws Exception {
    disconnectAllFromDS();

    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM locatorVM = VM.getVM(3);
    Locator locator = null;

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    DistributedTestUtils.deleteLocatorStateFile(port1, port2);
    final String host0 = NetworkUtils.getServerHostName();
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = getClusterProperties(locators, "true");

    addDSProps(properties);
    try {
      locatorVM.invoke(() -> {
        File lf = new File("");
        try {
          Locator.startLocatorAndDS(port2, lf, properties);
        } catch (IOException ios) {
          throw new RuntimeException("Unable to start locator1", ios);
        }
      });

      File logFile = new File("");
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
      DistributedSystem sys = locator.getDistributedSystem();
      sys.getLogWriter().info(
          "<ExpectedException action=add>org.apache.geode.ForcedDisconnectException</ExpectedException>");

      assertNull(MembershipManagerHelper.getLeadMember(sys));

      DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));

      vm1.invoke(() -> MembershipManagerHelper.inhibitForcedDisconnectLogging(true));

      DistributedMember mem2 = vm2.invoke(() -> getDistributedMember(properties));

      DistributedMember loc1Mbr = locatorVM.invoke(LocatorDUnitTest::getLocatorDistributedMember);

      assertLeadMember(mem1, sys);

      assertThat(loc1Mbr).isEqualTo(MembershipManagerHelper.getCoordinator(sys));

      // crash the lead locator. Should be okay
      locatorVM.invoke("crash locator", () -> {
        Locator loc = Locator.getLocator();
        DistributedSystem msys = loc.getDistributedSystem();
        msys.getLogWriter()
            .info("<ExpectedException action=add>service failure</ExpectedException>");
        msys.getLogWriter().info(
            "<ExpectedException action=add>org.apache.geode.ForcedDisconnectException</ExpectedException>");
        msys.getLogWriter().info(
            "<ExpectedException action=add>org.apache.geode.ConnectException</ExpectedException>");
        MembershipManagerHelper.crashDistributedSystem(msys);
        loc.stop();
      });

      Awaitility.waitAtMost(5, TimeUnit.MINUTES).pollInterval(200, TimeUnit.MILLISECONDS)
          .until(sys::isConnected);

      checkSystemConnected(vm1, vm2);

      // disconnect the first vm and demonstrate that the non-lead vm and the
      // locator notice the failure and continue to run
      vm1.invoke(LocatorDUnitTest::disconnectDistributedSystem);

      Awaitility.waitAtMost(5, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
          .until(() -> vm2.invoke(LocatorDUnitTest::isSystemConnected));

      assertThat(vm2.invoke(LocatorDUnitTest::isSystemConnected))
          .describedAs("Distributed system should not have disconnected").isTrue();

      Awaitility.waitAtMost(5, TimeUnit.MINUTES).pollInterval(100, TimeUnit.MILLISECONDS)
          .until(() -> {
            assertThat(sys.getDistributedMember())
                .isEqualTo(MembershipManagerHelper.getCoordinator(sys));
            return true;
          });
      Awaitility.waitAtMost(5, TimeUnit.MINUTES).pollInterval(100, TimeUnit.MILLISECONDS)
          .until(() -> {
            assertThat(mem2).isEqualTo(MembershipManagerHelper.getLeadMember(sys));
            return true;
          });

    } finally {
      vm2.invoke(LocatorDUnitTest::disconnectDistributedSystem);

      if (locator != null) {
        locator.stop();
      }
      locatorVM.invoke(LocatorDUnitTest::stopLocator);
    }
  }


  /**
   * Tests that attempting to connect to a distributed system in which no locator is defined throws
   * an exception.
   */
  @Test
  public void testNoLocator() {
    disconnectAllFromDS();

    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    String locators = NetworkUtils.getServerHostName() + "[" + port + "]";
    Properties props = getBasicProperties(locators);

    addDSProps(props);
    final String expected = "java.net.ConnectException";
    final String addExpected = "<ExpectedException action=add>" + expected + "</ExpectedException>";
    final String removeExpected =
        "<ExpectedException action=remove>" + expected + "</ExpectedException>";

    LogWriter bgexecLogger = new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);
    bgexecLogger.info(addExpected);

    boolean exceptionOccurred = true;
    try {
      getConnectedDistributedSystem(props);
      exceptionOccurred = false;

    } catch (DistributionException ex) {
      // I guess it can throw this too...

    } catch (GemFireConfigException ex) {
      String s = ex.getMessage();
      assertThat(s.contains("Locator does not exist")).isTrue();

    } catch (Exception ex) {
      // if you see this fail, determine if unexpected exception is expected
      // if expected then add in a catch block for it above this catch
      throw new RuntimeException("Failed with unexpected exception", ex);
    } finally {
      bgexecLogger.info(removeExpected);
    }

    if (!exceptionOccurred) {
      fail("Should have thrown a GemFireConfigException");
    }
  }

  /**
   * Tests starting one locator in a remote VM and having multiple members of the distributed system
   * join it. This ensures that members start up okay, and that handling of a stopped locator is
   * correct.
   * <p>
   * The locator is then restarted and is shown to take over the role of membership coordinator.
   */
  @Test
  public void testOneLocator() {
    disconnectAllFromDS();

    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);

    final int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    final String locators = NetworkUtils.getServerHostName() + "[" + port + "]";

    startLocatorWithSomeBasicProperties(vm0, port);

    try {
      SerializableRunnable connect = new SerializableRunnable("Connect to " + locators) {
        public void run() {
          Properties props = getBasicProperties(locators);
          props.setProperty(MEMBER_TIMEOUT, "1000");
          addDSProps(props);
          getConnectedDistributedSystem(props);
        }
      };

      vm1.invoke(connect);
      vm2.invoke(connect);

      Properties props = getBasicProperties(locators);
      props.setProperty(MEMBER_TIMEOUT, "1000");
      addDSProps(props);
      system = getConnectedDistributedSystem(props);

      final DistributedMember coord = MembershipManagerHelper.getCoordinator(system);
      logger.info("coordinator before termination of locator is " + coord);

      vm0.invoke(LocatorDUnitTest::stopLocator);

      // now ensure that one of the remaining members became the coordinator

      Awaitility.waitAtMost(15000, TimeUnit.MILLISECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
          .until(() -> !coord.equals(MembershipManagerHelper.getCoordinator(system)));

      DistributedMember newCoord = MembershipManagerHelper.getCoordinator(system);
      logger.info("coordinator after shutdown of locator was " + newCoord);
      if (coord.equals(newCoord)) {
        fail("another member should have become coordinator after the locator was stopped");
      }

      system.disconnect();

      vm1.invoke(LocatorDUnitTest::disconnectDistributedSystem);
      vm2.invoke(LocatorDUnitTest::disconnectDistributedSystem);

    } finally {
      vm0.invoke(LocatorDUnitTest::stopLocator);
    }
  }


  /**
   * Tests starting one locator in a remote VM and having multiple members of the distributed system
   * join it. This ensures that members start up okay, and that handling of a stopped locator is
   * correct. It then restarts the locator to demonstrate that it can connect to and function as the
   * group coordinator
   */
  @Test
  public void testLocatorBecomesCoordinator() {
    disconnectAllFromDS();
    final String expected = "java.net.ConnectException";
    final String addExpected = "<ExpectedException action=add>" + expected + "</ExpectedException>";
    final String removeExpected =
        "<ExpectedException action=remove>" + expected + "</ExpectedException>";

    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);

    final int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    final String locators = NetworkUtils.getServerHostName() + "[" + port + "]";

    startLocatorPreferredCoordinators(vm0, port);

    try {

      final Properties props = new Properties();
      props.setProperty(LOCATORS, locators);
      props.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "true");
      props.put(ENABLE_CLUSTER_CONFIGURATION, "false");

      addDSProps(props);
      vm1.invoke(() -> {
        DistributedSystem sys = getSystem(props);
        sys.getLogWriter().info(addExpected);
      });
      vm2.invoke(() -> {
        DistributedSystem sys = getSystem(props);
        sys.getLogWriter().info(addExpected);
      });

      system = getSystem(props);

      final DistributedMember coord = MembershipManagerHelper.getCoordinator(system);
      logger.info("coordinator before termination of locator is " + coord);

      vm0.invoke(LocatorDUnitTest::stopLocator);

      // now ensure that one of the remaining members became the coordinator
      Awaitility.waitAtMost(15000, TimeUnit.MILLISECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
          .until(() -> !coord.equals(MembershipManagerHelper.getCoordinator(system)));

      DistributedMember newCoord = MembershipManagerHelper.getCoordinator(system);
      logger.info("coordinator after shutdown of locator was " + newCoord);
      if (newCoord == null || coord.equals(newCoord)) {
        fail("another member should have become coordinator after the locator was stopped: "
            + newCoord);
      }

      // restart the locator to demonstrate reconnection & make disconnects faster
      // it should also regain the role of coordinator, so we check to make sure
      // that the coordinator has changed
      startLocatorPreferredCoordinators(vm0, port);

      final DistributedMember tempCoord = newCoord;

      Awaitility.waitAtMost(5000, TimeUnit.MILLISECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
          .until(() -> !tempCoord.equals(MembershipManagerHelper.getCoordinator(system)));

      system.disconnect();
      LogWriter bgexecLogger = new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);
      bgexecLogger.info(removeExpected);

      checkConnectionAndPrintInfo(vm1);
      checkConnectionAndPrintInfo(vm2);
      vm0.invoke(LocatorDUnitTest::stopLocator);
    } finally {
      vm0.invoke(LocatorDUnitTest::stopLocator);
    }
  }

  /**
   * Tests starting multiple locators in multiple VMs.
   */
  @Test
  public void testMultipleLocators() {
    disconnectAllFromDS();

    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM vm3 = VM.getVM(3);

    int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = freeTCPPorts[0];
    this.port1 = port1;
    final int port2 = freeTCPPorts[1];
    this.port2 = port2;
    DistributedTestUtils.deleteLocatorStateFile(port1, port2);
    final String host0 = NetworkUtils.getServerHostName();
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";

    final Properties dsProps = getBasicProperties(locators);
    dsProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    addDSProps(dsProps);

    startLocator(vm0, dsProps, port1);
    try {
      startLocator(vm3, dsProps, port2);
      try {

        SerializableRunnable connect = new SerializableRunnable("Connect to " + locators) {
          public void run() {
            Properties props = getBasicProperties(locators);
            addDSProps(props);
            getConnectedDistributedSystem(props);
          }
        };
        vm1.invoke(connect);
        vm2.invoke(connect);

        Properties props = getBasicProperties(locators);

        addDSProps(props);
        system = getConnectedDistributedSystem(props);

        Awaitility.waitAtMost(10000, TimeUnit.MILLISECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
            .until(() -> system.getDM().getViewMembers().size() >= 3);

        // three applications plus
        assertThat(system.getDM().getViewMembers().size()).isEqualTo(5);

        system.disconnect();

        vm1.invoke(LocatorDUnitTest::disconnectDistributedSystem);
        vm2.invoke(LocatorDUnitTest::disconnectDistributedSystem);

      } finally {
        vm3.invoke(LocatorDUnitTest::stopLocator);
      }
    } finally {
      vm0.invoke(LocatorDUnitTest::stopLocator);
    }
  }


  private void waitUntilLocatorBecomesCoordinator() {
    Awaitility.waitAtMost(30000, TimeUnit.MILLISECONDS).pollInterval(1000, TimeUnit.MILLISECONDS)
        .until(() -> GMSJoinLeaveTestHelper.getCurrentCoordinator()
            .getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE);
  }

  /**
   * Tests starting multiple locators at the same time and ensuring that the locators end up only
   * have 1 master. GEODE-870
   */
  @Test
  public void testMultipleLocatorsRestartingAtSameTime() {
    disconnectAllFromDS();

    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM vm3 = VM.getVM(3);
    VM vm4 = VM.getVM(4);

    int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    this.port1 = freeTCPPorts[0];
    this.port2 = freeTCPPorts[1];
    int port3 = freeTCPPorts[2];
    DistributedTestUtils.deleteLocatorStateFile(port1, port2, port3);
    final String host0 = NetworkUtils.getServerHostName();
    final String locators =
        host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]," + host0 + "[" + port3 + "]";

    final Properties dsProps = getBasicProperties(locators);
    dsProps.setProperty(LOG_LEVEL, logger.getLevel().name());
    dsProps.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "true");
    dsProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    addDSProps(dsProps);
    startLocator(vm0, dsProps, port1);
    startLocator(vm1, dsProps, port2);
    startLocator(vm2, dsProps, port3);

    try {
      vm3.invoke(() -> {
        getConnectedDistributedSystem(dsProps);
        return true;
      });
      vm4.invoke(() -> {
        getConnectedDistributedSystem(dsProps);
        return true;
      });

      system = getConnectedDistributedSystem(dsProps);

      Awaitility.waitAtMost(10000, TimeUnit.MILLISECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
          .until(() -> system.getDM().getViewMembers().size() == 6);

      // three applications plus
      assertThat(system.getDM().getViewMembers().size()).isEqualTo(6);

      vm0.invoke(LocatorDUnitTest::stopLocator);
      vm1.invoke(LocatorDUnitTest::stopLocator);
      vm2.invoke(LocatorDUnitTest::stopLocator);

      Awaitility.waitAtMost(10000, TimeUnit.MILLISECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
          .until(() -> system.getDM().getMembershipManager().getView().size() <= 3);

      final String newLocators = host0 + "[" + port2 + "]," + host0 + "[" + port3 + "]";
      dsProps.setProperty(LOCATORS, newLocators);

      final InternalDistributedMember currentCoordinator =
          GMSJoinLeaveTestHelper.getCurrentCoordinator();
      DistributedMember vm3ID = vm3.invoke(() -> GMSJoinLeaveTestHelper
          .getInternalDistributedSystem().getDM().getDistributionManagerId());
      assertTrue("View is " + system.getDM().getMembershipManager().getView() + " and vm3's ID is "
          + vm3ID, vm3.invoke(() -> GMSJoinLeaveTestHelper.isViewCreator()));

      startLocator(vm1, dsProps, port2);
      startLocator(vm2, dsProps, port3);

      Awaitility.waitAtMost(30000, TimeUnit.MILLISECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
          .until(() -> !GMSJoinLeaveTestHelper.getCurrentCoordinator().equals(currentCoordinator)
              && system.getDM().getAllHostedLocators().size() == 2);

      vm1.invoke("waitUntilLocatorBecomesCoordinator", this::waitUntilLocatorBecomesCoordinator);
      vm2.invoke("waitUntilLocatorBecomesCoordinator", this::waitUntilLocatorBecomesCoordinator);
      vm3.invoke("waitUntilLocatorBecomesCoordinator", this::waitUntilLocatorBecomesCoordinator);
      vm4.invoke("waitUntilLocatorBecomesCoordinator", this::waitUntilLocatorBecomesCoordinator);

      int netviewId = vm1.invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.getViewId());
      assertThat((int) vm2.invoke("checking ViewID", () -> GMSJoinLeaveTestHelper.getViewId()))
          .isEqualTo(netviewId);
      assertThat((int) vm3.invoke("checking ViewID", () -> GMSJoinLeaveTestHelper.getViewId()))
          .isEqualTo(netviewId);
      assertThat((int) vm4.invoke("checking ViewID", () -> GMSJoinLeaveTestHelper.getViewId()))
          .isEqualTo(netviewId);
      assertThat((boolean) vm4
          .invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.isViewCreator())).isFalse();
      // Given the start up order of servers, this server is the elder server
      assertFalse((boolean) vm3
          .invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.isViewCreator()));
      if (vm1.invoke(() -> GMSJoinLeaveTestHelper.isViewCreator())) {
        assertThat((boolean)
            vm2.invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.isViewCreator()))
            .isFalse();
      } else {
        assertThat((boolean)
            vm2.invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.isViewCreator()))
            .isTrue();
      }

    } finally {
      system.disconnect();
      vm3.invoke(LocatorDUnitTest::disconnectDistributedSystem);
      vm4.invoke(LocatorDUnitTest::disconnectDistributedSystem);
      vm2.invoke(LocatorDUnitTest::stopLocator);
      vm1.invoke(LocatorDUnitTest::stopLocator);
    }
  }


  @Test
  public void testMultipleLocatorsRestartingAtSameTimeWithMissingServers() throws Exception {
    disconnectAllFromDS();
    IgnoredException.addIgnoredException("ForcedDisconnectException");
    IgnoredException.addIgnoredException("Possible loss of quorum");

    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM vm3 = VM.getVM(3);
    VM vm4 = VM.getVM(4);

    int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    this.port1 = freeTCPPorts[0];
    this.port2 = freeTCPPorts[1];
    int port3 = freeTCPPorts[2];
    DistributedTestUtils.deleteLocatorStateFile(port1, port2, port3);
    final String host0 = NetworkUtils.getServerHostName();
    final String locators =
        host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]," + host0 + "[" + port3 + "]";

    final Properties dsProps = getBasicProperties(locators);
    dsProps.setProperty(LOG_LEVEL, logger.getLevel().name());
    dsProps.setProperty(DISABLE_AUTO_RECONNECT, "true");

    addDSProps(dsProps);
    startLocator(vm0, dsProps, port1);
    startLocator(vm1, dsProps, port2);
    startLocator(vm2, dsProps, port3);

    try {
      vm3.invoke(() -> {
        getConnectedDistributedSystem(dsProps);
        return true;
      });
      vm4.invoke(() -> {
        getConnectedDistributedSystem(dsProps);

        Awaitility.waitAtMost(10000, TimeUnit.MILLISECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
            .until(() -> InternalDistributedSystem.getConnectedInstance().getDM().getViewMembers()
                .size() == 5);
        return true;
      });

      vm0.invoke(this::forceDisconnect);
      vm1.invoke(this::forceDisconnect);
      vm2.invoke(this::forceDisconnect);

      SerializableRunnable waitForDisconnect = new SerializableRunnable("waitForDisconnect") {
        public void run() {
          Awaitility.waitAtMost(10000, TimeUnit.MILLISECONDS)
              .pollInterval(200, TimeUnit.MILLISECONDS)
              .until(() -> InternalDistributedSystem.getConnectedInstance() == null);
        }
      };
      vm0.invoke(() -> waitForDisconnect);
      vm1.invoke(() -> waitForDisconnect);
      vm2.invoke(() -> waitForDisconnect);
      disconnectAllFromDS();

      final String newLocators = host0 + "[" + port2 + "]," + host0 + "[" + port3 + "]";
      dsProps.setProperty(LOCATORS, newLocators);

      getBlackboard().initBlackboard();
      AsyncInvocation async1 = vm1.invokeAsync(() -> {
        getBlackboard().signalGate("vm1ready");
        getBlackboard().waitForGate("readyToConnect", 30, TimeUnit.SECONDS);
        System.out.println("vm1 is ready to connect");
        startLocatorBase(dsProps, port2);
      });
      AsyncInvocation async2 = vm2.invokeAsync(() -> {
        getBlackboard().signalGate("vm2ready");
        getBlackboard().waitForGate("readyToConnect", 30, TimeUnit.SECONDS);
        System.out.println("vm2 is ready to connect");
        startLocatorBase(dsProps, port3);
      });
      getBlackboard().waitForGate("vm1ready", 30, TimeUnit.SECONDS);
      getBlackboard().waitForGate("vm2ready", 30, TimeUnit.SECONDS);
      getBlackboard().signalGate("readyToConnect");
      async1.join();
      async2.join();

      vm1.invoke("waitUntilLocatorBecomesCoordinator", this::waitUntilLocatorBecomesCoordinator);
      vm2.invoke("waitUntilLocatorBecomesCoordinator", this::waitUntilLocatorBecomesCoordinator);

      if (vm1.invoke(() -> GMSJoinLeaveTestHelper.isViewCreator())) {
        assertFalse(
            vm2.invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.isViewCreator()));
      } else {
        assertTrue(
            vm2.invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.isViewCreator()));
      }

    } finally {
      vm2.invoke(LocatorDUnitTest::stopLocator);
      vm1.invoke(LocatorDUnitTest::stopLocator);
    }
  }


  /**
   * Tests that a VM can connect to a locator that is hosted in its own VM.
   */
  @Test
  public void testConnectToOwnLocator() throws Exception {
    disconnectAllFromDS();

    port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);

    final String locators = NetworkUtils.getServerHostName() + "[" + port1 + "]";

    Properties props = getBasicProperties(locators);
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    Locator locator = Locator.startLocatorAndDS(port1, new File(""), props);
    system = (InternalDistributedSystem) locator.getDistributedSystem();
    system.disconnect();
    locator.stop();

  }

  /**
   * Tests that a single VM can NOT host multiple locators
   */
  @Test
  public void testHostingMultipleLocators() throws Exception {
    disconnectAllFromDS();

    int[] randomAvailableTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    port1 = randomAvailableTCPPorts[0];
    File logFile1 = new File("");
    DistributedTestUtils.deleteLocatorStateFile(port1);
    Locator locator1 = Locator.startLocator(port1, logFile1);

    try {

      int port2 = randomAvailableTCPPorts[1];
      DistributedTestUtils.deleteLocatorStateFile(port2);

      try {
        Locator.startLocator(port2, new File(""));
        fail("expected second locator start to fail.");
      } catch (IllegalStateException expected) {
      }

      final String host0 = NetworkUtils.getServerHostName();
      final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";

      SerializableRunnable connect = new SerializableRunnable("Connect to " + locators) {
        public void run() {
          Properties props = getBasicProperties(locators);
          props.setProperty(LOG_LEVEL, logger.getLevel().name());
          getConnectedDistributedSystem(props);
        }
      };
      connect.run();

      disconnectDistributedSystem();

    } finally {
      locator1.stop();
    }
  }

  /**
   * Tests starting, stopping, and restarting a locator. See bug 32856.
   * @since GemFire 4.1
   */
  @Test
  public void testRestartLocator() throws Exception {
    disconnectAllFromDS();
    port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    File logFile = new File("");
    File stateFile = new File("locator" + port1 + "state.dat");
    VM vm = VM.getVM(0);
    final Properties
        properties =
        getBasicProperties(Host.getHost(0).getHostName() + "[" + port1 + "]");
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(LOG_LEVEL, DUnitLauncher.logLevel);
    addDSProps(properties);
    if (stateFile.exists()) {
      stateFile.delete();
    }

    logger.info("Starting locator");
    Locator locator = Locator.startLocatorAndDS(port1, logFile, properties);
    try {

      vm.invoke(() -> {
        getConnectedDistributedSystem(properties);
        return true;
      });

      logger.info("Stopping locator");
      locator.stop();

      logger.info("Starting locator");
      locator = Locator.startLocatorAndDS(port1, logFile, properties);

      vm.invoke("disconnect", () -> {
        getConnectedDistributedSystem(properties).disconnect();
        return null;
      });

    } finally {
      locator.stop();
    }

  }

  /**
   * See GEODE-3588 - a locator is restarted twice with a server and ends up in a split-brain
   */
  @Test
  public void testRestartLocatorMultipleTimes() throws Exception {
    disconnectAllFromDS();
    port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    File logFile = new File("");
    File stateFile = new File("locator" + port1 + "state.dat");
    VM vm = VM.getVM(0);
    final Properties
        properties =
        getBasicProperties(Host.getHost(0).getHostName() + "[" + port1 + "]");
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(LOG_LEVEL, "finest");
    addDSProps(properties);
    if (stateFile.exists()) {
      stateFile.delete();
    }

    Locator locator = Locator.startLocatorAndDS(port1, logFile, properties);

    vm.invoke(() -> {
      getConnectedDistributedSystem(properties);
      return null;
    });

    try {
      locator.stop();
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
      assertEquals(2, ((InternalDistributedSystem) locator.getDistributedSystem()).getDM()
          .getViewMembers().size());

      locator.stop();
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
      assertEquals(2, ((InternalDistributedSystem) locator.getDistributedSystem()).getDM()
          .getViewMembers().size());

    } finally {
      vm.invoke("disconnect", () -> {
        getConnectedDistributedSystem(properties).disconnect();
        return null;
      });
      locator.stop();
    }

  }

  @Override
  public final void postSetUp() {
    port1 = -1;
    port2 = -1;
    IgnoredException.addIgnoredException("Removing shunned member");
  }

  @Override
  public final void preTearDown() {
    if (Locator.hasLocator()) {
      Locator.getLocator().stop();
    }
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
    // delete locator state files so they don't accidentally
    // get used by other tests
    if (port1 > 0) {
      DistributedTestUtils.deleteLocatorStateFile(port1);
    }
    if (port2 > 0) {
      DistributedTestUtils.deleteLocatorStateFile(port2);
    }
  }

  @Override
  public final void postTearDown() {
    disconnectAllFromDS();
    if (system != null) {
      system.disconnect();
      system = null;
    }
  }

  // for child classes
  protected void addDSProps(Properties p) {
  }

  protected static InternalDistributedSystem getConnectedDistributedSystem(Properties properties) {
    return (InternalDistributedSystem) DistributedSystem.connect(properties);
  }

  private void startLocatorWithPortAndProperties(final int port, final Properties properties)
      throws IOException {
    assertThat(Locator.startLocatorAndDS(port, new File(""), properties)).isNotNull();
  }

  private String getSingleKeyKeystore() {
    return TestUtil.getResourcePath(getClass(), "/ssl/trusted.keystore");
  }

  private String getMultiKeyKeystore() {
    return TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKey.jks");
  }

  private String getMultiKeyTruststore() {
    return TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKeyTrust.jks");
  }

  private void startVerifyAndStopLocator(VM loc1, VM loc2, int port1, int port2,
                                         Properties properties) throws Exception {
    try {
      getBlackboard().initBlackboard();
      AsyncInvocation<Void> async1 = loc1.invokeAsync("startLocator1", () -> {
        getBlackboard().signalGate("locator1");
        getBlackboard().waitForGate("go", 60, TimeUnit.SECONDS);
        startLocatorWithPortAndProperties(port1, properties);
      });

      AsyncInvocation<Void> async2 = loc2.invokeAsync("startLocator2", () -> {
        getBlackboard().signalGate("locator2");
        getBlackboard().waitForGate("go", 60, TimeUnit.SECONDS);
        startLocatorWithPortAndProperties(port2, properties);
      });

      getBlackboard().waitForGate("locator1", 60, TimeUnit.SECONDS);
      getBlackboard().waitForGate("locator2", 60, TimeUnit.SECONDS);
      getBlackboard().signalGate("go");

      async1.await();
      async2.await();

      // verify that they found each other
      loc2.invoke("expectSystemToContainThisManyMembers",
          () -> expectSystemToContainThisManyMembers(2));
      loc1.invoke("expectSystemToContainThisManyMembers",
          () -> expectSystemToContainThisManyMembers(2));
    } finally {
      loc2.invoke("stop locator", LocatorDUnitTest::stopLocator);
      loc1.invoke("stop locator", LocatorDUnitTest::stopLocator);
    }
  }

  private void assertLeadMember(final DistributedMember member, final DistributedSystem sys) {
    Awaitility.waitAtMost((long) 5000, TimeUnit.MILLISECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> {
          DistributedMember lead = MembershipManagerHelper.getLeadMember(sys);
          if (member != null) {
            return member.equals(lead);
          }
          return (lead == null);
        });
  }

  private int startLocatorGetPort(VM vm, Properties properties, int port) {
    return vm.invoke(() -> startLocatorBase(properties, port).getPort());
  }

  private void startLocator(VM vm, Properties properties, int port) {
    vm.invoke(() -> {
      startLocatorBase(properties, port);
    });
  }

  private Locator startLocatorBase(Properties properties, int port) {
    File lf = new File("");
    try {
      properties.put(NAME, "vm" + VM.getCurrentVMNum());
      return Locator.startLocatorAndDS(port, lf, properties);
    } catch (IOException ios) {
      throw new RuntimeException("Unable to start locator", ios);
    }
  }

  void startLocatorWithSomeBasicProperties(VM vm, int port) {
    Properties locProps = new Properties();
    locProps.setProperty(MCAST_PORT, "0");
    locProps.setProperty(MEMBER_TIMEOUT, "1000");
    locProps.put(ENABLE_CLUSTER_CONFIGURATION, "false");
    addDSProps(locProps);

    startLocator(vm, locProps, port);
  }

  private void startLocatorPreferredCoordinators(VM vm0, int port) {
    try {
      System.setProperty(InternalLocator.LOCATORS_PREFERRED_AS_COORDINATORS, "true");
      Properties locProps1 = new Properties();
      locProps1.put(MCAST_PORT, "0");
      locProps1.put(LOG_LEVEL, logger.getLevel().name());

      addDSProps(locProps1);

      startLocator(vm0, locProps1, port);
    } finally {
      System.getProperties().remove(InternalLocator.LOCATORS_PREFERRED_AS_COORDINATORS);
    }
  }

  private void checkSystemConnectedInVMs(VM vm1, VM vm2) {
    assertThat(vm1.invoke(LocatorDUnitTest::isSystemConnected))
        .describedAs("Distributed system should not have disconnected").isTrue();

    assertThat(vm2.invoke(LocatorDUnitTest::isSystemConnected))
        .describedAs("Distributed system should not have disconnected").isTrue();
  }

  private void checkConnectionAndPrintInfo(VM vm1) {
    vm1.invoke(() -> {
      DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
      if (sys != null && sys.isConnected()) {
        sys.disconnect();
      }
      // connectExceptions occur during disconnect, so we need the
      // ExpectedException hint to be in effect until this point
      LogWriter bLogger = new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);
      bLogger
          .info("<ExpectedException action=remove>java.net.ConnectException</ExpectedException>");
    });
  }

  private void forceDisconnect() {
    DistributedTestUtils.crashDistributedSystem(InternalDistributedSystem.getConnectedInstance());
  }

  Properties getBasicProperties(String locators) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);
    return props;
  }

  private Properties getClusterProperties(String locators, String s) {
    final Properties properties = getBasicProperties(locators);
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, s);
    properties.put(DISABLE_AUTO_RECONNECT, "true");
    properties.put(MEMBER_TIMEOUT, "2000");
    properties.put(LOG_LEVEL, logger.getLevel().name());
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
    return properties;
  }

  private void checkSystemConnected(VM vm2, VM locatorVM) {

    Awaitility.waitAtMost(5, TimeUnit.MINUTES)
        .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
      assertThat(isSystemConnected()).describedAs("Distributed system should not have disconnected")
          .isTrue();

      checkSystemConnectedInVMs(vm2, locatorVM);
      return true;
    });
  }


  // New test hook which blocks before closing channel.
  protected static class TestHook implements MembershipTestHook {

    volatile boolean unboundedWait = true;

    @Override
    public void beforeMembershipFailure(String reason, Throwable cause) {
      System.out.println("Inside TestHook.beforeMembershipFailure with " + cause);
      long giveUp = System.currentTimeMillis() + 30000;
      if (cause instanceof ForcedDisconnectException) {
        while (unboundedWait && System.currentTimeMillis() < giveUp) {
          Wait.pause(1000);
        }
      } else {
        cause.printStackTrace();
      }
    }

    @Override
    public void afterMembershipFailure(String reason, Throwable cause) {
    }

    void reset() {
      unboundedWait = false;
    }

  }

  static class MyMembershipListener implements MembershipListener {

    boolean quorumLostInvoked;
    final List<String> suspectReasons = new ArrayList<>(50);

    public void memberJoined(DistributionManager distributionManager,
                             InternalDistributedMember id) {
    }

    public void memberDeparted(DistributionManager distributionManager,
                               InternalDistributedMember id, boolean crashed) {
    }

    public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
                              InternalDistributedMember whoSuspected, String reason) {
      suspectReasons.add(reason);
    }

    public void quorumLost(DistributionManager distributionManager,
                           Set<InternalDistributedMember> failures,
                           List<InternalDistributedMember> remaining) {
      quorumLostInvoked = true;
      logger.info("quorumLost invoked in test code");
    }
  }
}
