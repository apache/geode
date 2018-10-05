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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.LogWriter;
import org.apache.geode.SystemConnectException;
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
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.tcp.Connection;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
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
 *
 * @since GemFire 4.0
 */
@Category({MembershipTest.class})
public class LocatorDUnitTest extends JUnit4DistributedTestCase {

  static volatile InternalDistributedSystem system = null;

  private static TestHook hook;

  protected int port1;
  private int port2;

  @Override
  public final void postSetUp() throws Exception {
    port1 = -1;
    port2 = -1;
    IgnoredException.addIgnoredException("Removing shunned member");
  }

  @Override
  public final void preTearDown() throws Exception {
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
  public final void postTearDown() throws Exception {
    disconnectAllFromDS();
    if (system != null) {
      system.disconnect();
      system = null;
    }
  }

  // for child classes
  protected void addDSProps(Properties p) {

  }

  //////// Test Methods

  /**
   * This tests that the locator can resume control as coordinator after all locators have been shut
   * down and one is restarted. It's necessary to have a lock service start so elder failover is
   * forced to happen. Prior to fixing how this worked it hung with the restarted locator trying to
   * become elder again because it put its address at the beginning of the new view it sent out.
   */
  @Test
  public void testCollocatedLocatorWithSecurity() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);

    final String locators = NetworkUtils.getServerHostName(host) + "[" + port1 + "]";
    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(START_LOCATOR, locators);
    properties.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    properties.put(SECURITY_PEER_AUTH_INIT, "org.apache.geode.distributed.AuthInitializer.create");
    properties.put(SECURITY_PEER_AUTHENTICATOR,
        "org.apache.geode.distributed.MyAuthenticator.create");
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.put(USE_CLUSTER_CONFIGURATION, "false");
    addDSProps(properties);
    system = (InternalDistributedSystem) DistributedSystem.connect(properties);
    InternalDistributedMember mbr = system.getDistributedMember();
    assertEquals("expected the VM to have NORMAL vmKind", ClusterDistributionManager.NORMAL_DM_TYPE,
        system.getDistributedMember().getVmKind());

    properties.remove(START_LOCATOR);
    properties.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    properties.put(LOCATORS, locators);
    SerializableRunnable startSystem = new SerializableRunnable("start system") {
      public void run() {
        system = (InternalDistributedSystem) DistributedSystem.connect(properties);
      }
    };
    vm1.invoke(startSystem);
    vm2.invoke(startSystem);

    // ensure that I, as a collocated locator owner, can create a cache region
    Cache cache = CacheFactory.create(system);
    Region r = cache.createRegionFactory(RegionShortcut.REPLICATE).create("test-region");
    assertNotNull("expected to create a region", r);

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
        await()
            .until(() -> serviceNamed.isLockGrantor());
        assertTrue(serviceNamed.isLockGrantor());
      });

      properties.put(START_LOCATOR, locators);
      properties.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
      system = (InternalDistributedSystem) DistributedSystem.connect(properties);
      System.out.println("done connecting distributed system.  Membership view is "
          + MembershipManagerHelper.getMembershipManager(system).getView());

      assertEquals("should be the coordinator", system.getDistributedMember(),
          MembershipManagerHelper.getCoordinator(system));
      NetView view = MembershipManagerHelper.getMembershipManager(system).getView();
      org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
          .info("view after becoming coordinator is " + view);
      assertNotSame("should not be the first member in the view (" + view + ")",
          system.getDistributedMember(), view.get(0));

      service = DistributedLockService.create("test service", system);

      // now force a non-elder VM to get a lock. This will hang if the bug is not fixed
      vm2.invoke("get the lock service and lock something", () -> {
        DistributedLockService.getServiceNamed("test service").lock("foo4", 0, 0);
      });

      assertFalse("should not have become lock grantor", service.isLockGrantor());

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
    Host host = Host.getHost(0);
    VM loc1 = host.getVM(1);
    VM loc2 = host.getVM(2);

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    this.port2 = port2; // for cleanup in tearDown2
    DistributedTestUtils.deleteLocatorStateFile(port1);
    DistributedTestUtils.deleteLocatorStateFile(port2);
    final String host0 = NetworkUtils.getServerHostName(host);
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, locators);
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    properties.put(DISABLE_AUTO_RECONNECT, "true");
    properties.put(MEMBER_TIMEOUT, "2000");
    properties.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
    addDSProps(properties);

    startVerifyAndStopLocator(loc1, loc2, port1, port2, properties);
    // GEODE-3052 - split brain on restart from persistent view data
    startVerifyAndStopLocator(loc1, loc2, port1, port2, properties);
    startVerifyAndStopLocator(loc1, loc2, port1, port2, properties);
  }

  private void startLocatorWithPortAndProperties(final int port, final Properties properties)
      throws IOException {
    assertNotNull(Locator.startLocatorAndDS(port, new File(""), properties));
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

  @Test
  public void testStartTwoLocatorsWithSingleKeystoreSSL() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM loc1 = host.getVM(1);
    VM loc2 = host.getVM(2);

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    this.port2 = port2; // for cleanup in tearDown2
    DistributedTestUtils.deleteLocatorStateFile(port1);
    DistributedTestUtils.deleteLocatorStateFile(port2);
    final String host0 = NetworkUtils.getServerHostName(host);
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, locators);
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    properties.put(DISABLE_AUTO_RECONNECT, "true");
    properties.put(MEMBER_TIMEOUT, "2000");
    properties.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
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
    Host host = Host.getHost(0);
    VM loc1 = host.getVM(1);
    VM loc2 = host.getVM(2);

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    this.port2 = port2; // for cleanup in tearDown2
    DistributedTestUtils.deleteLocatorStateFile(port1);
    DistributedTestUtils.deleteLocatorStateFile(port2);
    final String host0 = NetworkUtils.getServerHostName(host);
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, locators);
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    properties.put(DISABLE_AUTO_RECONNECT, "true");
    properties.put(MEMBER_TIMEOUT, "2000");
    properties.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
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
      loc2.invoke("stop locator", () -> stopLocator());
      loc1.invoke("stop locator", () -> stopLocator());
    }
  }

  @Test
  public void testNonSSLLocatorDiesWhenConnectingToSSLLocator() throws Exception {
    IgnoredException.addIgnoredException("Unrecognized SSL message, plaintext connection");
    IgnoredException.addIgnoredException("LocatorCancelException");
    disconnectAllFromDS();

    Host host = Host.getHost(0);
    final String hostname = NetworkUtils.getServerHostName(host);
    VM loc1 = host.getVM(1);
    VM loc2 = host.getVM(2);

    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    properties.put(DISABLE_AUTO_RECONNECT, "true");
    properties.put(MEMBER_TIMEOUT, "2000");
    properties.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
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
      port1 = loc1.invoke(() -> startLocatorWithRandomPort(properties));

      loc1.invoke("expect only one member in system",
          () -> expectSystemToContainThisManyMembers(1));

      properties.remove(SSL_ENABLED_COMPONENTS);
      properties.put(LOCATORS, hostname + "[" + port1 + "]");
      // we set port2 so that the state file gets cleaned up later.
      port2 = loc2.invoke("start Locator2", () -> {
        // Sometimes the LocatorCancelException becomes a SystemConnectException, which then causes
        // an RMIException. This is a normal part of the connect failing.
        int port;
        try {
          port = startLocatorWithRandomPort(properties);
        } catch (SystemConnectException expected_sometimes) {
          return 0;
        }
        return port;
      });

      loc1.invoke("expect only one member in system",
          () -> expectSystemToContainThisManyMembers(1));

    } finally {
      loc1.invoke("stop locator", () -> stopLocator());
      // loc2 should die from inability to connect.
      loc2.invoke(() -> await("locator2 dies")
          .until(() -> Locator.getLocator() == null));
    }
  }

  @Test
  public void testSSLEnabledLocatorDiesWhenConnectingToNonSSLLocator() {
    IgnoredException.addIgnoredException("Remote host closed connection during handshake");
    IgnoredException.addIgnoredException("Unrecognized SSL message, plaintext connection");
    IgnoredException.addIgnoredException("LocatorCancelException");

    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM loc1 = host.getVM(1);
    VM loc2 = host.getVM(2);

    final String hostname = NetworkUtils.getServerHostName(host);
    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    properties.put(DISABLE_AUTO_RECONNECT, "true");
    properties.put(MEMBER_TIMEOUT, "2000");
    properties.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.put(SSL_CIPHERS, "any");
    properties.put(SSL_PROTOCOLS, "any");

    try {
      // we set port1 so that the state file gets cleaned up later.
      port1 = loc1.invoke("start Locator1", () -> startLocatorWithRandomPort(properties));
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
      port2 = loc2.invoke("start Locator2", () -> startLocatorWithRandomPort(properties));
      loc1.invoke("expectSystemToContainThisManyMembers",
          () -> expectSystemToContainThisManyMembers(1));
    } finally {
      // loc2 should die from inability to connect.
      loc2.invoke(() -> await("locator2 dies")
          .until(() -> Locator.getLocator() == null));
      loc1.invoke("expectSystemToContainThisManyMembers",
          () -> expectSystemToContainThisManyMembers(1));
      loc1.invoke("stop locator", () -> stopLocator());
    }
  }

  @Test
  public void testStartTwoLocatorsWithDifferentSSLCertificates() throws Exception {
    IgnoredException.addIgnoredException("Remote host closed connection during handshake");
    IgnoredException
        .addIgnoredException("unable to find valid certification path to requested target");
    IgnoredException.addIgnoredException("Received fatal alert: certificate_unknown");
    IgnoredException.addIgnoredException("LocatorCancelException");
    disconnectAllFromDS();
    IgnoredException.addIgnoredException("Unrecognized SSL message, plaintext connection");
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM loc1 = host.getVM(1);
    VM loc2 = host.getVM(2);

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    this.port2 = port2; // for cleanup in tearDown2
    DistributedTestUtils.deleteLocatorStateFile(port1);
    DistributedTestUtils.deleteLocatorStateFile(port2);
    final String host0 = NetworkUtils.getServerHostName(host);
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, locators);
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    properties.put(DISABLE_AUTO_RECONNECT, "true");
    properties.put(MEMBER_TIMEOUT, "2000");
    properties.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
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
      loc1.invoke("start Locator1", () -> startLocator(port1, properties));
      loc1.invoke("expectSystemToContainThisManyMembers",
          () -> expectSystemToContainThisManyMembers(1));

      properties.put(SSL_KEYSTORE, getMultiKeyKeystore());
      properties.put(SSL_TRUSTSTORE, getMultiKeyTruststore());
      properties.put(SSL_LOCATOR_ALIAS, "locatorkey");

      loc2.invoke("start Locator2", () -> startLocator(port2, properties));
    } finally {
      try {
        loc1.invoke("expectSystemToContainThisManyMembers",
            () -> expectSystemToContainThisManyMembers(1));
      } finally {
        loc1.invoke("stop locator", () -> stopLocator());
      }
    }
  }

  private static void expectSystemToContainThisManyMembers(final int expectedMembers) {
    InternalDistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    assertNotNull(sys);
    assertEquals(expectedMembers, sys.getDM().getViewMembers().size());
  }

  private void startLocator(final int port, final Properties properties) throws IOException {
    startLocatorInternal(port, properties);
  }

  // Don't call this directly for RMI -- the locator isn't RMI serializable.
  private Locator startLocatorInternal(final int port, final Properties properties)
      throws IOException {
    Locator locator;
    locator = Locator.startLocatorAndDS(port, new File(""), properties);
    return locator;
  }

  private int startLocatorWithRandomPort(Properties properties) throws IOException {
    Locator locator = startLocatorInternal(0, properties);
    return locator.getPort();
  }


  /**
   * test lead member selection
   */
  @Test
  public void testLeadMemberSelection() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    final String locators = NetworkUtils.getServerHostName(host) + "[" + port1 + "]";
    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, locators);
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

      assertTrue(MembershipManagerHelper.getLeadMember(sys) == null);

      // connect three vms and then watch the lead member selection as they
      // are disconnected/reconnected
      properties.put("name", "vm1");

      DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));

      // assertTrue(MembershipManagerHelper.getLeadMember(sys) != null);
      assertLeadMember(mem1, sys, 5000);

      properties.put("name", "vm2");
      DistributedMember mem2 = vm2.invoke(() -> getDistributedMember(properties));
      assertLeadMember(mem1, sys, 5000);

      properties.put("name", "vm3");
      DistributedMember mem3 = vm3.invoke(() -> getDistributedMember(properties));
      assertLeadMember(mem1, sys, 5000);

      // after disconnecting the first vm, the second one should become the leader
      vm1.invoke(() -> disconnectDistributedSystem());
      MembershipManagerHelper.getMembershipManager(sys).waitForDeparture(mem1);
      assertLeadMember(mem2, sys, 5000);

      properties.put("name", "vm1");
      mem1 = vm1.invoke(() -> getDistributedMember(properties));
      assertLeadMember(mem2, sys, 5000);

      vm2.invoke(() -> disconnectDistributedSystem());
      MembershipManagerHelper.getMembershipManager(sys).waitForDeparture(mem2);
      assertLeadMember(mem3, sys, 5000);

      vm1.invoke(() -> disconnectDistributedSystem());
      MembershipManagerHelper.getMembershipManager(sys).waitForDeparture(mem1);
      assertLeadMember(mem3, sys, 5000);

      vm3.invoke(() -> disconnectDistributedSystem());
      MembershipManagerHelper.getMembershipManager(sys).waitForDeparture(mem3);
      assertLeadMember(null, sys, 5000);

    } finally {
      locator.stop();
    }
  }

  private void assertLeadMember(final DistributedMember member, final DistributedSystem sys,
      long timeout) {
    await()
        .until(() -> {
          DistributedMember lead = MembershipManagerHelper.getLeadMember(sys);
          if (member != null) {
            return member.equals(lead);
          }
          return (lead == null);
        });
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
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM locvm = host.getVM(3);
    Locator locator = null;

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    DistributedTestUtils.deleteLocatorStateFile(port1, port2);
    final String host0 = NetworkUtils.getServerHostName(host);
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, locators);
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, "true");
    properties.put(DISABLE_AUTO_RECONNECT, "true");
    properties.put(MEMBER_TIMEOUT, "2000");
    properties.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    // properties.put("log-level", "fine");
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");

    addDSProps(properties);
    try {
      final String uname = getUniqueName();
      File logFile = new File("");
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
      final DistributedSystem sys = locator.getDistributedSystem();
      sys.getLogWriter()
          .info("<ExpectedException action=add>java.net.ConnectException</ExpectedException>");
      MembershipManagerHelper.inhibitForcedDisconnectLogging(true);
      locvm.invoke(new SerializableRunnable() {
        public void run() {
          File lf = new File("");
          try {
            Locator.startLocatorAndDS(port2, lf, properties);
          } catch (IOException ios) {
            throw new RuntimeException("Unable to start locator2", ios);
          }
        }
      });

      SerializableRunnable crashLocator = new SerializableRunnable("Crash locator") {
        public void run() {
          Locator loc = Locator.getLocators().iterator().next();
          DistributedSystem msys = loc.getDistributedSystem();
          MembershipManagerHelper.crashDistributedSystem(msys);
          loc.stop();
        }
      };

      assertTrue(MembershipManagerHelper.getLeadMember(sys) == null);

      // properties.put("log-level", getDUnitLogLevel());

      DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));
      vm2.invoke(() -> getDistributedMember(properties));
      assertLeadMember(mem1, sys, 5000);

      assertEquals(sys.getDistributedMember(), MembershipManagerHelper.getCoordinator(sys));

      // crash the second vm and the locator. Should be okay
      DistributedTestUtils.crashDistributedSystem(vm2);
      locvm.invoke(crashLocator);

      assertTrue("Distributed system should not have disconnected",
          vm1.invoke(() -> LocatorDUnitTest.isSystemConnected()));

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
       * This vm is watching vm1, which is watching vm2 which is watching locvm. It will take 3 * (3
       * * member-timeout) milliseconds to detect the full failure and eject the lost members from
       * the view.
       */

      org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
          .info("waiting for my distributed system to disconnect due to partition detection");

      await()
          .until(() -> {
            return !sys.isConnected();
          });

      if (sys.isConnected()) {
        fail(
            "Distributed system did not disconnect as expected - network partition detection is broken");
      }
      // quorumLost should be invoked if we get a ForcedDisconnect in this situation
      assertTrue("expected quorumLost to be invoked", listener.quorumLostInvoked);
      assertTrue("expected suspect processing initiated by TCPConduit",
          listener.suspectReasons.contains(Connection.INITIATING_SUSPECT_PROCESSING));
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
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM locvm = host.getVM(3);
    Locator locator = null;

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    this.port2 = port2;
    DistributedTestUtils.deleteLocatorStateFile(port1, port2);
    final String host0 = NetworkUtils.getServerHostName(host);
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, locators);
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, "true");
    properties.put(DISABLE_AUTO_RECONNECT, "true");
    properties.put(MEMBER_TIMEOUT, "2000");
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");

    addDSProps(properties);

    try {
      File logFile = new File("");
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
      DistributedSystem sys = locator.getDistributedSystem();
      locvm.invoke(new SerializableRunnable() {
        public void run() {
          File lf = new File("");
          try {
            Locator.startLocatorAndDS(port2, lf, properties);
            MembershipManagerHelper.inhibitForcedDisconnectLogging(true);
          } catch (IOException ios) {
            throw new RuntimeException("Unable to start locator2", ios);
          }
        }
      });

      SerializableRunnable crashSystem = new SerializableRunnable("Crash system") {
        public void run() {
          DistributedSystem msys = InternalDistributedSystem.getAnyInstance();
          msys.getLogWriter()
              .info("<ExpectedException action=add>service failure</ExpectedException>");
          msys.getLogWriter().info(
              "<ExpectedException action=add>org.apache.geode.ConnectException</ExpectedException>");
          msys.getLogWriter().info(
              "<ExpectedException action=add>org.apache.geode.ForcedDisconnectException</ExpectedException>");
          MembershipManagerHelper.crashDistributedSystem(msys);
        }
      };

      assertTrue(MembershipManagerHelper.getLeadMember(sys) == null);

      DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));
      DistributedMember mem2 = vm2.invoke(() -> getDistributedMember(properties));

      assertEquals(mem1, MembershipManagerHelper.getLeadMember(sys));

      assertEquals(sys.getDistributedMember(), MembershipManagerHelper.getCoordinator(sys));

      MembershipManagerHelper.inhibitForcedDisconnectLogging(true);

      // crash the lead vm. Should be okay
      vm1.invoke(crashSystem);

      await().until(() -> isSystemConnected());

      assertTrue("Distributed system should not have disconnected", isSystemConnected());

      assertTrue("Distributed system should not have disconnected",
          vm2.invoke(() -> LocatorDUnitTest.isSystemConnected()));

      assertTrue("Distributed system should not have disconnected",
          locvm.invoke(() -> LocatorDUnitTest.isSystemConnected()));

      // stop the locator normally. This should also be okay
      locator.stop();

      if (!Locator.getLocators().isEmpty()) {
        // log this for debugging purposes before throwing assertion error
        org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
            .warning("found locator " + Locator.getLocators().iterator().next());
      }
      assertTrue("locator is not stopped", Locator.getLocators().isEmpty());

      assertTrue("Distributed system should not have disconnected",
          vm2.invoke(() -> LocatorDUnitTest.isSystemConnected()));

      assertTrue("Distributed system should not have disconnected",
          locvm.invoke(() -> LocatorDUnitTest.isSystemConnected()));

      // the remaining non-locator member should now be the lead member
      assertEquals(
          "This test sometimes fails.  If the log contains "
              + "'failed to collect all ACKs' it is a false failure.",
          mem2, vm2.invoke(() -> LocatorDUnitTest.getLeadMember()));

      SerializableRunnable disconnect = new SerializableRunnable("Disconnect from " + locators) {
        public void run() {
          DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
          if (sys != null && sys.isConnected()) {
            sys.disconnect();
          }
        }
      };

      // disconnect the first vm and demonstrate that the third vm and the
      // locator notice the failure and exit
      vm2.invoke(() -> disconnectDistributedSystem());
      locvm.invoke(() -> stopLocator());
    } finally {
      MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
      if (locator != null) {
        locator.stop();
      }
      try {
        locvm.invoke(() -> stopLocator());
      } catch (Exception e) {
        org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
            .severe("failed to stop locator in vm 3", e);
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
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM locvm = host.getVM(3);
    Locator locator = null;

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    DistributedTestUtils.deleteLocatorStateFile(port1, port2);
    final String host0 = NetworkUtils.getServerHostName(host);
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";

    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, locators);
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, "true");
    properties.put(DISABLE_AUTO_RECONNECT, "true");
    properties.put(MEMBER_TIMEOUT, "2000");
    properties.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");

    addDSProps(properties);

    try {
      final String uname = getUniqueName();
      File logFile = new File("");
      locator = Locator.startLocatorAndDS(port1, logFile, properties);
      DistributedSystem sys = locator.getDistributedSystem();
      locvm.invoke(() -> {
        File lf = new File("");
        try {
          Locator.startLocatorAndDS(port2, lf, properties);
        } catch (IOException ios) {
          throw new RuntimeException("Unable to start locator2", ios);
        }
      });

      SerializableRunnable crashSystem = new SerializableRunnable("Crash system") {
        public void run() {
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
        }
      };

      assertNull(MembershipManagerHelper.getLeadMember(sys));

      final DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));
      final DistributedMember mem2 = vm2.invoke(() -> getDistributedMember(properties));

      assertEquals(mem1, MembershipManagerHelper.getLeadMember(sys));

      assertEquals(sys.getDistributedMember(), MembershipManagerHelper.getCoordinator(sys));

      // crash the lead vm. Should be okay. it should hang in test hook thats
      // why call is asynchronous.
      // vm1.invokeAsync(crashSystem);

      assertTrue("Distributed system should not have disconnected", isSystemConnected());

      assertTrue("Distributed system should not have disconnected",
          vm2.invoke(() -> LocatorDUnitTest.isSystemConnected()));

      assertTrue("Distributed system should not have disconnected",
          locvm.invoke(() -> LocatorDUnitTest.isSystemConnected()));

      vm2.invokeAsync(crashSystem);

      Wait.pause(1000); // 4 x the member-timeout

      // request member removal for first peer from second peer.
      vm2.invoke(new SerializableRunnable("Request Member Removal") {

        @Override
        public void run() {
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
            assertTrue("This should have been ForceDisconnectException but found " + cause,
                cause instanceof ForcedDisconnectException);
          } finally {
            hook.reset();
          }
        }
      });

    } finally {
      if (locator != null) {
        locator.stop();
      }
      locvm.invoke(() -> stopLocator());
      assertTrue("locator is not stopped", Locator.getLocators().isEmpty());
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
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM locvm = host.getVM(3);
    Locator locator = null;

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    this.port1 = port1;
    final int port2 = ports[1];
    DistributedTestUtils.deleteLocatorStateFile(port1, port2);
    final String host0 = NetworkUtils.getServerHostName(host);
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";
    final Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, locators);
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, "true");
    properties.put(DISABLE_AUTO_RECONNECT, "true");
    properties.put(MEMBER_TIMEOUT, "2000");
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");

    addDSProps(properties);
    try {
      locvm.invoke(() -> {
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

      assertTrue(MembershipManagerHelper.getLeadMember(sys) == null);

      DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));

      vm1.invoke(() -> MembershipManagerHelper.inhibitForcedDisconnectLogging(true));

      DistributedMember mem2 = vm2.invoke(() -> getDistributedMember(properties));

      DistributedMember loc1Mbr = locvm.invoke(() -> this.getLocatorDistributedMember());

      assertLeadMember(mem1, sys, 5000);

      assertEquals(loc1Mbr, MembershipManagerHelper.getCoordinator(sys));

      // crash the lead locator. Should be okay
      locvm.invoke("crash locator", () -> {
        Locator loc = Locator.getLocators().iterator().next();
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

      await()
          .until(() -> sys.isConnected());

      assertTrue("Distributed system should not have disconnected", sys.isConnected());

      assertTrue("Distributed system should not have disconnected",
          vm1.invoke(() -> LocatorDUnitTest.isSystemConnected()));

      assertTrue("Distributed system should not have disconnected",
          vm2.invoke(() -> LocatorDUnitTest.isSystemConnected()));

      // disconnect the first vm and demonstrate that the non-lead vm and the
      // locator notice the failure and continue to run
      vm1.invoke(() -> disconnectDistributedSystem());

      await()
          .until(() -> vm2.invoke(() -> LocatorDUnitTest.isSystemConnected()));

      assertTrue("Distributed system should not have disconnected",
          vm2.invoke(() -> LocatorDUnitTest.isSystemConnected()));

      await().untilAsserted(() -> assertEquals(sys.getDistributedMember(),
          MembershipManagerHelper.getCoordinator(sys)));
      await().untilAsserted(() -> assertEquals(mem2, MembershipManagerHelper.getLeadMember(sys)));

    } finally {
      vm2.invoke(() -> disconnectDistributedSystem());

      if (locator != null) {
        locator.stop();
      }
      locvm.invoke(() -> stopLocator());
    }
  }

  /**
   * Tests that attempting to connect to a distributed system in which no locator is defined throws
   * an exception.
   */
  @Test
  public void testNoLocator() {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    String locators = NetworkUtils.getServerHostName(host) + "[" + port + "]";
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);

    addDSProps(props);
    final String expected = "java.net.ConnectException";
    final String addExpected = "<ExpectedException action=add>" + expected + "</ExpectedException>";
    final String removeExpected =
        "<ExpectedException action=remove>" + expected + "</ExpectedException>";

    LogWriter bgexecLogger = new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);
    bgexecLogger.info(addExpected);

    boolean exceptionOccurred = true;
    try {
      DistributedSystem.connect(props);
      exceptionOccurred = false;

    } catch (DistributionException ex) {
      // I guess it can throw this too...

    } catch (GemFireConfigException ex) {
      String s = ex.getMessage();
      assertTrue(s.indexOf("Locator does not exist") >= 0);

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
  public void testOneLocator() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    final String locators = NetworkUtils.getServerHostName(host) + "[" + port + "]";

    vm0.invoke("Start locator " + locators, () -> startLocator(port));
    try {

      SerializableRunnable connect = new SerializableRunnable("Connect to " + locators) {
        public void run() {
          Properties props = new Properties();
          props.setProperty(MCAST_PORT, "0");
          props.setProperty(LOCATORS, locators);
          props.setProperty(MEMBER_TIMEOUT, "1000");
          addDSProps(props);
          DistributedSystem.connect(props);
        }
      };
      vm1.invoke(connect);
      vm2.invoke(connect);

      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, locators);
      props.setProperty(MEMBER_TIMEOUT, "1000");

      addDSProps(props);
      system = (InternalDistributedSystem) DistributedSystem.connect(props);

      final DistributedMember coord = MembershipManagerHelper.getCoordinator(system);
      org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
          .info("coordinator before termination of locator is " + coord);

      vm0.invoke(() -> stopLocator());

      // now ensure that one of the remaining members became the coordinator

      await()
          .until(() -> !coord.equals(MembershipManagerHelper.getCoordinator(system)));

      DistributedMember newCoord = MembershipManagerHelper.getCoordinator(system);
      LogWriterUtils.getLogWriter().info("coordinator after shutdown of locator was " + newCoord);
      if (coord.equals(newCoord)) {
        fail("another member should have become coordinator after the locator was stopped");
      }

      system.disconnect();

      vm1.invoke(() -> disconnectDistributedSystem());
      vm2.invoke(() -> disconnectDistributedSystem());

    } finally {
      vm0.invoke(() -> stopLocator());
    }
  }

  protected void startLocator(int port) {
    File logFile = new File("");
    try {
      Properties locProps = new Properties();
      locProps.setProperty(MCAST_PORT, "0");
      locProps.setProperty(MEMBER_TIMEOUT, "1000");
      locProps.put(ENABLE_CLUSTER_CONFIGURATION, "false");

      addDSProps(locProps);
      Locator.startLocatorAndDS(port, logFile, locProps);
    } catch (IOException ex) {
      throw new RuntimeException("While starting locator on port " + port, ex);
    }
  }

  /**
   * Tests starting one locator in a remote VM and having multiple members of the distributed system
   * join it. This ensures that members start up okay, and that handling of a stopped locator is
   * correct. It then restarts the locator to demonstrate that it can connect to and function as the
   * group coordinator
   */
  @Test
  public void testLocatorBecomesCoordinator() throws Exception {
    disconnectAllFromDS();
    final String expected = "java.net.ConnectException";
    final String addExpected = "<ExpectedException action=add>" + expected + "</ExpectedException>";
    final String removeExpected =
        "<ExpectedException action=remove>" + expected + "</ExpectedException>";

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    final String locators = NetworkUtils.getServerHostName(host) + "[" + port + "]";

    vm0.invoke(getUniqueName() + 1, () -> startSBLocator(port));
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
      org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
          .info("coordinator before termination of locator is " + coord);

      vm0.invoke(() -> stopLocator());

      // now ensure that one of the remaining members became the coordinator
      await()
          .until(() -> !coord.equals(MembershipManagerHelper.getCoordinator(system)));

      DistributedMember newCoord = MembershipManagerHelper.getCoordinator(system);
      LogWriterUtils.getLogWriter().info("coordinator after shutdown of locator was " + newCoord);
      if (newCoord == null || coord.equals(newCoord)) {
        fail("another member should have become coordinator after the locator was stopped: "
            + newCoord);
      }

      // restart the locator to demonstrate reconnection & make disconnects faster
      // it should also regain the role of coordinator, so we check to make sure
      // that the coordinator has changed
      vm0.invoke(getUniqueName() + "2", () -> startSBLocator(port));

      final DistributedMember tempCoord = newCoord;

      await()
          .until(() -> !tempCoord.equals(MembershipManagerHelper.getCoordinator(system)));

      system.disconnect();
      LogWriter bgexecLogger = new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);
      bgexecLogger.info(removeExpected);

      vm1.invoke(() -> {
        DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
        if (sys != null && sys.isConnected()) {
          sys.disconnect();
        }
        // connectExceptions occur during disconnect, so we need the
        // expectedexception hint to be in effect until this point
        LogWriter bLogger = new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);
        bLogger.info(removeExpected);
      });
      vm2.invoke(() -> {
        DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
        if (sys != null && sys.isConnected()) {
          sys.disconnect();
        }
        // connectExceptions occur during disconnect, so we need the
        // expectedexception hint to be in effect until this point
        LogWriter bLogger = new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);
        bLogger.info(removeExpected);
      });
      vm0.invoke(() -> stopLocator());
    } finally {
      vm0.invoke(() -> stopLocator());
    }

  }

  private static boolean isSystemConnected() {
    DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    return sys != null && sys.isConnected();
  }

  /**
   * Tests starting multiple locators in multiple VMs.
   */
  @Test
  public void testMultipleLocators() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = freeTCPPorts[0];
    this.port1 = port1;
    final int port2 = freeTCPPorts[1];
    this.port2 = port2;
    DistributedTestUtils.deleteLocatorStateFile(port1, port2);
    final String host0 = NetworkUtils.getServerHostName(host);
    final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";

    final Properties dsProps = new Properties();
    dsProps.setProperty(LOCATORS, locators);
    dsProps.setProperty(MCAST_PORT, "0");
    dsProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    addDSProps(dsProps);

    vm0.invoke("start Locator1", () -> startLocator(port1, dsProps));
    try {

      vm3.invoke("Start locator on " + port2, () -> startLocator(port2, dsProps));
      try {

        SerializableRunnable connect = new SerializableRunnable("Connect to " + locators) {
          public void run() {
            Properties props = new Properties();
            props.setProperty(MCAST_PORT, "0");
            props.setProperty(LOCATORS, locators);
            addDSProps(props);
            DistributedSystem.connect(props);
          }
        };
        vm1.invoke(connect);
        vm2.invoke(connect);

        Properties props = new Properties();
        props.setProperty(MCAST_PORT, "0");
        props.setProperty(LOCATORS, locators);

        addDSProps(props);
        system = (InternalDistributedSystem) DistributedSystem.connect(props);

        await()
            .until(() -> system.getDM().getViewMembers().size() >= 3);

        // three applications plus
        assertEquals(5, system.getDM().getViewMembers().size());

        system.disconnect();

        vm1.invoke(() -> disconnectDistributedSystem());
        vm2.invoke(() -> disconnectDistributedSystem());

      } finally {
        vm3.invoke(() -> stopLocator());
      }
    } finally {
      vm0.invoke(() -> stopLocator());
    }
  }

  private static void disconnectDistributedSystem() {
    DistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    if (sys != null && sys.isConnected()) {
      sys.disconnect();
    }
    MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
  }

  /**
   * Tests starting multiple locators at the same time and ensuring that the locators end up only
   * have 1 master. GEODE-870
   */
  @Test
  public void testMultipleLocatorsRestartingAtSameTime() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    VM vm4 = host.getVM(4);

    int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    this.port1 = freeTCPPorts[0];
    this.port2 = freeTCPPorts[1];
    int port3 = freeTCPPorts[2];
    DistributedTestUtils.deleteLocatorStateFile(port1, port2, port3);
    final String host0 = NetworkUtils.getServerHostName(host);
    final String locators =
        host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]," + host0 + "[" + port3 + "]";

    final Properties dsProps = new Properties();
    dsProps.setProperty(LOCATORS, locators);
    dsProps.setProperty(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    dsProps.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "true");
    dsProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    dsProps.setProperty(MCAST_PORT, "0");

    addDSProps(dsProps);
    vm0.invoke(() -> startLocatorAsync(new Object[] {port1, dsProps}));
    vm1.invoke(() -> startLocatorAsync(new Object[] {port2, dsProps}));
    vm2.invoke(() -> startLocatorAsync(new Object[] {port3, dsProps}));

    try {
      vm3.invoke(() -> {
        DistributedSystem.connect(dsProps);
        return true;
      });
      vm4.invoke(() -> {
        DistributedSystem.connect(dsProps);
        return true;
      });

      system = (InternalDistributedSystem) DistributedSystem.connect(dsProps);

      await()
          .until(() -> system.getDM().getViewMembers().size() == 6);

      // three applications plus
      assertEquals(6, system.getDM().getViewMembers().size());

      vm0.invoke(() -> stopLocator());
      vm1.invoke(() -> stopLocator());
      vm2.invoke(() -> stopLocator());

      await()
          .until(() -> system.getDM().getMembershipManager().getView().size() <= 3);

      final String newLocators = host0 + "[" + port2 + "]," + host0 + "[" + port3 + "]";
      dsProps.setProperty(LOCATORS, newLocators);

      final InternalDistributedMember currentCoordinator =
          GMSJoinLeaveTestHelper.getCurrentCoordinator();
      DistributedMember vm3ID = vm3.invoke(() -> GMSJoinLeaveTestHelper
          .getInternalDistributedSystem().getDM().getDistributionManagerId());
      assertTrue("View is " + system.getDM().getMembershipManager().getView() + " and vm3's ID is "
          + vm3ID, vm3.invoke(() -> GMSJoinLeaveTestHelper.isViewCreator()));

      vm1.invoke(() -> startLocatorAsync(new Object[] {port2, dsProps}));
      vm2.invoke(() -> startLocatorAsync(new Object[] {port3, dsProps}));

      await()
          .until(() -> !GMSJoinLeaveTestHelper.getCurrentCoordinator().equals(currentCoordinator)
              && system.getDM().getAllHostedLocators().size() == 2);

      vm1.invoke("waitUntilLocatorBecomesCoordinator", () -> waitUntilLocatorBecomesCoordinator());
      vm2.invoke("waitUntilLocatorBecomesCoordinator", () -> waitUntilLocatorBecomesCoordinator());
      vm3.invoke("waitUntilLocatorBecomesCoordinator", () -> waitUntilLocatorBecomesCoordinator());
      vm4.invoke("waitUntilLocatorBecomesCoordinator", () -> waitUntilLocatorBecomesCoordinator());

      int netviewId = vm1.invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.getViewId());
      assertEquals(netviewId,
          (int) vm2.invoke("checking ViewID", () -> GMSJoinLeaveTestHelper.getViewId()));
      assertEquals(netviewId,
          (int) vm3.invoke("checking ViewID", () -> GMSJoinLeaveTestHelper.getViewId()));
      assertEquals(netviewId,
          (int) vm4.invoke("checking ViewID", () -> GMSJoinLeaveTestHelper.getViewId()));
      assertFalse(vm4.invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.isViewCreator()));
      // Given the start up order of servers, this server is the elder server
      assertFalse(vm3.invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.isViewCreator()));
      if (vm1.invoke(() -> GMSJoinLeaveTestHelper.isViewCreator())) {
        assertFalse(
            vm2.invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.isViewCreator()));
      } else {
        assertTrue(
            vm2.invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.isViewCreator()));
      }

    } finally {
      system.disconnect();
      vm3.invoke(() -> disconnectDistributedSystem());
      vm4.invoke(() -> disconnectDistributedSystem());
      vm2.invoke(() -> stopLocator());
      vm1.invoke(() -> stopLocator());
    }
  }

  @Test
  public void testMultipleLocatorsRestartingAtSameTimeWithMissingServers() throws Exception {
    disconnectAllFromDS();
    IgnoredException.addIgnoredException("ForcedDisconnectException");
    IgnoredException.addIgnoredException("Possible loss of quorum");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    VM vm4 = host.getVM(4);

    int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    this.port1 = freeTCPPorts[0];
    this.port2 = freeTCPPorts[1];
    int port3 = freeTCPPorts[2];
    DistributedTestUtils.deleteLocatorStateFile(port1, port2, port3);
    final String host0 = NetworkUtils.getServerHostName(host);
    final String locators =
        host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]," + host0 + "[" + port3 + "]";

    final Properties dsProps = new Properties();
    dsProps.setProperty(LOCATORS, locators);
    dsProps.setProperty(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    dsProps.setProperty(DISABLE_AUTO_RECONNECT, "true");
    dsProps.setProperty(MCAST_PORT, "0");

    addDSProps(dsProps);
    vm0.invoke(() -> startLocatorAsync(new Object[] {port1, dsProps}));
    vm1.invoke(() -> startLocatorAsync(new Object[] {port2, dsProps}));
    vm2.invoke(() -> startLocatorAsync(new Object[] {port3, dsProps}));

    try {
      vm3.invoke(() -> {
        DistributedSystem.connect(dsProps);
        return true;
      });
      vm4.invoke(() -> {
        DistributedSystem.connect(dsProps);

        await()
            .until(() -> InternalDistributedSystem.getConnectedInstance().getDM().getViewMembers()
                .size() == 5);
        return true;
      });

      vm0.invoke(() -> forceDisconnect());
      vm1.invoke(() -> forceDisconnect());
      vm2.invoke(() -> forceDisconnect());

      SerializableRunnable waitForDisconnect = new SerializableRunnable("waitForDisconnect") {
        public void run() {
          await()
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
        startLocatorAsync(new Object[] {port2, dsProps});
      });
      AsyncInvocation async2 = vm2.invokeAsync(() -> {
        getBlackboard().signalGate("vm2ready");
        getBlackboard().waitForGate("readyToConnect", 30, TimeUnit.SECONDS);
        System.out.println("vm2 is ready to connect");
        startLocatorAsync(new Object[] {port3, dsProps});
      });
      getBlackboard().waitForGate("vm1ready", 30, TimeUnit.SECONDS);
      getBlackboard().waitForGate("vm2ready", 30, TimeUnit.SECONDS);
      getBlackboard().signalGate("readyToConnect");
      async1.join();
      async2.join();

      vm1.invoke("waitUntilLocatorBecomesCoordinator", () -> waitUntilLocatorBecomesCoordinator());
      vm2.invoke("waitUntilLocatorBecomesCoordinator", () -> waitUntilLocatorBecomesCoordinator());

      if (vm1.invoke(() -> GMSJoinLeaveTestHelper.isViewCreator())) {
        assertFalse(
            vm2.invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.isViewCreator()));
      } else {
        assertTrue(
            vm2.invoke("Checking ViewCreator", () -> GMSJoinLeaveTestHelper.isViewCreator()));
      }

    } finally {
      vm2.invoke(() -> stopLocator());
      vm1.invoke(() -> stopLocator());
    }
  }

  private void waitUntilLocatorBecomesCoordinator() {
    await()
        .until(() -> GMSJoinLeaveTestHelper.getCurrentCoordinator()
            .getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE);
  }

  private void startLocatorAsync(Object[] args) {
    File logFile = new File("");
    Properties properties = (Properties) args[1];
    properties.put(NAME, "vm" + VM.getCurrentVMNum());
    try {
      Locator.startLocatorAndDS((int) args[0], logFile, properties);
    } catch (IOException ex) {
      throw new RuntimeException("While starting process on port " + args[0], ex);
    }
  }

  /**
   * Tests that a VM can connect to a locator that is hosted in its own VM.
   */
  @Test
  public void testConnectToOwnLocator() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);

    port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    File logFile = new File("");
    Locator locator = Locator.startLocator(port1, logFile);
    try {

      final String locators = NetworkUtils.getServerHostName(host) + "[" + port1 + "]";

      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, locators);
      props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
      system = (InternalDistributedSystem) DistributedSystem.connect(props);
      system.disconnect();
    } finally {
      locator.stop();
    }
  }

  /**
   * Tests that a single VM can NOT host multiple locators
   */
  @Test
  public void testHostingMultipleLocators() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    int[] randomAvailableTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    port1 = randomAvailableTCPPorts[0];
    File logFile1 = new File("");
    DistributedTestUtils.deleteLocatorStateFile(port1);
    Locator locator1 = Locator.startLocator(port1, logFile1);

    try {

      int port2 = randomAvailableTCPPorts[1];
      File logFile2 = new File("");

      DistributedTestUtils.deleteLocatorStateFile(port2);

      try {
        Locator locator2 = Locator.startLocator(port2, logFile2);
        fail("expected second locator start to fail.");
      } catch (IllegalStateException expected) {
      }

      final String host0 = NetworkUtils.getServerHostName(host);
      final String locators = host0 + "[" + port1 + "]," + host0 + "[" + port2 + "]";

      SerializableRunnable connect = new SerializableRunnable("Connect to " + locators) {
        public void run() {
          Properties props = new Properties();
          props.setProperty(MCAST_PORT, "0");
          props.setProperty(LOCATORS, locators);
          props.setProperty(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
          DistributedSystem.connect(props);
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
   *
   * @since GemFire 4.1
   */
  @Test
  public void testRestartLocator() throws Exception {
    disconnectAllFromDS();
    port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    File logFile = new File("");
    File stateFile = new File("locator" + port1 + "state.dat");
    VM vm0 = Host.getHost(0).getVM(0);
    final Properties p = new Properties();
    p.setProperty(LOCATORS, Host.getHost(0).getHostName() + "[" + port1 + "]");
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    p.setProperty(LOG_LEVEL, DUnitLauncher.logLevel);
    addDSProps(p);
    if (stateFile.exists()) {
      stateFile.delete();
    }

    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("Starting locator");
    Locator locator = Locator.startLocatorAndDS(port1, logFile, p);
    try {

      vm0.invoke(() -> {
        DistributedSystem.connect(p);
        return null;
      });

      LogWriterUtils.getLogWriter().info("Stopping locator");
      locator.stop();

      LogWriterUtils.getLogWriter().info("Starting locator");
      locator = Locator.startLocatorAndDS(port1, logFile, p);

      vm0.invoke("disconnect", () -> {
        DistributedSystem.connect(p).disconnect();
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
    VM vm0 = Host.getHost(0).getVM(0);
    final Properties p = new Properties();
    p.setProperty(LOCATORS, Host.getHost(0).getHostName() + "[" + port1 + "]");
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    p.setProperty(LOG_LEVEL, "finest");
    addDSProps(p);
    if (stateFile.exists()) {
      stateFile.delete();
    }

    Locator locator = Locator.startLocatorAndDS(port1, logFile, p);

    vm0.invoke(() -> {
      DistributedSystem.connect(p);
      return null;
    });

    try {
      locator.stop();
      locator = Locator.startLocatorAndDS(port1, logFile, p);
      assertEquals(2, ((InternalDistributedSystem) locator.getDistributedSystem()).getDM()
          .getViewMembers().size());

      locator.stop();
      locator = Locator.startLocatorAndDS(port1, logFile, p);
      assertEquals(2, ((InternalDistributedSystem) locator.getDistributedSystem()).getDM()
          .getViewMembers().size());

    } finally {
      vm0.invoke("disconnect", () -> {
        DistributedSystem.connect(p).disconnect();
        return null;
      });
      locator.stop();
    }

  }

  /**
   * return the distributed member id for the ds on this vm
   */
  public static DistributedMember getDistributedMember(Properties props) {
    props.put("name", "vm_" + VM.getCurrentVMNum());
    DistributedSystem sys = DistributedSystem.connect(props);
    sys.getLogWriter().info("<ExpectedException action=add>service failure</ExpectedException>");
    sys.getLogWriter().info(
        "<ExpectedException action=add>org.apache.geode.ConnectException</ExpectedException>");
    sys.getLogWriter().info(
        "<ExpectedException action=add>org.apache.geode.ForcedDisconnectException</ExpectedException>");
    return DistributedSystem.connect(props).getDistributedMember();
  }

  /**
   * find a running locator and return its distributed member id
   */
  private static DistributedMember getLocatorDistributedMember() {
    return (Locator.getLocators().iterator().next()).getDistributedSystem().getDistributedMember();
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
      assertFalse(Locator.hasLocator());
    }
  }

  private void forceDisconnect() {
    DistributedTestUtils.crashDistributedSystem(InternalDistributedSystem.getConnectedInstance());
  }

  private void startSBLocator(final int port) {
    File logFile = new File("");
    try {
      System.setProperty(InternalLocator.LOCATORS_PREFERRED_AS_COORDINATORS, "true");
      Properties locProps = new Properties();
      locProps.put(MCAST_PORT, "0");
      locProps.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
      addDSProps(locProps);
      Locator.startLocatorAndDS(port, logFile, locProps);
    } catch (IOException ex) {
      throw new RuntimeException("While starting locator on port " + port, ex);
    } finally {
      System.getProperties().remove(InternalLocator.LOCATORS_PREFERRED_AS_COORDINATORS);
    }
  }

  // New test hook which blocks before closing channel.
  private static class TestHook implements MembershipTestHook {

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
    public void afterMembershipFailure(String reason, Throwable cause) {}

    public void reset() {
      unboundedWait = false;
    }

  }

  private static class MyMembershipListener implements MembershipListener {

    boolean quorumLostInvoked;
    List<String> suspectReasons = new ArrayList<>(50);

    public void memberJoined(DistributionManager distributionManager,
        InternalDistributedMember id) {}

    public void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {}

    public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
        InternalDistributedMember whoSuspected, String reason) {
      suspectReasons.add(reason);
    }

    public void quorumLost(DistributionManager distributionManager,
        Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
      quorumLostInvoked = true;
      org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
          .info("quorumLost invoked in test code");
    }
  }
}
