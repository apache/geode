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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATOR_WAIT_TIME;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
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
import static org.apache.geode.internal.security.SecurableCommunicationChannel.LOCATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectFromDS;
import static org.apache.geode.test.dunit.DistributedTestUtils.deleteLocatorStateFile;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.SystemConnectException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.Distribution;
import org.apache.geode.distributed.internal.DistributionException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityAckedMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.MembershipTestHook;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper;
import org.apache.geode.distributed.internal.membership.api.MembershipView;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.tcp.Connection;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Tests the ability of the {@link Locator} API to start and stop locators running in remote VMs.
 *
 * @since GemFire 4.0
 */
@Category(MembershipTest.class)
@SuppressWarnings("serial")
public class LocatorDUnitTest implements Serializable {
  private static final Logger logger = LogService.getLogger();

  protected static volatile InternalDistributedSystem system;
  private static volatile DUnitBlackboard blackboard;
  private static volatile TestHook hook;

  protected int port1;
  private int port2;
  private int port3;
  private int port4;

  protected String hostName;

  protected VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;
  private VM vm4;

  @Rule
  public DistributedRule distributedRule = new DistributedRule(6);

  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @Before
  public void setUp() {
    addIgnoredException("Removing shunned member");

    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);
    vm4 = getVM(4);

    hostName = NetworkUtils.getServerHostName();

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(4);
    port1 = ports[0];
    port2 = ports[1];
    port3 = ports[2];
    port4 = ports[3];
    Invoke.invokeInEveryVM(() -> deleteLocatorStateFile(port1, port2, port3, port4));
    addIgnoredException(MemberDisconnectedException.class); // ignore this suspect string
    addIgnoredException(MembershipConfigurationException.class); // ignore this suspect string
  }

  @After
  public void tearDown() {
    for (VM vm : toArray(getController(), vm0, vm1, vm2, vm3, vm4)) {
      vm.invoke(() -> {
        stopLocator();
        disconnectFromDS();
        system = null;
        blackboard = null;
        hook = null;
      });
    }

    // delete locator state files so they don't accidentally get used by other tests
    Invoke.invokeInEveryVM(() -> {
      for (int port : new int[] {port1, port2, port3, port4}) {
        if (port > 0) {
          deleteLocatorStateFile(port);
        }
      }
    });
  }

  private static DUnitBlackboard getBlackboard() {
    if (blackboard == null) {
      blackboard = new DUnitBlackboard();
    }
    return blackboard;
  }

  private static void expectSystemToContainThisManyMembers(final int expectedMembers) {
    assertThat(system).isNotNull();
    await()
        .untilAsserted(() -> assertEquals(expectedMembers, system.getDM().getViewMembers().size()));
  }

  private static boolean isSystemConnected() {
    return system != null && system.isConnected();
  }

  private static void disconnectDistributedSystem() {
    if (system != null && system.isConnected()) {
      system.disconnect();
    }
    MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
  }

  /**
   * return the distributed member id for the ds on this vm
   */
  private static DistributedMember getDistributedMember(Properties props) {
    props.setProperty("name", "vm_" + VM.getCurrentVMNum());
    DistributedSystem sys = getConnectedDistributedSystem(props);
    addIgnoredException("service failure");
    addIgnoredException(ForcedDisconnectException.class);
    return sys.getDistributedMember();
  }

  /**
   * find a running locator and return its distributed member id
   */
  private static DistributedMember getLocatorDistributedMember() {
    return Locator.getLocator().getDistributedSystem().getDistributedMember();
  }

  /**
   * find the lead member and return its id
   */
  private static DistributedMember getLeadMember() {
    return MembershipManagerHelper.getLeadMember(system);
  }

  protected static void stopLocator() {
    MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
    Locator loc = Locator.getLocator();
    if (loc != null) {
      loc.stop();
      assertThat(Locator.hasLocator()).isFalse();
    }
  }

  @Test
  @Ignore("GEODE=7760 - test sometimes hangs due to product issue")
  public void testCrashLocatorMultipleTimes() throws Exception {
    port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    File logFile = new File("");
    File stateFile = new File("locator" + port1 + "state.dat");
    VM vm = VM.getVM(0);
    final Properties properties =
        getBasicProperties(Host.getHost(0).getHostName() + "[" + port1 + "]");
    int memberTimeoutMS = 3000;
    properties.put(MEMBER_TIMEOUT, "" + memberTimeoutMS);
    properties.put(MAX_WAIT_TIME_RECONNECT, "" + (3 * memberTimeoutMS));
    // since we're restarting location services let's be a little forgiving about that service
    // starting up so that stress-tests can pass
    properties.put(LOCATOR_WAIT_TIME, "" + 3);
    addDSProps(properties);
    if (stateFile.exists()) {
      assertThat(stateFile.delete()).isTrue();
    }

    IgnoredException
        .addIgnoredException("Possible loss of quorum due to the loss of 1 cache processes");

    Locator locator = Locator.startLocatorAndDS(port1, logFile, properties);
    system = (InternalDistributedSystem) locator.getDistributedSystem();

    vm.invoke(() -> {
      getConnectedDistributedSystem(properties);
      return null;
    });

    try {
      for (int i = 0; i < 4; i++) {
        forceDisconnect();
        system.waitUntilReconnected(GeodeAwaitility.getTimeout().toMillis(), MILLISECONDS);
        assertThat(system.getReconnectedSystem()).isNotNull();
        system = (InternalDistributedSystem) system.getReconnectedSystem();
      }
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

  /**
   * This tests that the locator can resume control as coordinator after all locators have been shut
   * down and one is restarted. It's necessary to have a lock service start so elder failover is
   * forced to happen. Prior to fixing how this worked it hung with the restarted locator trying to
   * become elder again because it put its address at the beginning of the new view it sent out.
   */
  @Test
  public void testCollocatedLocatorWithSecurity() {
    String locators = hostName + "[" + port1 + "]";

    Properties properties = new Properties();
    properties.setProperty(SECURITY_PEER_AUTH_INIT,
        "org.apache.geode.distributed.AuthInitializer.create");
    properties.setProperty(SECURITY_PEER_AUTHENTICATOR,
        "org.apache.geode.distributed.MyAuthenticator.create");
    properties.setProperty(START_LOCATOR, locators);
    addDSProps(properties);

    system = getConnectedDistributedSystem(properties);
    assertThat(system.getDistributedMember().getVmKind())
        .describedAs("expected the VM to have NORMAL vmKind")
        .isEqualTo(ClusterDistributionManager.NORMAL_DM_TYPE);

    properties.remove(START_LOCATOR);
    properties.setProperty(LOCATORS, locators);

    SerializableRunnable startSystem = new SerializableRunnable("start system") {
      @Override
      public void run() {
        system = getConnectedDistributedSystem(properties);
      }
    };
    vm1.invoke(startSystem);
    vm2.invoke(startSystem);

    // ensure that I, as a collocated locator owner, can create a cache region
    Cache cache = CacheFactory.create(system);
    Region r = cache.createRegionFactory(RegionShortcut.REPLICATE).create("test-region");
    assertThat(r).describedAs("expected to create a region").isNotNull();

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

    vm1.invoke("ensure grantor failover", () -> {
      DistributedLockService serviceNamed = DistributedLockService.getServiceNamed("test service");
      serviceNamed.lock("foo3", 0, 0);
      await().until(serviceNamed::isLockGrantor);
      assertThat(serviceNamed.isLockGrantor()).isTrue();
    });

    properties.setProperty(START_LOCATOR, locators);

    system = getConnectedDistributedSystem(properties);
    System.out.println("done connecting distributed system.  Membership view is "
        + MembershipManagerHelper.getDistribution(system).getView());

    assertThat(MembershipManagerHelper.getCoordinator(system))
        .describedAs("should be the coordinator").isEqualTo(system.getDistributedMember());
    MembershipView view = MembershipManagerHelper.getDistribution(system).getView();
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
  }

  /**
   * Bug 30341 concerns race conditions in JGroups that allow two locators to start up in a
   * split-brain configuration. To work around this we have always told customers that they need to
   * stagger the starting of locators. This test configures two locators to start up simultaneously
   * and shows that they find each other and form a single system.
   */
  @Test
  public void testStartTwoLocators() throws Exception {
    String locators = hostName + "[" + port1 + "]," + hostName + "[" + port2 + "]";

    Properties properties = getClusterProperties(locators, "false");
    addDSProps(properties);

    startVerifyAndStopLocator(vm1, vm2, port1, port2, properties);
    startVerifyAndStopLocator(vm1, vm2, port1, port2, properties);
    startVerifyAndStopLocator(vm1, vm2, port1, port2, properties);
  }

  @Test
  public void testStartTwoLocatorsWithSingleKeystoreSSL() throws Exception {
    String locators = hostName + "[" + port1 + "]," + hostName + "[" + port2 + "]";

    Properties properties = getClusterProperties(locators, "false");
    properties.setProperty(SSL_CIPHERS, "any");
    properties.setProperty(SSL_ENABLED_COMPONENTS, LOCATOR.getConstant());
    properties.setProperty(SSL_KEYSTORE, getSingleKeyKeystore());
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_PROTOCOLS, "TLSv1,TLSv1.1,TLSv1.2");
    properties.setProperty(SSL_TRUSTSTORE, getSingleKeyKeystore());
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");

    startVerifyAndStopLocator(vm1, vm2, port1, port2, properties);
  }

  @Test
  public void testStartTwoLocatorsWithMultiKeystoreSSL() throws Exception {
    String locators = hostName + "[" + port1 + "]," + hostName + "[" + port2 + "]";

    Properties properties = getClusterProperties(locators, "false");
    properties.setProperty(SSL_CIPHERS, "any");
    properties.setProperty(SSL_ENABLED_COMPONENTS, LOCATOR.getConstant());
    properties.setProperty(SSL_KEYSTORE, getMultiKeyKeystore());
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_TRUSTSTORE, getMultiKeyTruststore());
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.setProperty(SSL_LOCATOR_ALIAS, "locatorkey");
    properties.setProperty(SSL_PROTOCOLS, "any");

    startVerifyAndStopLocator(vm1, vm2, port1, port2, properties);
  }

  @Test
  public void testNonSSLLocatorDiesWhenConnectingToSSLLocator() {
    addIgnoredException("Unrecognized SSL message, plaintext connection");
    addIgnoredException(IllegalStateException.class);

    Properties properties = new Properties();
    properties.setProperty(DISABLE_AUTO_RECONNECT, "true");
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    properties.setProperty(MEMBER_TIMEOUT, "2000");
    properties.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(SSL_CIPHERS, "any");
    properties.setProperty(SSL_ENABLED_COMPONENTS, LOCATOR.getConstant());
    properties.setProperty(SSL_KEYSTORE, getSingleKeyKeystore());
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_PROTOCOLS, "any");
    properties.setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(SSL_TRUSTSTORE, getSingleKeyKeystore());
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");

    // we set port1 so that the state file gets cleaned up later.
    port1 = startLocatorGetPort(vm1, properties, 0);

    vm1.invoke("expect only one member in system",
        () -> expectSystemToContainThisManyMembers(1));

    properties.setProperty(LOCATORS, hostName + "[" + port1 + "]");
    properties.remove(SSL_ENABLED_COMPONENTS);

    // we set port2 so that the state file gets cleaned up later.
    vm2.invoke(() -> {
      assertThatThrownBy(() -> startLocatorBase(properties, 0))
          .isInstanceOfAny(IllegalThreadStateException.class, SystemConnectException.class);

      assertThat(Locator.getLocator()).isNull();
    });

    vm1.invoke("expect only one member in system",
        () -> expectSystemToContainThisManyMembers(1));

    vm1.invoke("stop locator", LocatorDUnitTest::stopLocator);
  }

  @Test
  public void testSSLEnabledLocatorDiesWhenConnectingToNonSSLLocator() {
    addIgnoredException("Remote host closed connection during handshake");
    addIgnoredException("Unrecognized SSL message, plaintext connection");
    IgnoredException.addIgnoredException(IllegalStateException.class);


    Properties properties = getClusterProperties("", "false");
    properties.remove(LOCATORS);
    properties.setProperty(SSL_CIPHERS, "any");
    properties.setProperty(SSL_PROTOCOLS, "any");

    // we set port1 so that the state file gets cleaned up later.
    port1 = startLocatorGetPort(vm1, properties, 0);
    vm1.invoke("expectSystemToContainThisManyMembers",
        () -> expectSystemToContainThisManyMembers(1));

    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(SSL_KEYSTORE, getSingleKeyKeystore());
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(SSL_TRUSTSTORE, getSingleKeyKeystore());
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.setProperty(SSL_ENABLED_COMPONENTS, LOCATOR.getConstant());
    properties.setProperty(USE_CLUSTER_CONFIGURATION, "false");

    String locators = hostName + "[" + port1 + "]";

    properties.setProperty(LOCATORS, locators);

    // we set port2 so that the state file gets cleaned up later.
    assertThatThrownBy(() -> startLocatorGetPort(vm2, properties, 0))
        .isInstanceOfAny(IllegalStateException.class, RMIException.class);
    assertThat(Locator.getLocator()).isNull();

    vm1.invoke("expectSystemToContainThisManyMembers",
        () -> expectSystemToContainThisManyMembers(1));

    vm1.invoke("stop locator", LocatorDUnitTest::stopLocator);
  }

  @Test
  public void testStartTwoLocatorsWithDifferentSSLCertificates() {
    addIgnoredException("Remote host closed connection during handshake");
    addIgnoredException("unable to find valid certification path to requested target");
    addIgnoredException("Received fatal alert: certificate_unknown");
    addIgnoredException("Unrecognized SSL message, plaintext connection");

    String locators = hostName + "[" + port1 + "]," + hostName + "[" + port2 + "]";

    Properties properties = getClusterProperties(locators, "false");
    properties.setProperty(SSL_CIPHERS, "any");
    properties.setProperty(SSL_ENABLED_COMPONENTS, LOCATOR.getConstant());
    properties.setProperty(SSL_KEYSTORE, getSingleKeyKeystore());
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_PROTOCOLS, "any");
    properties.setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(SSL_TRUSTSTORE, getSingleKeyKeystore());
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");

    startLocator(vm1, properties, port1);

    vm1.invoke("expectSystemToContainThisManyMembers",
        () -> expectSystemToContainThisManyMembers(1));

    properties.setProperty(SSL_KEYSTORE, getMultiKeyKeystore());
    properties.setProperty(SSL_LOCATOR_ALIAS, "locatorkey");
    properties.setProperty(SSL_TRUSTSTORE, getMultiKeyTruststore());

    assertThatThrownBy(() -> startLocator(vm2, properties, port2))
        .isInstanceOfAny(IllegalStateException.class, RMIException.class);
    assertThat(Locator.getLocator()).isNull();

    vm1.invoke("expectSystemToContainThisManyMembers",
        () -> expectSystemToContainThisManyMembers(1));
  }

  /**
   * test lead member selection
   */
  @Test
  public void testLeadMemberSelection() throws Exception {
    String locators = hostName + "[" + port1 + "]";

    Properties properties = getBasicProperties(locators);
    properties.setProperty(DISABLE_AUTO_RECONNECT, "true");
    properties.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "true");
    addDSProps(properties);

    Locator locator = Locator.startLocatorAndDS(port1, null, properties);

    system = (InternalDistributedSystem) locator.getDistributedSystem();

    assertThat(MembershipManagerHelper.getLeadMember(system)).isNull();

    // connect three vms and then watch the lead member selection as they
    // are disconnected/reconnected
    properties.setProperty("name", "vm1");

    DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));

    waitForMemberToBecomeLeadMemberOfDistributedSystem(mem1, system);

    properties.setProperty("name", "vm2");
    DistributedMember mem2 = vm2.invoke(() -> getDistributedMember(properties));
    waitForMemberToBecomeLeadMemberOfDistributedSystem(mem1, system);

    properties.setProperty("name", "vm3");
    DistributedMember mem3 = vm3.invoke(() -> getDistributedMember(properties));
    waitForMemberToBecomeLeadMemberOfDistributedSystem(mem1, system);

    // after disconnecting the first vm, the second one should become the leader
    vm1.invoke(LocatorDUnitTest::disconnectDistributedSystem);
    MembershipManagerHelper.getDistribution(system).waitForDeparture(
        (InternalDistributedMember) mem1);
    waitForMemberToBecomeLeadMemberOfDistributedSystem(mem2, system);

    properties.setProperty("name", "vm1");
    mem1 = vm1.invoke(() -> getDistributedMember(properties));
    waitForMemberToBecomeLeadMemberOfDistributedSystem(mem2, system);

    vm2.invoke(LocatorDUnitTest::disconnectDistributedSystem);
    MembershipManagerHelper.getDistribution(system).waitForDeparture(
        (InternalDistributedMember) mem2);
    waitForMemberToBecomeLeadMemberOfDistributedSystem(mem3, system);

    vm1.invoke(LocatorDUnitTest::disconnectDistributedSystem);
    MembershipManagerHelper.getDistribution(system).waitForDeparture(
        (InternalDistributedMember) mem1);
    waitForMemberToBecomeLeadMemberOfDistributedSystem(mem3, system);

    vm3.invoke(LocatorDUnitTest::disconnectDistributedSystem);
    MembershipManagerHelper.getDistribution(system).waitForDeparture(
        (InternalDistributedMember) mem3);
    waitForMemberToBecomeLeadMemberOfDistributedSystem(null, system);
  }

  /**
   * test lead member and coordinator failure with network partition detection enabled. It would be
   * nice for this test to have more than two "server" vms, to demonstrate that they all exit when
   * the leader and potential- coordinator both disappear in the loss-correlation-window, but there
   * are only four vms available for DUnit testing.
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
    addIgnoredException("Possible loss of quorum due");

    String locators = hostName + "[" + port1 + "]," + hostName + "[" + port2 + "]";

    Properties properties = getClusterProperties(locators, "true");
    addDSProps(properties);

    Locator locator = Locator.startLocatorAndDS(port1, null, properties);
    system = (InternalDistributedSystem) locator.getDistributedSystem();

    addIgnoredException(ConnectException.class);
    MembershipManagerHelper.inhibitForcedDisconnectLogging(true);
    startLocator(vm3, properties, port2);

    assertThat(MembershipManagerHelper.getLeadMember(system)).isNull();

    DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));
    vm2.invoke(() -> getDistributedMember(properties));
    waitForMemberToBecomeLeadMemberOfDistributedSystem(mem1, system);

    assertThat(system.getDistributedMember())
        .isEqualTo(MembershipManagerHelper.getCoordinator(system));

    // crash the second vm and the locator. Should be okay
    DistributedTestUtils.crashDistributedSystem(vm2);
    vm3.invoke(() -> {
      Locator loc = Locator.getLocator();
      MembershipManagerHelper.crashDistributedSystem(loc.getDistributedSystem());
      loc.stop();
    });

    assertThat(vm1.invoke(LocatorDUnitTest::isSystemConnected))
        .describedAs("Distributed system should not have disconnected").isTrue();

    // ensure quorumLost is properly invoked
    DistributionManager dm = system.getDistributionManager();
    MyMembershipListener listener = new MyMembershipListener();
    dm.addMembershipListener(listener);
    // ensure there is an unordered reader thread for the member
    new HighPriorityAckedMessage().send(Collections.singleton(mem1), false);

    // disconnect the first vm and demonstrate that the third vm and the
    // locator notice the failure and exit
    DistributedTestUtils.crashDistributedSystem(vm1);

    /*
     * This vm is watching vm1, which is watching vm2 which is watching locatorVM. It will take 3
     * (3 member-timeout) milliseconds to detect the full failure and eject the lost members from
     * the view.
     */

    logger.info("waiting for my distributed system to disconnect due to partition detection");

    await().until(() -> !system.isConnected());

    if (system.isConnected()) {
      fail(
          "Distributed system did not disconnect as expected - network partition detection is broken");
    }
    // quorumLost should be invoked if we get a ForcedDisconnect in this situation
    assertThat(listener.quorumLostInvoked).describedAs("expected quorumLost to be invoked")
        .isTrue();
    assertThat(listener.suspectReasons.contains(Connection.INITIATING_SUSPECT_PROCESSING))
        .describedAs("expected suspect processing initiated by TCPConduit").isTrue();
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
    String locators = hostName + "[" + port1 + "]," + hostName + "[" + port2 + "]";

    Properties properties = getClusterProperties(locators, "true");
    addDSProps(properties);

    Locator locator = Locator.startLocatorAndDS(port1, null, properties);
    system = (InternalDistributedSystem) locator.getDistributedSystem();

    vm3.invoke(() -> {
      Locator loc = Locator.startLocatorAndDS(port2, null, properties);
      system = (InternalDistributedSystem) loc.getDistributedSystem();
      MembershipManagerHelper.inhibitForcedDisconnectLogging(true);
    });

    assertThat(MembershipManagerHelper.getLeadMember(system)).isNull();

    DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));
    DistributedMember mem2 = vm2.invoke(() -> getDistributedMember(properties));

    assertThat(mem1).isEqualTo(MembershipManagerHelper.getLeadMember(system));

    assertThat(system.getDistributedMember())
        .isEqualTo(MembershipManagerHelper.getCoordinator(system));

    MembershipManagerHelper.inhibitForcedDisconnectLogging(true);

    // crash the lead vm. Should be okay
    vm1.invoke(() -> {
      addIgnoredException("service failure");
      addIgnoredException(ForcedDisconnectException.class);
      MembershipManagerHelper.crashDistributedSystem(system);
    });

    waitUntilTheSystemIsConnected(vm2, vm3);
    // stop the locator normally. This should also be okay
    locator.stop();

    await()
        .until(() -> {
          assertThat(Locator.getLocator()).describedAs("locator is not stopped").isNull();
          return true;
        });

    checkSystemConnectedInVMs(vm2, vm3);

    // the remaining non-locator member should now be the lead member
    assertEquals(
        "This test sometimes fails.  If the log contains "
            + "'failed to collect all ACKs' it is a false failure.",
        mem2, vm2.invoke(LocatorDUnitTest::getLeadMember));

    // disconnect the first vm and demonstrate that the third vm and the
    // locator notice the failure and exit
    vm2.invoke(LocatorDUnitTest::disconnectDistributedSystem);
    vm3.invoke(LocatorDUnitTest::stopLocator);
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
  @Test
  public void testForceDisconnectAndPeerShutdownCause() throws Exception {
    String locators = hostName + "[" + port1 + "]," + hostName + "[" + port2 + "]";

    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, locators);
    properties.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "true");
    properties.setProperty(DISABLE_AUTO_RECONNECT, "true");
    properties.setProperty(MEMBER_TIMEOUT, "2000");
    addDSProps(properties);

    Locator locator = Locator.startLocatorAndDS(port1, null, properties);
    system = (InternalDistributedSystem) locator.getDistributedSystem();

    vm3.invoke(() -> {
      Locator loc = Locator.startLocatorAndDS(port2, null, properties);
      system = (InternalDistributedSystem) loc.getDistributedSystem();
    });

    SerializableRunnable crashSystem = new SerializableRunnable("Crash system") {
      @Override
      public void run() {
        addIgnoredException("service failure");
        addIgnoredException("Possible loss of quorum");
        addIgnoredException(ForcedDisconnectException.class);

        hook = new TestHook();
        MembershipManagerHelper.addTestHook(system, hook);
        try {
          MembershipManagerHelper.crashDistributedSystem(system);
        } finally {
          hook.reset();
        }
      }
    };

    assertNull(MembershipManagerHelper.getLeadMember(system));

    DistributedMember mem1 = vm1.invoke(() -> getDistributedMember(properties));
    DistributedMember mem2 = vm2.invoke(() -> getDistributedMember(properties));

    assertEquals(mem1, MembershipManagerHelper.getLeadMember(system));

    assertEquals(system.getDistributedMember(), MembershipManagerHelper.getCoordinator(system));

    assertTrue("Distributed system should not have disconnected", isSystemConnected());

    assertTrue("Distributed system should not have disconnected",
        vm2.invoke(() -> isSystemConnected()));

    assertTrue("Distributed system should not have disconnected",
        vm3.invoke(() -> isSystemConnected()));

    vm2.invokeAsync(crashSystem);

    // request member removal for first peer from second peer.
    vm2.invoke(new SerializableRunnable("Request Member Removal") {

      @Override
      public void run() {
        Distribution mmgr = MembershipManagerHelper.getDistribution(system);

        // check for shutdown cause in Distribution. Following call should
        // throw DistributedSystemDisconnectedException which should have cause as
        // ForceDisconnectException.
        await().until(() -> !mmgr.getMembership().isConnected());
        await().untilAsserted(() -> {
          Throwable cause = mmgr.getShutdownCause();
          assertThat(cause).isInstanceOf(ForcedDisconnectException.class);
        });
        try (IgnoredException i = addIgnoredException("Membership: requesting removal of")) {
          mmgr.requestMemberRemoval((InternalDistributedMember) mem1, "test reasons");
          fail("It should have thrown exception in requestMemberRemoval");
        } catch (DistributedSystemDisconnectedException e) {
          // expected
          assertThat(e.getCause()).isInstanceOf(ForcedDisconnectException.class);
        } finally {
          hook.reset();
        }
      }
    });
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
    VM memberThatWillBeShutdownVM = vm1;
    VM memberVM = vm2;
    VM locatorThatWillBeShutdownVM = vm3;

    String locators = hostName + "[" + port1 + "]," + hostName + "[" + port2 + "]";

    Properties properties = getClusterProperties(locators, "true");
    addDSProps(properties);

    locatorThatWillBeShutdownVM.invoke(() -> {
      Locator localLocator = Locator.startLocatorAndDS(port2, null, properties);
      system = (InternalDistributedSystem) localLocator.getDistributedSystem();
      assertThat(localLocator.getDistributedSystem().isConnected()).isTrue();
    });

    // Test runner will be locator 2
    Locator locator = Locator.startLocatorAndDS(port1, null, properties);
    system = (InternalDistributedSystem) locator.getDistributedSystem();
    assertThat(locator.getDistributedSystem().isConnected()).isTrue();
    DistributedSystem testRunnerLocatorDS = locator.getDistributedSystem();
    addIgnoredException(ForcedDisconnectException.class);
    assertThat(MembershipManagerHelper.getLeadMember(testRunnerLocatorDS))
        .describedAs("There was a lead member when there should not be.").isNull();

    DistributedMember distributedMemberThatWillBeShutdown =
        memberThatWillBeShutdownVM.invoke(() -> getDistributedMember(properties));
    memberThatWillBeShutdownVM
        .invoke(() -> MembershipManagerHelper.inhibitForcedDisconnectLogging(true));

    DistributedMember distributedMember = memberVM.invoke(() -> getDistributedMember(properties));

    DistributedMember locatorMemberToBeShutdown =
        locatorThatWillBeShutdownVM.invoke(LocatorDUnitTest::getLocatorDistributedMember);

    waitForMemberToBecomeLeadMemberOfDistributedSystem(distributedMemberThatWillBeShutdown,
        testRunnerLocatorDS);
    DistributedMember oldLeader = MembershipManagerHelper.getLeadMember(testRunnerLocatorDS);

    assertThat(locatorMemberToBeShutdown)
        .isEqualTo(MembershipManagerHelper.getCoordinator(testRunnerLocatorDS));
    DistributedMember oldCoordinator =
        MembershipManagerHelper.getCoordinator(testRunnerLocatorDS);

    // crash the lead locator. Should be okay
    locatorThatWillBeShutdownVM.invoke("crash locator", () -> {
      Locator loc = Locator.getLocator();
      DistributedSystem distributedSystem = loc.getDistributedSystem();
      addIgnoredException("service failure");
      addIgnoredException(ForcedDisconnectException.class);
      MembershipManagerHelper.crashDistributedSystem(distributedSystem);
      loc.stop();
    });

    await().until(testRunnerLocatorDS::isConnected);

    waitUntilTheSystemIsConnected(memberThatWillBeShutdownVM, memberVM);

    // disconnect the first vm and demonstrate that the non-lead vm and the
    // locator notice the failure and continue to run
    memberThatWillBeShutdownVM.invoke(LocatorDUnitTest::disconnectDistributedSystem);
    await().until(
        () -> memberThatWillBeShutdownVM.invoke(() -> !isSystemConnected()));
    await().until(() -> memberVM.invoke(LocatorDUnitTest::isSystemConnected));

    assertThat(memberVM.invoke(LocatorDUnitTest::isSystemConnected))
        .describedAs("Distributed system should not have disconnected").isTrue();

    await("waiting for the old coordinator to drop out").until(
        () -> MembershipManagerHelper.getCoordinator(testRunnerLocatorDS) != oldCoordinator);

    await().untilAsserted(() -> {
      DistributedMember survivingDistributedMember = testRunnerLocatorDS.getDistributedMember();
      DistributedMember coordinator = MembershipManagerHelper.getCoordinator(testRunnerLocatorDS);
      assertThat(survivingDistributedMember).isEqualTo(coordinator);
    });

    await("Waiting for the old leader to drop out")
        .pollInterval(1, SECONDS).until(() -> {
          DistributedMember leader = MembershipManagerHelper.getLeadMember(testRunnerLocatorDS);
          return leader != oldLeader;
        });

    await().untilAsserted(() -> {
      assertThat(distributedMember)
          .isEqualTo(MembershipManagerHelper.getLeadMember(testRunnerLocatorDS));
    });
  }

  /**
   * Tests that attempting to connect to a distributed system in which no locator is defined throws
   * an exception.
   */
  @Test
  public void testNoLocator() {
    String locators = hostName + "[" + port2 + "]";

    Properties props = getBasicProperties(locators);
    addDSProps(props);

    addIgnoredException(ConnectException.class);

    try {
      getConnectedDistributedSystem(props);
      fail("Should have thrown a GemFireConfigException");

    } catch (DistributionException ex) {
      // I guess it can throw this too...

    } catch (GemFireConfigException ex) {
      String s = ex.getMessage();
      assertThat(s.contains("Locator does not exist")).isTrue();
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
    String locators = hostName + "[" + port2 + "]";

    startLocatorWithSomeBasicProperties(vm0, port2);

    SerializableRunnable connect = new SerializableRunnable("Connect to " + locators) {
      @Override
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

    DistributedMember coord = MembershipManagerHelper.getCoordinator(system);
    logger.info("coordinator before termination of locator is " + coord);

    vm0.invoke(LocatorDUnitTest::stopLocator);

    // now ensure that one of the remaining members became the coordinator

    await().until(() -> !coord.equals(MembershipManagerHelper.getCoordinator(system)));

    DistributedMember newCoord = MembershipManagerHelper.getCoordinator(system);
    logger.info("coordinator after shutdown of locator was " + newCoord);
    if (coord.equals(newCoord)) {
      fail("another member should have become coordinator after the locator was stopped");
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
    addIgnoredException(ConnectException.class);

    String locators = hostName + "[" + port2 + "]";

    startLocatorPreferredCoordinators(vm0, port2);

    Properties props = new Properties();
    props.setProperty(LOCATORS, locators);
    props.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "true");
    addDSProps(props);

    vm1.invoke(() -> {
      getSystem(props);
    });
    vm2.invoke(() -> {
      getSystem(props);
    });

    system = getSystem(props);

    DistributedMember coord = MembershipManagerHelper.getCoordinator(system);
    logger.info("coordinator before termination of locator is " + coord);

    vm0.invoke(LocatorDUnitTest::stopLocator);

    // now ensure that one of the remaining members became the coordinator
    await().until(() -> !coord.equals(MembershipManagerHelper.getCoordinator(system)));

    DistributedMember newCoord = MembershipManagerHelper.getCoordinator(system);
    logger.info("coordinator after shutdown of locator was " + newCoord);
    if (newCoord == null || coord.equals(newCoord)) {
      fail("another member should have become coordinator after the locator was stopped: "
          + newCoord);
    }

    // restart the locator to demonstrate reconnection & make disconnects faster
    // it should also regain the role of coordinator, so we check to make sure
    // that the coordinator has changed
    startLocatorPreferredCoordinators(vm0, port2);

    DistributedMember tempCoord = newCoord;

    await().until(() -> !tempCoord.equals(MembershipManagerHelper.getCoordinator(system)));

    system.disconnect();

    checkConnectionAndPrintInfo(vm1);
    checkConnectionAndPrintInfo(vm2);
  }

  @Test
  public void testConcurrentLocatorStartup() throws Exception {
    List<AvailablePort.Keeper> portKeepers =
        AvailablePortHelper.getRandomAvailableTCPPortKeepers(4);
    StringBuilder sb = new StringBuilder(100);
    for (int i = 0; i < portKeepers.size(); i++) {
      AvailablePort.Keeper keeper = portKeepers.get(i);
      sb.append("localhost[").append(keeper.getPort()).append("]");
      if (i < portKeepers.size() - 1) {
        sb.append(',');
      }
    }
    String locators = sb.toString();

    Properties dsProps = getClusterProperties(locators, "false");

    List<AsyncInvocation<Object>> asyncInvocations = new ArrayList<>(portKeepers.size());

    for (int i = 0; i < portKeepers.size(); i++) {
      AvailablePort.Keeper keeper = portKeepers.get(i);
      int port = keeper.getPort();
      keeper.release();
      AsyncInvocation<Object> startLocator = getVM(i).invokeAsync("start locator " + i, () -> {
        DUnitBlackboard blackboard = getBlackboard();
        blackboard.signalGate("" + port);
        blackboard.waitForGate("startLocators", 5, MINUTES);
        startLocatorBase(dsProps, port);
        assertTrue(isSystemConnected());
        System.out.println("Locator startup completed");
      });
      asyncInvocations.add(startLocator);
      getBlackboard().waitForGate("" + port, 5, MINUTES);
    }

    getBlackboard().signalGate("startLocators");
    int expectedCount = asyncInvocations.size() - 1;
    for (int i = 0; i < asyncInvocations.size(); i++) {
      asyncInvocations.get(i).await();
    }
    for (int i = 0; i < asyncInvocations.size(); i++) {
      assertTrue(getVM(i).invoke("assert all in same cluster", () -> CacheFactory
          .getAnyInstance().getDistributedSystem().getAllOtherMembers().size() == expectedCount));
    }
  }

  /**
   * Tests starting two locators and two servers in different JVMs
   */
  @Test
  public void testTwoLocatorsTwoServers() {
    String locators = hostName + "[" + port1 + "]," + hostName + "[" + port2 + "]";

    Properties dsProps = getBasicProperties(locators);
    addDSProps(dsProps);

    startLocator(vm0, dsProps, port1);

    startLocator(vm3, dsProps, port2);

    SerializableRunnable connect = new SerializableRunnable("Connect to " + locators) {
      @Override
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

    await().until(() -> system.getDM().getViewMembers().size() >= 3);

    // three applications plus
    assertThat(system.getDM().getViewMembers().size()).isEqualTo(5);
  }

  private void waitUntilLocatorBecomesCoordinator() {
    await().until(() -> system != null && system.isConnected() &&
        getCreator()
            .getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE);
  }

  private InternalDistributedMember getMember() {
    return MembershipManagerHelper.getDistribution(system).getLocalMember();
  }

  private InternalDistributedMember getCreator() {
    return (InternalDistributedMember) MembershipManagerHelper.getCreator(system);
  }


  private MembershipView getView() {
    return system.getDistributionManager().getDistribution().getView();
  }

  private InternalDistributedSystem getSystem(Properties properties) {
    return (InternalDistributedSystem) DistributedSystem.connect(properties);
  }

  /**
   * Tests starting multiple locators at the same time and ensuring that the locators end up only
   * have 1 master. GEODE-870
   */
  @Test
  public void testMultipleLocatorsRestartingAtSameTime() {
    String locators =
        hostName + "[" + port1 + "]," + hostName + "[" + port2 + "]," + hostName + "[" + port3
            + "]";

    Properties dsProps = getBasicProperties(locators);
    dsProps.setProperty(LOG_LEVEL, logger.getLevel().name());
    dsProps.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "true");
    addDSProps(dsProps);

    startLocator(vm0, dsProps, port1);
    startLocator(vm1, dsProps, port2);
    startLocator(vm2, dsProps, port3);

    vm3.invoke(() -> {
      getConnectedDistributedSystem(dsProps);
    });
    vm4.invoke(() -> {
      getConnectedDistributedSystem(dsProps);
    });

    system = getConnectedDistributedSystem(dsProps);

    await().until(() -> system.getDM().getViewMembers().size() == 6);

    // three applications plus
    assertThat(system.getDM().getViewMembers().size()).isEqualTo(6);

    vm0.invoke(LocatorDUnitTest::stopLocator);
    vm1.invoke(LocatorDUnitTest::stopLocator);
    vm2.invoke(LocatorDUnitTest::stopLocator);

    await()
        .until(() -> system.getDM().getDistribution().getView().size() <= 3);

    String newLocators = hostName + "[" + port2 + "]," + hostName + "[" + port3 + "]";
    dsProps.setProperty(LOCATORS, newLocators);

    InternalDistributedMember currentCoordinator = getCreator();
    DistributedMember vm3ID = vm3.invoke(() -> system.getDM().getDistributionManagerId());
    assertEquals(
        "View is " + system.getDM().getDistribution().getView() + " and vm3's ID is "
            + vm3ID,
        vm3ID, vm3.invoke(
            () -> system.getDistributionManager().getDistribution().getView().getCreator()));

    startLocator(vm1, dsProps, port2);
    startLocator(vm2, dsProps, port3);

    await()
        .until(() -> !getCreator().equals(currentCoordinator)
            && system.getDM().getAllHostedLocators().size() == 2);

    vm1.invoke("waitUntilLocatorBecomesCoordinator", this::waitUntilLocatorBecomesCoordinator);
    vm2.invoke("waitUntilLocatorBecomesCoordinator", this::waitUntilLocatorBecomesCoordinator);
    vm3.invoke("waitUntilLocatorBecomesCoordinator", this::waitUntilLocatorBecomesCoordinator);
    vm4.invoke("waitUntilLocatorBecomesCoordinator", this::waitUntilLocatorBecomesCoordinator);

    int netViewId = vm1.invoke("Checking ViewCreator", () -> getView().getViewId());
    assertThat((int) vm2.invoke("checking ViewID", () -> getView().getViewId()))
        .isEqualTo(netViewId);
    assertThat((int) vm3.invoke("checking ViewID", () -> getView().getViewId()))
        .isEqualTo(netViewId);
    assertThat((int) vm4.invoke("checking ViewID", () -> getView().getViewId()))
        .isEqualTo(netViewId);
    assertThat((boolean) vm4
        .invoke("Checking ViewCreator",
            () -> system.getDistributedMember().equals(getView().getCreator()))).isFalse();
    // Given the start up order of servers, this server is the elder server
    assertFalse(vm3
        .invoke("Checking ViewCreator",
            () -> system.getDistributedMember().equals(getView().getCreator())));
    if (vm1.invoke(() -> system.getDistributedMember().equals(getView().getCreator()))) {
      assertThat((boolean) vm2.invoke("Checking ViewCreator",
          () -> system.getDistributedMember().equals(getView().getCreator())))
              .isFalse();
    } else {
      assertThat((boolean) vm2.invoke("Checking ViewCreator",
          () -> system.getDistributedMember().equals(getView().getCreator())))
              .isTrue();
    }
  }

  @Test
  public void testMultipleLocatorsRestartingAtSameTimeWithMissingServers() throws Exception {
    addIgnoredException("ForcedDisconnectException");
    addIgnoredException("Possible loss of quorum");
    addIgnoredException("java.lang.Exception: Message id is");

    String locators =
        hostName + "[" + port1 + "]," + hostName + "[" + port2 + "]," + hostName + "[" + port3
            + "]";

    Properties dsProps = getBasicProperties(locators);
    dsProps.setProperty(LOG_LEVEL, logger.getLevel().name());
    dsProps.setProperty(DISABLE_AUTO_RECONNECT, "true");
    dsProps.setProperty(MEMBER_TIMEOUT, "2000");
    addDSProps(dsProps);

    startLocator(vm0, dsProps, port1);
    startLocator(vm1, dsProps, port2);
    startLocator(vm2, dsProps, port3);

    vm3.invoke(() -> {
      getConnectedDistributedSystem(dsProps);
    });
    vm4.invoke(() -> {
      getConnectedDistributedSystem(dsProps);

      await().until(() -> system.getDM().getViewMembers().size() == 5);
    });

    vm0.invoke(this::forceDisconnect);
    vm1.invoke(this::forceDisconnect);
    vm2.invoke(this::forceDisconnect);

    SerializableRunnable waitForDisconnect = new SerializableRunnable("waitForDisconnect") {
      @Override
      public void run() {
        await()
            .until(() -> system == null);
      }
    };
    vm0.invoke(() -> waitForDisconnect);
    vm1.invoke(() -> waitForDisconnect);
    vm2.invoke(() -> waitForDisconnect);

    String newLocators = hostName + "[" + port2 + "]," + hostName + "[" + port3 + "]";
    dsProps.setProperty(LOCATORS, newLocators);

    getBlackboard().initBlackboard();
    AsyncInvocation async1 = vm1.invokeAsync(() -> {
      getBlackboard().signalGate("vm1ready");
      getBlackboard().waitForGate("readyToConnect", 30, SECONDS);
      System.out.println("vm1 is ready to connect");
      startLocatorBase(dsProps, port2);
    });
    AsyncInvocation async2 = vm2.invokeAsync(() -> {
      getBlackboard().signalGate("vm2ready");
      getBlackboard().waitForGate("readyToConnect", 30, SECONDS);
      System.out.println("vm2 is ready to connect");
      startLocatorBase(dsProps, port3);
    });
    getBlackboard().waitForGate("vm1ready", 30, SECONDS);
    getBlackboard().waitForGate("vm2ready", 30, SECONDS);
    getBlackboard().signalGate("readyToConnect");

    async1.await();
    async2.await();

    vm1.invoke("waitUntilLocatorBecomesCoordinator", this::waitUntilLocatorBecomesCoordinator);
    vm2.invoke("waitUntilLocatorBecomesCoordinator", this::waitUntilLocatorBecomesCoordinator);

    await().untilAsserted(() -> {
      MemberIdentifier viewCreator = vm1.invoke(() -> getView().getCreator());
      MemberIdentifier viewCreator2 = vm1.invoke(() -> getView().getCreator());

      InternalDistributedMember member1 = vm1.invoke(this::getMember);
      InternalDistributedMember member2 = vm2.invoke(this::getMember);

      assertThat(viewCreator2).isEqualTo(viewCreator);
      assertThat(viewCreator).isIn(member1, member2);
      assertThat(member1).isNotEqualTo(member2);

    });

  }

  /**
   * Tests that a VM can connect to a locator that is hosted in its own VM.
   */
  @Test
  public void testConnectToOwnLocator() throws Exception {
    String locators = hostName + "[" + port1 + "]";

    Properties props = getBasicProperties(locators);
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    Locator locator = Locator.startLocatorAndDS(port1, null, props);
    system = (InternalDistributedSystem) locator.getDistributedSystem();
    system.disconnect();
    locator.stop();
  }

  /**
   * Tests that a single VM can NOT host multiple locators
   */
  @Test
  public void testHostingMultipleLocators() throws Exception {
    Locator.startLocator(port1, null);

    assertThatThrownBy(() -> Locator.startLocator(port2, null))
        .isInstanceOf(IllegalStateException.class);
  }

  /**
   * Tests starting, stopping, and restarting a locator. See bug 32856.
   *
   * @since GemFire 4.1
   */
  @Test
  public void testRestartLocator() throws Exception {
    File stateFile = new File("locator" + port1 + "state.dat");

    Properties properties = getBasicProperties(getHostName() + "[" + port1 + "]");
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(LOG_LEVEL, DUnitLauncher.logLevel);
    addDSProps(properties);

    if (stateFile.exists()) {
      assertThat(stateFile.delete()).isTrue();
    }

    logger.info("Starting locator");
    Locator locator = Locator.startLocatorAndDS(port1, null, properties);
    system = (InternalDistributedSystem) locator.getDistributedSystem();

    vm0.invoke(() -> {
      getConnectedDistributedSystem(properties);
    });

    logger.info("Stopping locator");
    locator.stop();

    logger.info("Starting locator");
    locator = Locator.startLocatorAndDS(port1, null, properties);
    system = (InternalDistributedSystem) locator.getDistributedSystem();

    vm0.invoke("disconnect", () -> {
      getConnectedDistributedSystem(properties).disconnect();
    });
  }

  /**
   * a locator is restarted twice with a server and ends up in a split-brain
   */
  @Test
  public void testRestartLocatorMultipleTimes() throws Exception {
    File stateFile = new File("locator" + port1 + "state.dat");

    Properties properties = getBasicProperties(getHostName() + "[" + port1 + "]");
    addDSProps(properties);

    if (stateFile.exists()) {
      assertThat(stateFile.delete()).isTrue();
    }

    Locator locator = Locator.startLocatorAndDS(port1, null, properties);
    system = (InternalDistributedSystem) locator.getDistributedSystem();

    vm0.invoke(() -> {
      getConnectedDistributedSystem(properties);
    });

    locator.stop();
    locator = Locator.startLocatorAndDS(port1, null, properties);
    system = (InternalDistributedSystem) locator.getDistributedSystem();
    assertEquals(2, ((InternalDistributedSystem) locator.getDistributedSystem()).getDM()
        .getViewMembers().size());

    locator.stop();
    locator = Locator.startLocatorAndDS(port1, null, properties);
    system = (InternalDistributedSystem) locator.getDistributedSystem();
    assertEquals(2, ((InternalDistributedSystem) locator.getDistributedSystem()).getDM()
        .getViewMembers().size());
  }

  protected void addDSProps(Properties p) {
    p.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    p.setProperty(USE_CLUSTER_CONFIGURATION, "false");
  }

  static InternalDistributedSystem getConnectedDistributedSystem(Properties properties) {
    if (system == null || !system.isConnected()) {
      properties.setProperty(NAME, "vm" + VM.getCurrentVMNum());
      system = (InternalDistributedSystem) DistributedSystem.connect(properties);
    }
    return system;
  }

  private void startLocatorWithPortAndProperties(final int port, final Properties properties)
      throws IOException {
    Locator locator = Locator.startLocatorAndDS(port, null, properties);
    system = (InternalDistributedSystem) locator.getDistributedSystem();
    assertThat(locator).isNotNull();
  }

  private String getSingleKeyKeystore() {
    return createTempFileFromResource(getClass(), "/ssl/trusted.keystore").getAbsolutePath();
  }

  private String getMultiKeyKeystore() {
    return createTempFileFromResource(getClass(), "/org/apache/geode/internal/net/multiKey.jks")
        .getAbsolutePath();
  }

  private String getMultiKeyTruststore() {
    return createTempFileFromResource(getClass(),
        "/org/apache/geode/internal/net/multiKeyTrust.jks").getAbsolutePath();
  }

  private void startVerifyAndStopLocator(VM loc1, VM loc2, int port1, int port2,
      Properties properties) throws Exception {
    try {
      getBlackboard().initBlackboard();
      AsyncInvocation<Void> async1 = loc1.invokeAsync("startLocator1", () -> {
        getBlackboard().signalGate("locator1");
        getBlackboard().waitForGate("go", 60, SECONDS);
        startLocatorWithPortAndProperties(port1, properties);
      });

      AsyncInvocation<Void> async2 = loc2.invokeAsync("startLocator2", () -> {
        getBlackboard().signalGate("locator2");
        getBlackboard().waitForGate("go", 60, SECONDS);
        startLocatorWithPortAndProperties(port2, properties);
      });

      getBlackboard().waitForGate("locator1", 60, SECONDS);
      getBlackboard().waitForGate("locator2", 60, SECONDS);
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

  private void waitForMemberToBecomeLeadMemberOfDistributedSystem(final DistributedMember member,
      final DistributedSystem sys) {
    await().until(() -> {
      DistributedMember lead = MembershipManagerHelper.getLeadMember(sys);
      if (member != null) {
        return member.equals(lead);
      }
      return lead == null;
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

  private Locator startLocatorBase(Properties properties, int port) throws IOException {
    properties.setProperty(NAME, "vm" + VM.getCurrentVMNum());

    Locator locator = Locator.startLocatorAndDS(port, null, properties);
    system = (InternalDistributedSystem) locator.getDistributedSystem();

    return locator;
  }

  void startLocatorWithSomeBasicProperties(VM vm, int port) {
    Properties locProps = new Properties();
    locProps.setProperty(MEMBER_TIMEOUT, "1000");
    addDSProps(locProps);

    startLocator(vm, locProps, port);
  }

  private void startLocatorPreferredCoordinators(VM vm0, int port) {
    try {
      System.setProperty(InternalLocator.LOCATORS_PREFERRED_AS_COORDINATORS, "true");

      Properties locProps1 = new Properties();
      locProps1.setProperty(MCAST_PORT, "0");
      locProps1.setProperty(LOG_LEVEL, logger.getLevel().name());
      addDSProps(locProps1);

      startLocator(vm0, locProps1, port);
    } finally {
      System.clearProperty(InternalLocator.LOCATORS_PREFERRED_AS_COORDINATORS);
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
      DistributedSystem sys = system;
      if (sys != null && sys.isConnected()) {
        sys.disconnect();
      }
    });
  }

  private void forceDisconnect() {
    DistributedTestUtils.crashDistributedSystem(system);
  }

  Properties getBasicProperties(String locators) {
    Properties props = new Properties();
    props.setProperty(LOCATORS, locators);
    return props;
  }

  private Properties getClusterProperties(String locators,
      String enableNetworkPartitionDetectionString) {
    Properties properties = getBasicProperties(locators);
    properties.setProperty(DISABLE_AUTO_RECONNECT, "true");
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(ENABLE_NETWORK_PARTITION_DETECTION,
        enableNetworkPartitionDetectionString);
    properties.setProperty(LOCATOR_WAIT_TIME, "10"); // seconds
    properties.setProperty(MEMBER_TIMEOUT, "2000");
    properties.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    return properties;
  }

  private void waitUntilTheSystemIsConnected(VM vm2, VM locatorVM) {
    await().until(() -> {
      assertThat(isSystemConnected())
          .describedAs("Distributed system should not have disconnected")
          .isTrue();

      checkSystemConnectedInVMs(vm2, locatorVM);
      return true;
    });
  }

  /**
   * New test hook which blocks before closing channel.
   */
  private class TestHook implements MembershipTestHook {

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
        errorCollector.addError(cause);
      }
    }

    void reset() {
      unboundedWait = false;
    }
  }

  private static class MyMembershipListener implements MembershipListener {

    volatile boolean quorumLostInvoked;
    final List<String> suspectReasons = new ArrayList<>(50);

    @Override
    public void memberJoined(DistributionManager distributionManager,
        InternalDistributedMember id) {
      // nothing
    }

    @Override
    public void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      // nothing
    }

    @Override
    public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
        InternalDistributedMember whoSuspected, String reason) {
      suspectReasons.add(reason);
    }

    @Override
    public void quorumLost(DistributionManager distributionManager,
        Set<InternalDistributedMember> failures,
        List<InternalDistributedMember> remaining) {
      quorumLostInvoked = true;
      logger.info("quorumLost invoked in test code");
    }
  }
}
