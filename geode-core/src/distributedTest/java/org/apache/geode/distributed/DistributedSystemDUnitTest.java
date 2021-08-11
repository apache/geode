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

import static java.lang.Integer.parseInt;
import static java.net.NetworkInterface.getNetworkInterfaces;
import static org.apache.geode.distributed.ConfigurationProperties.ACK_WAIT_THRESHOLD;
import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_TCP;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_FLOW_CONTROL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBERSHIP_PORT_RANGE;
import static org.apache.geode.distributed.ConfigurationProperties.REDUNDANCY_ZONE;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.distributed.ConfigurationProperties.TCP_PORT;
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_ACK_WAIT_THRESHOLD;
import static org.apache.geode.distributed.internal.OperationExecutors.SERIAL_EXECUTOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPortRange;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableUDPPort;
import static org.apache.geode.internal.inet.LocalHostUtil.getLocalHost;
import static org.apache.geode.test.dunit.DistributedTestUtils.getDUnitLocatorPort;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

import org.apache.geode.CancelException;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.SerializationException;
import org.apache.geode.SystemConnectException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.Distribution;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.SerialDistributionMessage;
import org.apache.geode.distributed.internal.SizeableRunnable;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Tests the functionality of the {@link DistributedSystem} class.
 */
@Category({MembershipTest.class})
public class DistributedSystemDUnitTest extends JUnit4DistributedTestCase {
  @Rule
  public TestRule restoreSystemProperties = new DistributedRestoreSystemProperties();
  private int mcastPort;
  private int locatorPort;
  private int tcpPort;
  private int lowerBoundOfPortRange;
  private int upperBoundOfPortRange;

  @Before
  public void before() throws Exception {
    disconnectAllFromDS();

    this.mcastPort = getRandomAvailableUDPPort();
    this.locatorPort = getRandomAvailableTCPPort();
    this.tcpPort = getRandomAvailableTCPPort();

    int[] portRange = getRandomAvailableTCPPortRange(3, true);
    this.lowerBoundOfPortRange = portRange[0];
    this.upperBoundOfPortRange = portRange[portRange.length - 1];
  }

  @After
  public void after() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * ensure that waitForMemberDeparture correctly flushes the serial message queue for the given
   * member
   */
  @Test
  public void testWaitForDeparture() throws Exception {
    Properties config = new Properties();
    config.put(LOCATORS, "");
    config.put(START_LOCATOR, "localhost[" + this.locatorPort + "]");
    config.put(DISABLE_TCP, "true");

    InternalDistributedSystem system =
        (InternalDistributedSystem) DistributedSystem.connect(config);

    // construct a member ID that will represent a departed member
    InternalDistributedMember member =
        new InternalDistributedMember("localhost", 12345);

    // schedule a message in order to create a queue for the fake member
    ClusterDistributionManager distributionManager =
        (ClusterDistributionManager) system.getDistributionManager();
    final FakeMessage message = new FakeMessage(null);

    distributionManager.getExecutors().getExecutor(SERIAL_EXECUTOR, member)
        .execute(new SizeableRunnable(100) {

          @Override
          public void run() { // always throws NullPointerException
            message.doAction(distributionManager, false);
          }

          @Override
          public String toString() {
            return "Processing fake message";
          }
        });

    Assert.assertTrue("expected the serial queue to be flushed",
        distributionManager.getDistribution().waitForDeparture(member));
    Assert.assertTrue(message.processed);
  }

  /**
   * Tests that we can get a DistributedSystem with the same configuration twice.
   */
  @Test
  public void testGetSameSystemTwice() {
    Properties config = createLonerConfig();

    // set a flow-control property for the test (bug 37562)
    config.setProperty(MCAST_FLOW_CONTROL, "3000000,0.20,3000");

    DistributedSystem system1 = DistributedSystem.connect(config);
    DistributedSystem system2 = DistributedSystem.connect(config);

    assertThat(system2).isSameAs(system1);
  }

  /**
   * Tests that getting a <code>DistributedSystem</code> with a different configuration after one
   * has already been obtained throws an exception.
   */
  @Test
  public void testGetDifferentSystem() {
    Properties config = createLonerConfig();
    config.setProperty(MCAST_FLOW_CONTROL, "3000000,0.20,3000");

    DistributedSystem.connect(config);

    config.setProperty(MCAST_ADDRESS, "224.0.0.1");

    assertThatThrownBy(() -> DistributedSystem.connect(config))
        .isInstanceOf(IllegalStateException.class);
  }

  /**
   * Tests getting a system with a different configuration after another system has been closed.
   */
  @Test
  public void testGetDifferentSystemAfterClose() {
    Properties config = createLonerConfig();

    DistributedSystem system1 = DistributedSystem.connect(config);
    system1.disconnect();

    int time = DEFAULT_ACK_WAIT_THRESHOLD + 17;
    config.put(ACK_WAIT_THRESHOLD, String.valueOf(time));

    DistributedSystem system2 = DistributedSystem.connect(config);
    system2.disconnect();
  }

  @Test
  public void testGetProperties() {
    int unusedPort = 0;

    Properties config = createLonerConfig();
    DistributedSystem system = DistributedSystem.connect(config);

    assertThat(system.getProperties()).isNotSameAs(config);
    assertThat(parseInt(system.getProperties().getProperty(MCAST_PORT))).isEqualTo(unusedPort);

    system.disconnect();

    assertThat(system.getProperties()).isNotSameAs(config);
    assertThat(parseInt(system.getProperties().getProperty(MCAST_PORT))).isEqualTo(unusedPort);
  }

  @Test
  public void testIsolatedDistributedSystem() throws Exception {
    Properties config = createLonerConfig();
    InternalDistributedSystem system = getSystem(config);

    // make sure isolated distributed system can still create a cache and region
    Cache cache = CacheFactory.create(system);
    Region region = cache.createRegion(getUniqueName(), new AttributesFactory().create());
    region.put("test", "value");

    assertThat(region.get("test")).isEqualTo("value");
  }

  /**
   * test the ability to set the port used to listen for tcp/ip connections
   */
  @Test
  public void testSpecificTcpPort() throws Exception {
    Properties config = new Properties();
    config.put(LOCATORS, "localhost[" + getDUnitLocatorPort() + "]");
    config.setProperty(TCP_PORT, String.valueOf(this.tcpPort));

    InternalDistributedSystem system = getSystem(config);

    ClusterDistributionManager dm = (ClusterDistributionManager) system.getDistributionManager();
    Distribution mgr = dm.getDistribution();
    assertThat(mgr.getLocalMember().getDirectChannelPort()).isEqualTo(this.tcpPort);
  }

  /**
   * test that loopback cannot be used as a bind address when a locator w/o a bind address is being
   * used
   */
  @Test
  public void testLoopbackNotAllowed() throws Exception {
    // assert or assume that loopback is not null
    InetAddress loopback = getLoopback();
    assertThat(loopback).isNotNull();

    String locators = getLocalHost().getHostName() + "[" + getDUnitLocatorPort() + "]";

    Properties config = new Properties();
    config.put(LOCATORS, locators);
    config.setProperty(BIND_ADDRESS, loopback.getHostAddress());

    getLogWriter().info("attempting to connect with " + loopback + " and locators=" + locators);

    assertThatThrownBy(() -> getSystem(config)).isInstanceOf(GemFireConfigException.class);
  }

  @Test
  public void testPortRange() throws Exception {
    Properties config = new Properties();
    config.put(LOCATORS, "localhost[" + getDUnitLocatorPort() + "]");
    System.clearProperty(GEMFIRE_PREFIX + MEMBERSHIP_PORT_RANGE);
    config.setProperty(MEMBERSHIP_PORT_RANGE,
        this.lowerBoundOfPortRange + "-" + this.upperBoundOfPortRange);

    InternalDistributedSystem system = getSystem(config);
    ClusterDistributionManager dm = (ClusterDistributionManager) system.getDistributionManager();
    InternalDistributedMember member = dm.getDistributionManagerId();

    verifyMembershipPortsInRange(member, this.lowerBoundOfPortRange, this.upperBoundOfPortRange);
  }

  @Test
  public void testConflictingUDPPort() {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, String.valueOf(this.mcastPort));
    config.setProperty(START_LOCATOR, "localhost[" + this.locatorPort + "]");
    System.clearProperty(GEMFIRE_PREFIX + MEMBERSHIP_PORT_RANGE);
    config.setProperty(MEMBERSHIP_PORT_RANGE,
        this.lowerBoundOfPortRange + "-" + this.upperBoundOfPortRange);

    DistributedSystem.connect(config);

    IgnoredException.addIgnoredException("SystemConnectException", VM.getVM(1));
    VM.getVM(1).invoke(() -> {
      String oldMembershipPortRange = System.clearProperty(GEMFIRE_PREFIX + MEMBERSHIP_PORT_RANGE);
      String locators = (String) config.remove(START_LOCATOR);

      config.put(LOCATORS, locators);

      try {
        assertThatThrownBy(() -> DistributedSystem.connect(config))
            .isInstanceOfAny(GemFireConfigException.class, SystemConnectException.class);
      } finally {
        if (oldMembershipPortRange != null) {
          System.setProperty(GEMFIRE_PREFIX + MEMBERSHIP_PORT_RANGE, oldMembershipPortRange);
        }
      }
    });
  }

  @Test
  public void memberShouldWaitUntilAStartupResponseIsReceived() {

    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    Properties properties = new Properties();
    properties.setProperty(REDUNDANCY_ZONE, "testzone");

    vm0.invoke(() -> {
      getSystem(properties);
    });

    vm1.invoke(() -> {
      InternalDistributedSystem system = getSystem(properties);

      // Redundancy zone is part of the data that is sent with the startup response
      // If we receive a startup response, we should see that vm0 is in the same
      // redundancy zone as the current member.
      assertThat(
          system.getDistributionManager().getMembersInSameZone(system.getDistributedMember()))
              .hasSize(2);
    });
  }

  @Test(timeout = 600_000)
  public void failedMessageReceivedBeforeStartupShouldNotDeadlock() {

    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    // Install a membership listener which will send a message to
    // any new member that joins. The message will fail to deserialize, triggering
    // a failure reply
    vm0.invoke(() -> {
      InternalDistributedSystem system = getSystem();
      DistributionManager dm = system.getDM();
      dm.addMembershipListener(new MembershipListener() {
        @Override
        public void memberJoined(DistributionManager distributionManager,
            InternalDistributedMember id) {
          FunctionService.onMember(id).execute(new FailDeserializationFunction());
        }
      });
    });

    vm1.invoke(() -> {
      IgnoredException.addIgnoredException(SerializationException.class);

      // Join the the system. This will trigger the above membership listener. If
      // the failed serialization causes a deadlock, this method will hang
      getSystem();
    });
  }

  /**
   * Tests that configuring a distributed system with a cache-xml-file of "" does not initialize a
   * cache.
   *
   * Verifies: "Allow the cache-xml-file specification to be an empty string"
   *
   * @since GemFire 4.0
   */
  @Test
  public void testEmptyCacheXmlFile() throws Exception {
    Properties config = createLonerConfig();
    config.setProperty(CACHE_XML_FILE, "");

    DistributedSystem system = DistributedSystem.connect(config);

    assertThatThrownBy(() -> CacheFactory.getInstance(system)).isInstanceOf(CancelException.class);

    // now make sure we can create the cache
    Cache cache = CacheFactory.create(system);

    assertThat(cache).isNotNull();
    assertThat(cache.isClosed()).isFalse();
  }

  private Properties createLonerConfig() {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    return config;
  }

  private void verifyMembershipPortsInRange(final InternalDistributedMember member,
      final int lowerBound, final int upperBound) {
    assertThat(member.getMembershipPort())
        .as("membership port")
        .isBetween(lowerBound, upperBound);
    assertThat(member.getDirectChannelPort())
        .as("direct channel port")
        .isBetween(lowerBound, upperBound);
  }

  private InetAddress getLoopback() throws SocketException, UnknownHostException {
    for (Enumeration<NetworkInterface> networkInterfaceEnumeration =
        getNetworkInterfaces(); networkInterfaceEnumeration.hasMoreElements();) {

      NetworkInterface networkInterface = networkInterfaceEnumeration.nextElement();

      for (Enumeration<InetAddress> addressEnum = networkInterface.getInetAddresses(); addressEnum
          .hasMoreElements();) {

        InetAddress address = addressEnum.nextElement();
        Class theClass =
            getLocalHost() instanceof Inet4Address ? Inet4Address.class : Inet6Address.class;

        if (address.isLoopbackAddress() && address.getClass().isAssignableFrom(theClass)) {
          return address;
        }
      }
    }
    return null;
  }

  /**
   * What is the point of this FakeMessage? Member variables are unused and doAction actually throws
   * NullPointerException.
   */
  private static class FakeMessage extends SerialDistributionMessage {
    private volatile boolean[] blocked; // always null
    private volatile boolean processed; // unused

    FakeMessage(boolean[] blocked) { // null is always passed in
      this.blocked = blocked;
    }

    public void doAction(ClusterDistributionManager dm, boolean block) {
      this.processed = true;
      if (block) {
        synchronized (this.blocked) { // throws NullPointerException here
          this.blocked[0] = true;
          this.blocked.notify();
          try {
            this.blocked.wait(60000);
          } catch (InterruptedException e) {
          }
        }
      }
    }

    @Override
    public int getDSFID() {
      return 0; // never serialized
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      // this is never called
    }

    @Override
    public String toString() {
      return "FakeMessage(blocking=" + (this.blocked != null) + ")";
    }
  }
}
