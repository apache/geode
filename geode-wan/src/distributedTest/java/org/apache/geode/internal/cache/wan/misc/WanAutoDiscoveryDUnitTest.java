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
package org.apache.geode.internal.cache.wan.misc;

import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.IncompatibleSystemException;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class WanAutoDiscoveryDUnitTest extends WANTestBase {


  public WanAutoDiscoveryDUnitTest() {
    super();
  }

  /**
   * Test to validate that sender can not be started without locator started. else
   * GemFireConfigException will be thrown.
   */
  @Test
  public void test_GatewaySender_Started_Before_Locator() {
    try {
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      vm0.invoke(() -> WANTestBase.createCache(port));
      vm0.invoke(
          () -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, false));
      fail("Expected GemFireConfigException but not thrown");
    } catch (Exception e) {
      if (!(e.getCause() instanceof GemFireConfigException)) {
        fail("Expected GemFireConfigException but received :", e);
      }
    }
  }

  /**
   * Test to validate that all locators in one DS should have same name. Though this test passes, it
   * causes other below tests to fail. In this test, VM1 is throwing IncompatibleSystemException
   * after startInitLocator. I think, after throwing this exception, locator is not stopped properly
   * and hence other tests are failing.
   *
   */
  @Ignore
  @Test
  public void test_AllLocatorsInDSShouldHaveDistributedSystemId() {
    try {
      Integer lnLocPort1 = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

      vm1.invoke(() -> WANTestBase.createSecondLocator(2, lnLocPort1));
      fail("Expected IncompatibleSystemException but not thrown");
    } catch (Exception e) {
      if (!(e.getCause() instanceof IncompatibleSystemException)) {
        fail("Expected IncompatibleSystemException but received :", e);
      }
    }
  }

  /**
   * Test to validate that multiple locators added on LN site and multiple locators on Ny site
   * recognizes each other
   *
   */
  @Test
  public void test_NY_Recognises_ALL_LN_Locators() {
    Set<InetSocketAddress> locatorPorts = new HashSet<>();
    Map<Integer, Set<InetSocketAddress>> dsVsPort = new HashMap<>();
    dsVsPort.put(1, locatorPorts);

    Integer lnLocPort1 = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    locatorPorts.add(new InetSocketAddress("localhost", lnLocPort1));

    Integer lnLocPort2 = vm1.invoke(() -> WANTestBase.createSecondLocator(1, lnLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", lnLocPort2));

    locatorPorts = new HashSet<>();
    dsVsPort.put(2, locatorPorts);
    Integer nyLocPort1 =
        vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", nyLocPort1));

    Integer nyLocPort2 = vm3
        .invoke(() -> WANTestBase.createSecondRemoteLocator(2, nyLocPort1, lnLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", nyLocPort2));

    final int siteSizeToCheck = dsVsPort.size();
    vm0.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm1.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm2.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm3.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
  }

  /**
   * Test to validate that multiple locators added two sets receive eachothers hostname for client
   * setting even when the locator is started through the API.
   */
  @Test
  public void locatorsReceiveHostnameForClientsFromRemoteSite() {
    Set<InetSocketAddress> locatorPorts = new HashSet<>();
    Map<Integer, Set<InetSocketAddress>> dsVsPort = new HashMap<>();
    dsVsPort.put(1, locatorPorts);

    Integer lnLocPort1 = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    locatorPorts.add(new InetSocketAddress("localhost", lnLocPort1));

    Integer lnLocPort2 = vm1.invoke(() -> WANTestBase.createSecondLocator(1, lnLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", lnLocPort2));

    locatorPorts = new HashSet<>();
    dsVsPort.put(2, locatorPorts);
    Integer nyLocPort1 =
        vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", nyLocPort1));

    Integer nyLocPort2 = vm3.invoke(
        () -> WANTestBase.createSecondRemoteLocatorWithAPI(2, nyLocPort1, lnLocPort1, "localhost"));
    locatorPorts.add(new InetSocketAddress("localhost", nyLocPort2));

    final int siteSizeToCheck = dsVsPort.size();
    vm0.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm1.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm2.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm3.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
  }

  /**
   * Test to validate that TK site's locator is recognized by LN and NY. Test to validate that HK
   * site's locator is recognized by LN , NY, TK.
   */
  @Test
  public void test_NY_Recognises_TK_AND_HK_Through_LN_Locator() {

    Map<Integer, Set<InetSocketAddress>> dsVsPort = new HashMap<>();

    Set<InetSocketAddress> locatorPorts = new HashSet<>();
    dsVsPort.put(1, locatorPorts);

    Integer lnLocPort1 = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    locatorPorts.add(new InetSocketAddress("localhost", lnLocPort1));

    locatorPorts = new HashSet<>();
    dsVsPort.put(2, locatorPorts);
    Integer nyLocPort1 =
        vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", nyLocPort1));

    locatorPorts = new HashSet<>();
    dsVsPort.put(3, locatorPorts);
    Integer tkLocPort =
        vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", tkLocPort));

    locatorPorts = new HashSet<>();
    dsVsPort.put(4, locatorPorts);
    Integer hkLocPort =
        vm3.invoke(() -> WANTestBase.createFirstRemoteLocator(4, lnLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", hkLocPort));

    final int siteSizeToCheck = dsVsPort.size();
    vm0.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm1.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm2.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm3.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
  }

  @Test
  public void test_TK_Recognises_LN_AND_NY() {

    Map<Integer, Set<InetSocketAddress>> dsVsPort = new HashMap<>();

    Set<InetSocketAddress> locatorPorts = new HashSet<>();
    dsVsPort.put(1, locatorPorts);

    Integer lnLocPort1 = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    locatorPorts.add(new InetSocketAddress("localhost", lnLocPort1));

    locatorPorts = new HashSet<>();
    dsVsPort.put(2, locatorPorts);
    Integer nyLocPort1 =
        vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", nyLocPort1));

    locatorPorts = new HashSet<>();
    dsVsPort.put(3, locatorPorts);
    Integer tkLocPort =
        vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(3, nyLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", tkLocPort));

    final int siteSizeToCheck = dsVsPort.size();
    vm0.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm1.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm2.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
  }

  @Category({WanTest.class})
  @Test
  public void test_NY_Recognises_TK_AND_HK_Simultaneously() throws InterruptedException {
    Map<Integer, Set<InetSocketAddress>> dsVsPort = new HashMap<>();

    Set<InetSocketAddress> locatorPortsln = new HashSet<>();
    dsVsPort.put(1, locatorPortsln);
    Integer lnLocPort1 = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    locatorPortsln.add(new InetSocketAddress("localhost", lnLocPort1));

    Set<InetSocketAddress> locatorPortsny = new HashSet<>();
    dsVsPort.put(2, locatorPortsny);
    Integer nyLocPort1 =
        vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnLocPort1));
    locatorPortsny.add(new InetSocketAddress("localhost", nyLocPort1));

    int AsyncInvocationArrSize = 4;
    AsyncInvocation<?>[] async = new AsyncInvocation[AsyncInvocationArrSize];

    Set<InetSocketAddress> locatorPortstk = new HashSet<>();
    dsVsPort.put(3, locatorPortstk);
    async[0] = vm2.invokeAsync(() -> WANTestBase.createFirstRemoteLocator(3, lnLocPort1));

    Set<InetSocketAddress> locatorPortshk = new HashSet<>();
    dsVsPort.put(4, locatorPortshk);
    async[1] = vm3.invokeAsync(() -> WANTestBase.createFirstRemoteLocator(4, nyLocPort1));

    async[2] = vm4.invokeAsync(() -> WANTestBase.createSecondLocator(1, lnLocPort1));

    async[3] = vm5.invokeAsync(() -> WANTestBase.createSecondLocator(2, nyLocPort1));


    try {
      async[0].await();
      async[1].await();
      async[2].await();
      async[3].await();
    } catch (InterruptedException e) {
      fail("", e);
    }

    locatorPortstk.add(new InetSocketAddress("localhost", (Integer) async[0].get()));
    locatorPortshk.add(new InetSocketAddress("localhost", (Integer) async[1].get()));
    locatorPortsln.add(new InetSocketAddress("localhost", (Integer) async[2].get()));
    locatorPortsny.add(new InetSocketAddress("localhost", (Integer) async[3].get()));

    final int siteSizeToCheck = dsVsPort.size();
    vm0.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm1.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm2.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm3.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
  }


  @Test
  public void test_LN_Sender_recognises_ALL_NY_Locators() {
    IgnoredException ie = IgnoredException
        .addIgnoredException("could not get remote locator information for remote site");
    try {
      Integer lnLocPort1 = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

      Integer lnLocPort2 =
          vm5.invoke(() -> WANTestBase.createSecondLocator(1, lnLocPort1));

      vm2.invoke(() -> WANTestBase.createCache(lnLocPort1, lnLocPort2));

      vm2.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

      Integer nyLocPort1 =
          vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnLocPort1));

      vm2.invoke(() -> WANTestBase.startSender("ln"));

      // Since to fix Bug#46289, we have moved call to initProxy in getConnection which will be
      // called
      // only when batch is getting dispatched.
      // So for locator discovery callback to work, its now expected that atleast try to send a
      // batch
      // so that proxy will be initialized
      vm2.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

      vm2.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 10));

      Integer nyLocPort2 = vm3
          .invoke(() -> WANTestBase.createSecondRemoteLocator(2, nyLocPort1, lnLocPort1));

      InetSocketAddress locatorToWaitFor = new InetSocketAddress("localhost", nyLocPort2);

      vm2.invoke(() -> WANTestBase.checkLocatorsinSender("ln", locatorToWaitFor));

      Integer nyLocPort3 = vm4
          .invoke(() -> WANTestBase.createSecondRemoteLocator(2, nyLocPort1, lnLocPort1));

      InetSocketAddress locatorToWaitFor2 = new InetSocketAddress("localhost", nyLocPort3);

      vm2.invoke(() -> WANTestBase.checkLocatorsinSender("ln", locatorToWaitFor2));
    } finally {
      ie.remove();
    }
  }

  @Test
  public void test_RingTopology() {

    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(4);

    final Set<String> site1LocatorsPort = new HashSet<>();
    site1LocatorsPort.add("localhost[" + ports[0] + "]");

    final Set<String> site2LocatorsPort = new HashSet<>();
    site2LocatorsPort.add("localhost[" + ports[1] + "]");

    final Set<String> site3LocatorsPort = new HashSet<>();
    site3LocatorsPort.add("localhost[" + ports[2] + "]");

    final Set<String> site4LocatorsPort = new HashSet<>();
    site4LocatorsPort.add("localhost[" + ports[3] + "]");

    Map<Integer, Set<String>> dsVsPort = new HashMap<>();
    dsVsPort.put(1, site1LocatorsPort);
    dsVsPort.put(2, site2LocatorsPort);
    dsVsPort.put(3, site3LocatorsPort);
    dsVsPort.put(4, site4LocatorsPort);

    int AsyncInvocationArrSize = 9;
    AsyncInvocation<?>[] async = new AsyncInvocation[AsyncInvocationArrSize];

    async[0] = vm0.invokeAsync(
        () -> WANTestBase.createLocator(1, ports[0], site1LocatorsPort, site2LocatorsPort));

    async[1] = vm1.invokeAsync(
        () -> WANTestBase.createLocator(2, ports[1], site2LocatorsPort, site3LocatorsPort));

    async[2] = vm2.invokeAsync(
        () -> WANTestBase.createLocator(3, ports[2], site3LocatorsPort, site4LocatorsPort));

    async[3] = vm3.invokeAsync(
        () -> WANTestBase.createLocator(4, ports[3], site4LocatorsPort, site1LocatorsPort));

    // deprecatedPause(5000);
    try {
      async[0].await();
      async[1].await();
      async[2].await();
      async[3].await();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail("Could not join async operations");
    }

    vm0.invoke(() -> WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort));
    vm1.invoke(() -> WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort));
    vm2.invoke(() -> WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort));
    vm3.invoke(() -> WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort));
  }

  @Ignore
  @Test
  public void test_3Sites3Locators() {
    final Set<String> site1LocatorsPort = new HashSet<String>();
    int site1Port1 = AvailablePortHelper.getRandomAvailableTCPPort();
    site1LocatorsPort.add("localhost[" + site1Port1 + "]");
    int site1Port2 = AvailablePortHelper.getRandomAvailableTCPPort();
    site1LocatorsPort.add("localhost[" + site1Port2 + "]");
    int site1Port3 = AvailablePortHelper.getRandomAvailableTCPPort();
    site1LocatorsPort.add("localhost[" + site1Port3 + "]");

    final Set<String> site2LocatorsPort = new HashSet<String>();
    int site2Port1 = AvailablePortHelper.getRandomAvailableTCPPort();
    site2LocatorsPort.add("localhost[" + site2Port1 + "]");
    int site2Port2 = AvailablePortHelper.getRandomAvailableTCPPort();
    site2LocatorsPort.add("localhost[" + site2Port2 + "]");
    int site2Port3 = AvailablePortHelper.getRandomAvailableTCPPort();
    site2LocatorsPort.add("localhost[" + site2Port3 + "]");

    final Set<String> site3LocatorsPort = new HashSet<String>();
    int site3Port1 = AvailablePortHelper.getRandomAvailableTCPPort();
    site3LocatorsPort.add("localhost[" + site3Port1 + "]");
    final int site3Port2 = AvailablePortHelper.getRandomAvailableTCPPort();
    site3LocatorsPort.add("localhost[" + site3Port2 + "]");
    int site3Port3 = AvailablePortHelper.getRandomAvailableTCPPort();
    site3LocatorsPort.add("localhost[" + site3Port3 + "]");

    Map<Integer, Set<String>> dsVsPort = new HashMap<>();
    dsVsPort.put(1, site1LocatorsPort);
    dsVsPort.put(2, site2LocatorsPort);
    dsVsPort.put(3, site3LocatorsPort);

    int AsyncInvocationArrSize = 9;
    AsyncInvocation<?>[] async = new AsyncInvocation[AsyncInvocationArrSize];

    async[0] = vm0.invokeAsync(
        () -> WANTestBase.createLocator(1, site1Port1, site1LocatorsPort, site2LocatorsPort));

    async[8] = vm0.invokeAsync(() -> WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort));

    async[1] = vm1.invokeAsync(
        () -> WANTestBase.createLocator(1, site1Port2, site1LocatorsPort, site2LocatorsPort));
    async[2] = vm2.invokeAsync(
        () -> WANTestBase.createLocator(1, site1Port3, site1LocatorsPort, site2LocatorsPort));

    async[3] = vm3.invokeAsync(
        () -> WANTestBase.createLocator(2, site2Port1, site2LocatorsPort, site3LocatorsPort));
    async[4] = vm4.invokeAsync(
        () -> WANTestBase.createLocator(2, site2Port2, site2LocatorsPort, site3LocatorsPort));
    async[5] = vm5.invokeAsync(
        () -> WANTestBase.createLocator(2, site2Port3, site2LocatorsPort, site3LocatorsPort));

    async[6] = vm6.invokeAsync(
        () -> WANTestBase.createLocator(3, site3Port1, site3LocatorsPort, site1LocatorsPort));
    async[7] = vm7.invokeAsync(
        () -> WANTestBase.createLocator(3, site3Port2, site3LocatorsPort, site1LocatorsPort));

    WANTestBase.createLocator(3, site3Port3, site3LocatorsPort, site1LocatorsPort);
    long startTime = System.currentTimeMillis();

    try {
      async[0].await();
      async[1].await();
      async[2].await();
      async[3].await();
      async[4].await();
      async[5].await();
      async[6].await();
      async[7].await();
      async[8].await();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail("Could not join async operations");
    }
    Long endTime = null;
    try {
      endTime = (Long) async[8].get();
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Could not get end time", e);
    }

    getLogWriter().info(
        "Time taken for all 9 locators discovery in 3 sites: " + (endTime - startTime));

    vm0.invoke(() -> WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort));
    vm1.invoke(() -> WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort));
    vm2.invoke(() -> WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort));
    vm3.invoke(() -> WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort));
    vm4.invoke(() -> WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort));
    vm5.invoke(() -> WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort));
    vm6.invoke(() -> WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort));
    vm7.invoke(() -> WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort));
    WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort);
  }


  @Test
  public void test_LN_Peer_Locators_Exchange_Information() {
    Set<InetSocketAddress> locatorPorts = new HashSet<>();
    Map<Integer, Set<InetSocketAddress>> dsVsPort = new HashMap<>();
    dsVsPort.put(1, locatorPorts);

    Integer lnLocPort1 = vm0.invoke(() -> WANTestBase.createFirstPeerLocator(1));
    locatorPorts.add(new InetSocketAddress("localhost", lnLocPort1));

    Integer lnLocPort2 =
        vm1.invoke(() -> WANTestBase.createSecondPeerLocator(1, lnLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", lnLocPort2));

    final int siteSizeToCheck = dsVsPort.size();
    vm0.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm1.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
  }

  @Test
  public void test_LN_NY_TK_5_PeerLocators_1_ServerLocator() {
    Map<Integer, Set<InetSocketAddress>> dsVsPort = new HashMap<>();


    Set<InetSocketAddress> locatorPorts = new HashSet<>();
    dsVsPort.put(1, locatorPorts);
    Integer lnLocPort1 = vm0.invoke(() -> WANTestBase.createFirstPeerLocator(1));
    locatorPorts.add(new InetSocketAddress("localhost", lnLocPort1));
    Integer lnLocPort2 =
        vm1.invoke(() -> WANTestBase.createSecondPeerLocator(1, lnLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", lnLocPort2));

    locatorPorts = new HashSet<>();
    dsVsPort.put(2, locatorPorts);
    Integer nyLocPort1 =
        vm2.invoke(() -> WANTestBase.createFirstRemotePeerLocator(2, lnLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", nyLocPort1));
    Integer nyLocPort2 = vm3
        .invoke(() -> WANTestBase.createSecondRemotePeerLocator(2, nyLocPort1, lnLocPort2));
    locatorPorts.add(new InetSocketAddress("localhost", nyLocPort2));

    locatorPorts = new HashSet<>();
    dsVsPort.put(3, locatorPorts);
    Integer tkLocPort1 =
        vm4.invoke(() -> WANTestBase.createFirstRemotePeerLocator(3, nyLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", tkLocPort1));
    Integer tkLocPort2 = vm5
        .invoke(() -> WANTestBase.createSecondRemotePeerLocator(3, tkLocPort1, nyLocPort1));
    locatorPorts.add(new InetSocketAddress("localhost", tkLocPort2));
    Integer tkLocPort3 = vm6
        .invoke(() -> WANTestBase.createSecondRemoteLocator(3, tkLocPort1, nyLocPort2));
    locatorPorts.add(new InetSocketAddress("localhost", tkLocPort3));

    // deprecatedPause(5000);

    final int siteSizeToCheck = dsVsPort.size();
    vm0.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm1.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm2.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm3.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm4.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm5.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
    vm6.invoke(() -> WANTestBase.checkAllSiteMetaData(dsVsPort, siteSizeToCheck));
  }

  @Test
  public void testNoThreadLeftBehind() {
    // Get active thread count before test
    int activeThreadCountBefore = Thread.activeCount();

    // Start / stop locator
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    WANTestBase.createFirstRemoteLocator(2, port);
    disconnectFromDS();

    // Validate active thread count after test

    // Wait up to 60 seconds for all threads started during the test
    // (including the 'WAN Locator Discovery Thread') to stop
    // Note: Awaitility is not being used since it adds threads
    for (int i = 0; i < 60; i++) {
      if (Thread.activeCount() > activeThreadCountBefore) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          fail("Caught the following exception waiting for threads to stop: " + e);
        }
      } else {
        break;
      }
    }

    // Fail if the active thread count after the test is greater than the active thread count before
    // the test
    if (Thread.activeCount() > activeThreadCountBefore) {
      OSProcess.printStacks(0);
      String builder = "Expected " + activeThreadCountBefore + " threads but found "
          + Thread.activeCount() + ". Check log file for a thread dump.";
      fail(builder);
    }
  }

  @Test
  public void testNoRemoteLocators() {
    IgnoredException ie = IgnoredException
        .addIgnoredException("could not get remote locator information for remote site");
    try {
      testRemoteLocators(null, false, 0);
    } finally {
      ie.remove();
    }
  }

  @Test
  public void testValidHostRemoteLocators() {
    Set<String> remoteLocators = new HashSet<>();
    remoteLocators.add("localhost[12345]");
    testRemoteLocators(remoteLocators, true, 1);
  }

  // pool has been created even though locator address is not valid
  @Test
  public void testInvalidHostRemoteLocators() {
    IgnoredException ie = IgnoredException
        .addIgnoredException("could not get remote locator information for remote site");
    try {
      Set<String> remoteLocators = new HashSet<>();
      addUnknownHost(remoteLocators);
      // now we don't validata address upfront
      testRemoteLocators(remoteLocators, true, 1);
    } finally {
      ie.remove();
    }
  }

  @Test
  public void testValidAndInvalidHostRemoteLocators() {
    Set<String> remoteLocators = new HashSet<>();
    remoteLocators.add("localhost[12345]");
    addUnknownHost(remoteLocators);
    // now we add the locator to pool, because we don't validate locator address
    testRemoteLocators(remoteLocators, true, 2);
  }

  private void addUnknownHost(Set<String> remoteLocators) {
    String unknownHostName = "unknownGeodeHostWanAutoDiscoveryDUnitTest";
    boolean unknownHostFound = false;
    int numTries = 10;
    for (int i = 0; i < numTries; i++) {
      try {
        InetAddress.getByName(unknownHostName);
      } catch (UnknownHostException e) {
        unknownHostFound = true;
        break;
      }
      unknownHostName = "_" + unknownHostName + "_";
    }
    assertTrue("An unknown host name could not be found in " + numTries + " tries",
        unknownHostFound);

    remoteLocators.add(unknownHostName + "[12345]");
  }

  private void testRemoteLocators(Set<String> remoteLocators, boolean poolShouldExist,
      int expectedPoolLocatorsSize) {
    // Start locator
    Integer lnLocPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    // Add remote locators
    int remoteDsId = 2;
    vm0.invoke(() -> WANTestBase.putRemoteSiteLocators(remoteDsId, remoteLocators));

    // Create cache
    vm2.invoke(() -> WANTestBase.createCache(lnLocPort));

    // Create sender
    vm2.invoke(() -> WANTestBase.createSender("ln", remoteDsId, false, 100, 10, false, false, null,
        false));

    // Verify sender is running
    vm2.invoke(() -> WANTestBase.verifySenderRunningState("ln"));

    // Verify pool exists or not
    vm2.invoke(() -> WANTestBase.verifyPool("ln", poolShouldExist, expectedPoolLocatorsSize));
  }
}
