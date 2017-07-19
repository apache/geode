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
package org.apache.geode.cache.wan;

import static org.junit.Assert.assertEquals;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.parallel.BatchRemovalThreadHelper;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Category({DistributedTest.class, BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class WANRollingUpgradeDUnitTest extends JUnit4CacheTestCase {

  @Parameterized.Parameters
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    } else {
      System.out.println("running against these versions: " + result);
    }
    return result;
  }

  // the old version of Geode we're testing against
  private String oldVersion;

  public WANRollingUpgradeDUnitTest(String version) {
    oldVersion = version;
  }

  @Test
  // This test verifies that a GatewaySenderProfile serializes properly between versions.
  public void testVerifyGatewaySenderProfile() throws Exception {
    final Host host = Host.getHost(0);
    VM oldLocator = host.getVM(oldVersion, 0);
    VM oldServer = host.getVM(oldVersion, 1);
    VM currentServer = host.getVM(2);

    // Start locator
    final int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port);
    final String locators = NetworkUtils.getServerHostName(host) + "[" + port + "]";
    oldLocator.invoke(() -> startLocator(port, 0, locators, ""));

    IgnoredException ie =
        IgnoredException.addIgnoredException("could not get remote locator information");
    try {
      // Start old server
      oldServer.invoke(() -> createCache(locators));

      // Create GatewaySender in old server
      String senderId = getName() + "_gatewaysender";
      oldServer.invoke(() -> createGatewaySender(senderId, 10,
          ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL));

      // Start current server
      currentServer.invoke(() -> createCache(locators));

      // Attempt to create GatewaySender in new server
      currentServer.invoke(() -> createGatewaySender(senderId, 10,
          ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL));
    } finally {
      ie.remove();
    }
  }

  @Test
  public void testEventProcessingOldSiteOneCurrentSiteTwo() throws Exception {
    final Host host = Host.getHost(0);

    // Get old site members
    VM site1Locator = host.getVM(oldVersion, 0);
    VM site1Server1 = host.getVM(oldVersion, 1);
    VM site1Server2 = host.getVM(oldVersion, 2);
    VM site1Client = host.getVM(oldVersion, 3);

    // Get current site members
    VM site2Locator = host.getVM(4);
    VM site2Server1 = host.getVM(5);
    VM site2Server2 = host.getVM(6);
    VM site2Client = host.getVM(7);

    // Get old site locator properties
    String hostName = NetworkUtils.getServerHostName(host);
    final int site1LocatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(site1LocatorPort);
    final String site1Locators = hostName + "[" + site1LocatorPort + "]";
    final int site1DistributedSystemId = 0;

    // Get current site locator properties
    final int site2LocatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(site2LocatorPort);
    final String site2Locators = hostName + "[" + site2LocatorPort + "]";
    final int site2DistributedSystemId = 1;

    // Start old site locator
    site1Locator.invoke(() -> startLocator(site1LocatorPort, site1DistributedSystemId,
        site1Locators, site2Locators));

    // Start current site locator
    site2Locator.invoke(() -> startLocator(site2LocatorPort, site2DistributedSystemId,
        site2Locators, site1Locators));

    // Start and configure old site servers
    String regionName = getName() + "_region";
    String site1SenderId = getName() + "_gatewaysender_" + site2DistributedSystemId;
    startAndConfigureServers(site1Server1, site1Server2, site1Locators, site2DistributedSystemId,
        regionName, site1SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);

    // Start and configure current site servers
    String site2SenderId = getName() + "_gatewaysender_" + site1DistributedSystemId;
    startAndConfigureServers(site2Server1, site2Server2, site2Locators, site1DistributedSystemId,
        regionName, site2SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);

    // Do puts from old site client and verify events on current site
    int numPuts = 100;
    doClientPutsAndVerifyEvents(site1Client, site1Server1, site1Server2, site2Server1, site2Server2,
        hostName, site1LocatorPort, regionName, numPuts, site1SenderId, false);

    // Do puts from current site client and verify events on old site
    doClientPutsAndVerifyEvents(site2Client, site2Server1, site2Server2, site1Server1, site1Server2,
        hostName, site2LocatorPort, regionName, numPuts, site2SenderId, false);

    // Do puts from old client in the current site and verify events on old site
    site1Client.invoke(() -> closeCache());
    doClientPutsAndVerifyEvents(site1Client, site2Server1, site2Server2, site1Server1, site1Server2,
        hostName, site2LocatorPort, regionName, numPuts, site2SenderId, false);
  }

  @Test
  public void testSecondaryEventsNotReprocessedAfterOldSiteMemberFailover() throws Exception {
    final Host host = Host.getHost(0);

    // Get old site members
    VM site1Locator = host.getVM(oldVersion, 0);
    VM site1Server1 = host.getVM(oldVersion, 1);
    VM site1Server2 = host.getVM(oldVersion, 2);
    VM site1Client = host.getVM(oldVersion, 3);

    // Get current site members
    VM site2Locator = host.getVM(4);
    VM site2Server1 = host.getVM(5);
    VM site2Server2 = host.getVM(6);

    // Get old site locator properties
    String hostName = NetworkUtils.getServerHostName(host);
    final int site1LocatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(site1LocatorPort);
    final String site1Locators = hostName + "[" + site1LocatorPort + "]";
    final int site1DistributedSystemId = 0;

    // Get current site locator properties
    final int site2LocatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(site2LocatorPort);
    final String site2Locators = hostName + "[" + site2LocatorPort + "]";
    final int site2DistributedSystemId = 1;

    // Start old site locator
    site1Locator.invoke(() -> startLocator(site1LocatorPort, site1DistributedSystemId,
        site1Locators, site2Locators));

    // Start current site locator
    site2Locator.invoke(() -> startLocator(site2LocatorPort, site2DistributedSystemId,
        site2Locators, site1Locators));

    try {
      // Start and configure old site servers with secondary removals prevented
      String regionName = getName() + "_region";
      String site1SenderId = getName() + "_gatewaysender_" + site2DistributedSystemId;
      startAndConfigureServers(site1Server1, site1Server2, site1Locators, site2DistributedSystemId,
          regionName, site1SenderId, Integer.MAX_VALUE);

      // Start and configure current site servers with secondary removals prevented
      String site2SenderId = getName() + "_gatewaysender_" + site1DistributedSystemId;
      startAndConfigureServers(site2Server1, site2Server2, site2Locators, site1DistributedSystemId,
          regionName, site2SenderId, Integer.MAX_VALUE);

      // Do puts from old site client and verify events on current site
      int numPuts = 100;
      doClientPutsAndVerifyEvents(site1Client, site1Server1, site1Server2, site2Server1,
          site2Server2, hostName, site1LocatorPort, regionName, numPuts, site1SenderId, true);

      // Stop one sender in the old site and verify the other resends its events and that those
      // events
      // are ignored on the current site
      stopSenderAndVerifyEvents(site1Server1, site1Server2, site2Server1, site2Server2,
          site1SenderId, regionName, numPuts);
    } finally {
      resetAllMessageSyncIntervals(site1Server1, site1Server2, site2Server1, site2Server2);
    }
  }

  @Test
  public void testSecondaryEventsNotReprocessedAfterCurrentSiteMemberFailover() throws Exception {
    final Host host = Host.getHost(0);

    // Get old site members
    VM site1Locator = host.getVM(oldVersion, 0);
    VM site1Server1 = host.getVM(oldVersion, 1);
    VM site1Server2 = host.getVM(oldVersion, 2);

    // Get current site members
    VM site2Locator = host.getVM(4);
    VM site2Server1 = host.getVM(5);
    VM site2Server2 = host.getVM(6);
    VM site2Client = host.getVM(7);

    // Get old site locator properties
    String hostName = NetworkUtils.getServerHostName(host);
    final int site1LocatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(site1LocatorPort);
    final String site1Locators = hostName + "[" + site1LocatorPort + "]";
    final int site1DistributedSystemId = 0;

    // Get current site locator properties
    final int site2LocatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(site2LocatorPort);
    final String site2Locators = hostName + "[" + site2LocatorPort + "]";
    final int site2DistributedSystemId = 1;

    // Start old site locator
    site1Locator.invoke(() -> startLocator(site1LocatorPort, site1DistributedSystemId,
        site1Locators, site2Locators));

    // Start current site locator
    site2Locator.invoke(() -> startLocator(site2LocatorPort, site2DistributedSystemId,
        site2Locators, site1Locators));

    try {
      // Start and configure old site servers with secondary removals prevented
      String regionName = getName() + "_region";
      String site1SenderId = getName() + "_gatewaysender_" + site2DistributedSystemId;
      startAndConfigureServers(site1Server1, site1Server2, site1Locators, site2DistributedSystemId,
          regionName, site1SenderId, Integer.MAX_VALUE);

      // Start and configure current site servers with secondary removals prevented
      String site2SenderId = getName() + "_gatewaysender_" + site1DistributedSystemId;
      startAndConfigureServers(site2Server1, site2Server2, site2Locators, site1DistributedSystemId,
          regionName, site2SenderId, Integer.MAX_VALUE);

      // Do puts from current site client and verify events on old site
      int numPuts = 100;
      doClientPutsAndVerifyEvents(site2Client, site2Server1, site2Server2, site1Server1,
          site1Server2, hostName, site2LocatorPort, regionName, numPuts, site2SenderId, true);

      // Stop one sender in the current site and verify the other resends its events and that those
      // events are ignored on the old site
      stopSenderAndVerifyEvents(site2Server1, site2Server2, site1Server1, site1Server2,
          site2SenderId, regionName, numPuts);
    } finally {
      resetAllMessageSyncIntervals(site1Server1, site1Server2, site2Server1, site2Server2);
    }
  }

  @Test
  public void testSecondaryEventsNotReprocessedAfterCurrentSiteMemberFailoverWithOldClient()
      throws Exception {
    final Host host = Host.getHost(0);

    // Get old site members
    VM site1Locator = host.getVM(oldVersion, 0);
    VM site1Server1 = host.getVM(oldVersion, 1);
    VM site1Server2 = host.getVM(oldVersion, 2);
    VM site1Client = host.getVM(oldVersion, 3);

    // Get current site members
    VM site2Locator = host.getVM(4);
    VM site2Server1 = host.getVM(5);
    VM site2Server2 = host.getVM(6);

    // Get old site locator properties
    String hostName = NetworkUtils.getServerHostName(host);
    final int site1LocatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(site1LocatorPort);
    final String site1Locators = hostName + "[" + site1LocatorPort + "]";
    final int site1DistributedSystemId = 0;

    // Get current site locator properties
    final int site2LocatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(site2LocatorPort);
    final String site2Locators = hostName + "[" + site2LocatorPort + "]";
    final int site2DistributedSystemId = 1;

    // Start old site locator
    site1Locator.invoke(() -> startLocator(site1LocatorPort, site1DistributedSystemId,
        site1Locators, site2Locators));

    // Start current site locator
    site2Locator.invoke(() -> startLocator(site2LocatorPort, site2DistributedSystemId,
        site2Locators, site1Locators));

    try {
      // Start and configure old site servers with secondary removals prevented
      String regionName = getName() + "_region";
      String site1SenderId = getName() + "_gatewaysender_" + site2DistributedSystemId;
      startAndConfigureServers(site1Server1, site1Server2, site1Locators, site2DistributedSystemId,
          regionName, site1SenderId, Integer.MAX_VALUE);

      // Start and configure current site servers with secondary removals prevented
      String site2SenderId = getName() + "_gatewaysender_" + site1DistributedSystemId;
      startAndConfigureServers(site2Server1, site2Server2, site2Locators, site1DistributedSystemId,
          regionName, site2SenderId, Integer.MAX_VALUE);

      // Do puts from old client in the current site and verify events on old site
      int numPuts = 100;
      doClientPutsAndVerifyEvents(site1Client, site2Server1, site2Server2, site1Server1,
          site1Server2, hostName, site2LocatorPort, regionName, numPuts, site2SenderId, true);

      // Stop one sender in the current site and verify the other resends its events and that those
      // events are ignored on the remote site
      stopSenderAndVerifyEvents(site2Server1, site2Server2, site1Server1, site1Server2,
          site2SenderId, regionName, numPuts);
    } finally {
      resetAllMessageSyncIntervals(site1Server1, site1Server2, site2Server1, site2Server2);
    }
  }

  @Test
  public void testEventProcessingMixedSiteOneOldSiteTwo() throws Exception {
    final Host host = Host.getHost(0);

    // Get mixed site members
    VM site1Locator = host.getVM(oldVersion, 0);
    VM site1Server1 = host.getVM(oldVersion, 1);
    VM site1Server2 = host.getVM(2);
    VM site1Client = host.getVM(oldVersion, 3);

    // Get old site members
    VM site2Locator = host.getVM(oldVersion, 4);
    VM site2Server1 = host.getVM(oldVersion, 5);
    VM site2Server2 = host.getVM(oldVersion, 6);

    // Get mixed site locator properties
    String hostName = NetworkUtils.getServerHostName(host);
    final int site1LocatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(site1LocatorPort);
    final String site1Locators = hostName + "[" + site1LocatorPort + "]";
    final int site1DistributedSystemId = 0;

    // Get old site locator properties
    final int site2LocatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(site2LocatorPort);
    final String site2Locators = hostName + "[" + site2LocatorPort + "]";
    final int site2DistributedSystemId = 1;

    // Start mixed site locator
    site1Locator.invoke(() -> startLocator(site1LocatorPort, site1DistributedSystemId,
        site1Locators, site2Locators));

    // Start old site locator
    site2Locator.invoke(() -> startLocator(site2LocatorPort, site2DistributedSystemId,
        site2Locators, site1Locators));

    // Start and configure mixed site servers
    String regionName = getName() + "_region";
    String site1SenderId = getName() + "_gatewaysender_" + site2DistributedSystemId;
    startAndConfigureServers(site1Server1, site1Server2, site1Locators, site2DistributedSystemId,
        regionName, site1SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);

    // Start and configure old site servers
    String site2SenderId = getName() + "_gatewaysender_" + site1DistributedSystemId;
    startAndConfigureServers(site2Server1, site2Server2, site2Locators, site1DistributedSystemId,
        regionName, site2SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);

    // Do puts from mixed site client and verify events on old site
    int numPuts = 100;
    doClientPutsAndVerifyEvents(site1Client, site1Server1, site1Server2, site2Server1, site2Server2,
        hostName, site1LocatorPort, regionName, numPuts, site1SenderId, false);
  }

  @Test
  public void testEventProcessingMixedSiteOneCurrentSiteTwo() throws Exception {
    final Host host = Host.getHost(0);

    // Get mixed site members
    VM site1Locator = host.getVM(oldVersion, 0);
    VM site1Server1 = host.getVM(oldVersion, 1);
    VM site1Server2 = host.getVM(2);
    VM site1Client = host.getVM(oldVersion, 3);

    // Get old site members
    VM site2Locator = host.getVM(4);
    VM site2Server1 = host.getVM(5);
    VM site2Server2 = host.getVM(6);

    // Get mixed site locator properties
    String hostName = NetworkUtils.getServerHostName(host);
    final int site1LocatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(site1LocatorPort);
    final String site1Locators = hostName + "[" + site1LocatorPort + "]";
    final int site1DistributedSystemId = 0;

    // Get old site locator properties
    final int site2LocatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(site2LocatorPort);
    final String site2Locators = hostName + "[" + site2LocatorPort + "]";
    final int site2DistributedSystemId = 1;

    // Start mixed site locator
    site1Locator.invoke(() -> startLocator(site1LocatorPort, site1DistributedSystemId,
        site1Locators, site2Locators));

    // Start old site locator
    site2Locator.invoke(() -> startLocator(site2LocatorPort, site2DistributedSystemId,
        site2Locators, site1Locators));

    // Start and configure mixed site servers
    String regionName = getName() + "_region";
    String site1SenderId = getName() + "_gatewaysender_" + site2DistributedSystemId;
    startAndConfigureServers(site1Server1, site1Server2, site1Locators, site2DistributedSystemId,
        regionName, site1SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);

    // Start and configure old site servers
    String site2SenderId = getName() + "_gatewaysender_" + site1DistributedSystemId;
    startAndConfigureServers(site2Server1, site2Server2, site2Locators, site1DistributedSystemId,
        regionName, site2SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);

    // Do puts from mixed site client and verify events on old site
    int numPuts = 100;
    doClientPutsAndVerifyEvents(site1Client, site1Server1, site1Server2, site2Server1, site2Server2,
        hostName, site1LocatorPort, regionName, numPuts, site1SenderId, false);
  }

  private void startLocator(int port, int distributedSystemId, String locators,
      String remoteLocators) throws IOException {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME,
        String.valueOf(distributedSystemId));
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    props.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, remoteLocators);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, DUnitLauncher.logLevel);
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    Locator.startLocatorAndDS(port, null, props);
  }

  private void startAndConfigureServers(VM server1, VM server2, String locators,
      int distributedSystem, String regionName, String senderId, int messageSyncInterval) {
    // Start and configure servers
    // - Create Cache
    // - Create CacheServer
    // - Create GatewaySender
    // - Create GatewayReceiver
    // - Create Region

    // Start and configure server 1
    server1.invoke(() -> createCache(locators));
    server1.invoke(() -> addCacheServer());
    server1.invoke(() -> createGatewaySender(senderId, distributedSystem, messageSyncInterval));
    server1.invoke(() -> createGatewayReceiver());
    server1.invoke(() -> createPartitionedRegion(regionName, senderId));

    // Start and configure server 2 if necessary
    if (server2 != null) {
      server2.invoke(() -> createCache(locators));
      server2.invoke(() -> addCacheServer());
      server2.invoke(() -> createGatewaySender(senderId, distributedSystem, messageSyncInterval));
      server2.invoke(() -> createGatewayReceiver());
      server2.invoke(() -> createPartitionedRegion(regionName, senderId));
    }
  }

  private void doClientPutsAndVerifyEvents(VM client, VM localServer1, VM localServer2,
      VM remoteServer1, VM remoteServer2, String hostName, int locatorPort, String regionName,
      int numPuts, String senderId, boolean primaryOnly) {
    // Start client
    client.invoke(() -> startClient(hostName, locatorPort, regionName));

    // Do puts from client
    client.invoke(() -> doPuts(regionName, numPuts));

    // Wait for local site queues to be empty
    localServer1.invoke(() -> waitForEmptyQueueRegion(senderId, primaryOnly));
    localServer2.invoke(() -> waitForEmptyQueueRegion(senderId, primaryOnly));

    // Verify remote site received events
    int remoteServer1EventsReceived = remoteServer1.invoke(() -> getEventsReceived(regionName));
    int remoteServer2EventsReceived = remoteServer2.invoke(() -> getEventsReceived(regionName));
    assertEquals(numPuts, remoteServer1EventsReceived + remoteServer2EventsReceived);

    // Clear events received in both sites
    localServer1.invoke(() -> clearEventsReceived(regionName));
    localServer2.invoke(() -> clearEventsReceived(regionName));
    remoteServer1.invoke(() -> clearEventsReceived(regionName));
    remoteServer2.invoke(() -> clearEventsReceived(regionName));
  }

  private void stopSenderAndVerifyEvents(VM localServer1, VM localServer2, VM remoteServer1,
      VM remoteServer2, String senderId, String regionName, int numPuts) {
    // Verify the secondary events still exist
    int localServer1QueueSize = localServer1.invoke(() -> getQueueRegionSize(senderId, false));
    int localServer2QueueSize = localServer2.invoke(() -> getQueueRegionSize(senderId, false));
    assertEquals(numPuts, localServer1QueueSize + localServer2QueueSize);

    // Stop one sender
    localServer1.invoke(() -> closeCache());

    // Wait for the other sender's queue to be empty
    localServer2.invoke(() -> waitForEmptyQueueRegion(senderId, false));

    // Verify remote site did not receive any events. The events received were previously cleared on
    // all members, so there should be 0 events received on the remote site.
    int remoteServer1EventsReceived = remoteServer1.invoke(() -> getEventsReceived(regionName));
    int remoteServer2EventsReceived = remoteServer2.invoke(() -> getEventsReceived(regionName));
    assertEquals(0, remoteServer1EventsReceived + remoteServer2EventsReceived);
  }

  private void createCache(String locators) {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, DUnitLauncher.logLevel);
    getCache(props);
  }

  private void addCacheServer() throws Exception {
    CacheServer server = getCache().addCacheServer();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    server.setPort(port);
    server.start();
  }

  private void startClient(String hostName, int locatorPort, String regionName) {
    ClientCacheFactory ccf = new ClientCacheFactory().addPoolLocator(hostName, locatorPort);
    ClientCache cache = getClientCache(ccf);
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
  }

  private void createGatewaySender(String id, int remoteDistributedSystemId,
      int messageSyncInterval) throws Exception {
    // Setting the messageSyncInterval controls how often the BatchRemovalThread sends processed
    // events from the primary to the secondary. Setting it high prevents the events from being
    // removed from the secondary.
    BatchRemovalThreadHelper.setMessageSyncInterval(messageSyncInterval);
    GatewaySenderFactory gsf = getCache().createGatewaySenderFactory();
    gsf.setParallel(true);
    gsf.create(id, remoteDistributedSystemId);
  }

  private void resetAllMessageSyncIntervals(VM site1Server1, VM site1Server2, VM site2Server1,
      VM site2Server2) {
    site1Server1.invoke(() -> resetMessageSyncInterval());
    site1Server2.invoke(() -> resetMessageSyncInterval());
    site2Server1.invoke(() -> resetMessageSyncInterval());
    site2Server2.invoke(() -> resetMessageSyncInterval());
  }

  private void resetMessageSyncInterval() {
    BatchRemovalThreadHelper
        .setMessageSyncInterval(ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
  }

  private void createGatewayReceiver() {
    getCache().createGatewayReceiverFactory().create();
  }

  private void createPartitionedRegion(String regionName, String gatewaySenderId) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setTotalNumBuckets(10);
    getCache().createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
        .addCacheListener(new EventCountCacheListener()).addGatewaySenderId(gatewaySenderId)
        .setPartitionAttributes(paf.create()).create(regionName);
  }

  private void doPuts(String regionName, int numPuts) {
    Region region = getCache().getRegion(regionName);
    for (int i = 0; i < numPuts; i++) {
      region.put(i, i);
    }
  }

  private void waitForEmptyQueueRegion(String gatewaySenderId, boolean primaryOnly)
      throws Exception {
    Awaitility.await().atMost(60, TimeUnit.SECONDS)
        .until(() -> getQueueRegionSize(gatewaySenderId, primaryOnly) == 0);
  }

  private int getQueueRegionSize(String gatewaySenderId, boolean primaryOnly) throws Exception {
    // This method currently only supports parallel senders. It gets the size of the local data set
    // from the
    // underlying colocated region. Depending on the value of primaryOnly, it gets either the local
    // primary data set (just primary buckets) or all local data set (primary and secondary
    // buckets).
    AbstractGatewaySender ags =
        (AbstractGatewaySender) getCache().getGatewaySender(gatewaySenderId);
    ConcurrentParallelGatewaySenderQueue prq =
        (ConcurrentParallelGatewaySenderQueue) ags.getQueues().iterator().next();
    Region region = prq.getRegion();
    Region localDataSet = primaryOnly ? PartitionRegionHelper.getLocalPrimaryData(region)
        : PartitionRegionHelper.getLocalData(region);
    return localDataSet.size();
  }

  private Integer getEventsReceived(String regionName) {
    Region region = getCache().getRegion(regionName);
    EventCountCacheListener cl =
        (EventCountCacheListener) region.getAttributes().getCacheListener();
    return cl.getEventsReceived();
  }

  private void clearEventsReceived(String regionName) {
    Region region = getCache().getRegion(regionName);
    EventCountCacheListener cl =
        (EventCountCacheListener) region.getAttributes().getCacheListener();
    cl.clearEventsReceived();
  }

  private static class EventCountCacheListener extends CacheListenerAdapter {

    private AtomicInteger eventsReceived = new AtomicInteger();

    public void afterCreate(EntryEvent event) {
      process(event);
    }

    public void afterUpdate(EntryEvent event) {
      process(event);
    }

    private void process(EntryEvent event) {
      incrementEventsReceived();
    }

    private int incrementEventsReceived() {
      return this.eventsReceived.incrementAndGet();
    }

    private int getEventsReceived() {
      return this.eventsReceived.get();
    }

    private void clearEventsReceived() {
      this.eventsReceived.set(0);
    }
  }
}
