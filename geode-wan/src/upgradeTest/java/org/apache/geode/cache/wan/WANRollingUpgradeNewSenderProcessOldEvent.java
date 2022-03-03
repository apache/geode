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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

public class WANRollingUpgradeNewSenderProcessOldEvent
    extends WANRollingUpgradeDUnitTest {
  final Host host = Host.getHost(0);

  private VM site1Locator;
  private VM site1Server1;
  private VM site1Server2;
  private VM site1Client;

  private VM site2Locator;
  private VM site2Server1;
  private VM site2Server2;

  private String site1Locators;
  private String site2Locators;
  private String site1SenderId;
  private String site2SenderId;
  private String regionName;

  final int numPuts = 100;
  final int site1DistributedSystemId = 0;
  final int site2DistributedSystemId = 1;
  int site1LocatorPort;
  int site2LocatorPort;

  @Before
  public void prepare() {
    // Get mixed site members
    site1Locator = host.getVM(oldVersion, 0);
    site1Server1 = host.getVM(oldVersion, 1);
    site1Server2 = host.getVM(oldVersion, 2);
    site1Client = host.getVM(oldVersion, 3);

    // Get old site members
    site2Locator = host.getVM(oldVersion, 4);
    site2Server1 = host.getVM(oldVersion, 5);
    site2Server2 = host.getVM(oldVersion, 6);

    // Get mixed site locator properties
    String hostName = NetworkUtils.getServerHostName(host);
    final int[] availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    site1LocatorPort = availablePorts[0];
    site1Locator.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(site1LocatorPort));
    site1Locators = hostName + "[" + site1LocatorPort + "]";

    // Get old site locator properties
    site2LocatorPort = availablePorts[1];
    site2Locator.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(site2LocatorPort));
    site2Locators = hostName + "[" + site2LocatorPort + "]";

    // Start mixed site locator
    site1Locator.invoke(() -> startLocator(site1LocatorPort, site1DistributedSystemId,
        site1Locators, site2Locators));

    // Start old site locator
    site2Locator.invoke(() -> startLocator(site2LocatorPort, site2DistributedSystemId,
        site2Locators, site1Locators));

    // Locators before 1.4 handled configuration asynchronously.
    // We must wait for configuration configuration to be ready, or confirm that it is disabled.
    site1Locator.invoke(
        () -> await()
            .untilAsserted(() -> assertTrue(
                !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                    || InternalLocator.getLocator().isSharedConfigurationRunning())));
    site2Locator.invoke(
        () -> await()
            .untilAsserted(() -> assertTrue(
                !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                    || InternalLocator.getLocator().isSharedConfigurationRunning())));

    // Start and configure mixed site servers
    regionName = getName() + "_region";
    site1SenderId = getName() + "_gatewaysender_" + site2DistributedSystemId;
    startAndConfigureServers(site1Server1, site1Server2, site1Locators, site2DistributedSystemId,
        regionName, site1SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);

    site2SenderId = getName() + "_gatewaysender_" + site1DistributedSystemId;

    // pause the senders at mixed site
    site1Server1.invoke(() -> pauseSender(site1SenderId));
    site1Server2.invoke(() -> pauseSender(site1SenderId));

    // start client
    site1Client.invoke(() -> startClient(hostName, site1LocatorPort, regionName));

    // Do puts from client
    site1Client.invoke(() -> doPuts(regionName, numPuts));

    // check queue size should be numPuts
    site1Server1.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, numPuts, false));
    site1Server2.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, numPuts, false));
  }

  /**
   * 1.create a sender, pause
   * 2.do puts, build up the queue.
   * 3.roll one server to current and stop another old server
   * 4.start sender on current
   * Current sender should handle old events, the remove site should receive all of them
   */
  @Test
  public void oldEventShouldBeProcessedAtNewSender() {
    IgnoredException.addIgnoredException("This is normal if the locator was shutdown.");
    // Roll mixed site locator to current
    rollLocatorToCurrent(site1Locator, site1LocatorPort, site1DistributedSystemId, site1Locators,
        site2Locators);

    // Roll mixed site servers to current
    rollStartAndConfigureServerToCurrent(site1Server1, site1Locators, site2DistributedSystemId,
        regionName, site1SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
    site1Server1.invoke(() -> pauseSender(site1SenderId));

    // double check queue size should be numPuts
    site1Server1.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, numPuts, false));
    site1Server2.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, numPuts, false));

    // Start and configure old site servers
    startAndConfigureServers(site2Server1, site2Server2, site2Locators, site1DistributedSystemId,
        regionName, site2SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);

    // stop one of the sender to force all the old events are processed by the new sender
    site1Server2.invoke(JUnit4CacheTestCase::closeCache);

    // double check queue size should be numPuts since all primary buckets are in this server
    site1Server1.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, numPuts, true));

    // resume the senders at mixed site
    site1Server1.invoke(() -> resumeSender(site1SenderId));

    // Wait for mixed site queues to be empty
    site1Server1.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, 0, false));

    // Verify remote site received events
    int remoteServer1EventsReceived = site2Server1.invoke(() -> getEventsReceived(regionName));
    int remoteServer2EventsReceived = site2Server2.invoke(() -> getEventsReceived(regionName));
    assertEquals(numPuts, remoteServer1EventsReceived + remoteServer2EventsReceived);

    // Clear events received in both sites
    site1Server1.invoke(() -> clearEventsReceived(regionName));
    site2Server1.invoke(() -> clearEventsReceived(regionName));
    site2Server2.invoke(() -> clearEventsReceived(regionName));
  }

  /**
   * 1.create a sender, pause
   * 2.do puts, build up the queue.
   * 3.roll all old server to current
   * 4.start senders on current
   * Current sender should handle old events, the remove site should receive all of them
   */
  @Test
  public void oldEventShouldBeProcessedAtTwoNewSender() {
    IgnoredException.addIgnoredException("This is normal if the locator was shutdown.");
    // Roll mixed site locator to current
    rollLocatorToCurrent(site1Locator, site1LocatorPort, site1DistributedSystemId, site1Locators,
        site2Locators);

    // Roll mixed site servers to current
    rollStartAndConfigureServerToCurrent(site1Server1, site1Locators, site2DistributedSystemId,
        regionName, site1SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
    site1Server1.invoke(() -> pauseSender(site1SenderId));

    // wait until site1Server finished rolling
    site1Server1.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, numPuts, false));

    rollStartAndConfigureServerToCurrent(site1Server2, site1Locators, site2DistributedSystemId,
        regionName, site1SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
    site1Server2.invoke(() -> pauseSender(site1SenderId));

    // double check queue size should be numPuts
    site1Server1.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, numPuts, false));
    site1Server2.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, numPuts, false));

    // Start and configure old site servers
    startAndConfigureServers(site2Server1, site2Server2, site2Locators, site1DistributedSystemId,
        regionName, site2SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);

    // resume the senders at mixed site
    site1Server1.invoke(() -> resumeSender(site1SenderId));
    site1Server2.invoke(() -> resumeSender(site1SenderId));

    // Wait for mixed site queues to be empty
    site1Server1.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, 0, false));
    site1Server2.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, 0, false));

    // Verify remote site received events
    int remoteServer1EventsReceived = site2Server1.invoke(() -> getEventsReceived(regionName));
    int remoteServer2EventsReceived = site2Server2.invoke(() -> getEventsReceived(regionName));
    assertEquals(numPuts, remoteServer1EventsReceived + remoteServer2EventsReceived);

    // Clear events received in both sites
    site1Server1.invoke(() -> clearEventsReceived(regionName));
    site1Server2.invoke(() -> clearEventsReceived(regionName));
    site2Server1.invoke(() -> clearEventsReceived(regionName));
    site2Server2.invoke(() -> clearEventsReceived(regionName));
  }

  /**
   * get new events to flow back to the old member and have the old member try to deserialize
   * the new event
   * 1.create 2 old senders, pause
   * 2.do puts, build up the queue.
   * 3.roll one sender to current
   * 4.do additional puts that get queued in the new member with a redundant copy on the old member
   * 5.remove the new member
   * 6.start sender on old member
   *
   * The old sender should handle both old and new events, the remove site should receive all of
   * them
   */
  @Test
  public void bothOldAndNewEventsShouldBeProcessedByOldSender() {
    // Roll mixed site locator to current
    rollLocatorToCurrent(site1Locator, site1LocatorPort, site1DistributedSystemId, site1Locators,
        site2Locators);

    // Roll one mixed site server to current
    rollStartAndConfigureServerToCurrent(site1Server1, site1Locators, site2DistributedSystemId,
        regionName, site1SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
    site1Server1.invoke(() -> pauseSender(site1SenderId));

    // double check queue size should be numPuts
    site1Server1.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, numPuts, false));
    site1Server2.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, numPuts, false));

    // do additional puts, at least half of them will be put into new sender, then distributed to
    // the old sender
    site1Client.invoke(() -> {
      Region region = getCache().getRegion(regionName);
      for (int i = numPuts; i < numPuts * 2; i++) {
        region.put(i, i);
      }
    });

    // check queue size should be numPuts*2
    site1Server1.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, numPuts * 2, false));
    site1Server2.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, numPuts * 2, false));

    // Start and configure old site servers
    startAndConfigureServers(site2Server1, site2Server2, site2Locators, site1DistributedSystemId,
        regionName, site2SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);

    // stop the new sender to force all the old+new events are processed by the remained old sender
    site1Server1.invoke(JUnit4CacheTestCase::closeCache);

    // resume the senders at mixed site
    site1Server2.invoke(() -> resumeSender(site1SenderId));

    // Wait for mixed site queues to be empty
    site1Server2.invoke(() -> waitForQueueRegionToCertainSize(site1SenderId, 0, false));

    // Verify remote site received events
    int remoteServer1EventsReceived = site2Server1.invoke(() -> getEventsReceived(regionName));
    int remoteServer2EventsReceived = site2Server2.invoke(() -> getEventsReceived(regionName));
    assertEquals(numPuts * 2, remoteServer1EventsReceived + remoteServer2EventsReceived);

    // Clear events received in both sites
    site1Server2.invoke(() -> clearEventsReceived(regionName));
    site2Server1.invoke(() -> clearEventsReceived(regionName));
    site2Server2.invoke(() -> clearEventsReceived(regionName));
  }
}
