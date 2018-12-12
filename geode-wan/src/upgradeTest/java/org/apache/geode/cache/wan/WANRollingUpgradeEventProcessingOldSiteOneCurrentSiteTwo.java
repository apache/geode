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
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.VersionManager;

public class WANRollingUpgradeEventProcessingOldSiteOneCurrentSiteTwo
    extends WANRollingUpgradeDUnitTest {

  @Test
  public void testEventProcessingOldSiteOneCurrentSiteTwo() {
    final Host host = Host.getHost(0);

    // Get old site members
    VM site1Locator = host.getVM(oldVersion, 0);
    VM site1Server1 = host.getVM(oldVersion, 1);
    VM site1Server2 = host.getVM(oldVersion, 2);
    VM site1Client = host.getVM(oldVersion, 3);

    // Get current site members
    VM site2Locator = host.getVM(VersionManager.CURRENT_VERSION, 4);
    VM site2Server1 = host.getVM(VersionManager.CURRENT_VERSION, 5);
    VM site2Server2 = host.getVM(VersionManager.CURRENT_VERSION, 6);
    VM site2Client = host.getVM(VersionManager.CURRENT_VERSION, 7);

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

    // Locators before 1.4 handled configuration asynchronously.
    // We must wait for configuration configuration to be ready, or confirm that it is disabled.
    site1Locator.invoke(
        () -> await()
            .untilAsserted(() -> assertTrue(
                !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                    || InternalLocator.getLocator().isSharedConfigurationRunning())));

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
}
