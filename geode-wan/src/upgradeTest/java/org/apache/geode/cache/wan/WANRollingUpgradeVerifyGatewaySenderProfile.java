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
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.VersionManager;

public class WANRollingUpgradeVerifyGatewaySenderProfile extends WANRollingUpgradeDUnitTest {
  @Test
  // This test verifies that a GatewaySenderProfile serializes properly between versions.
  public void testVerifyGatewaySenderProfile() {
    final Host host = Host.getHost(0);
    VM oldLocator = host.getVM(oldVersion, 0);
    VM oldServer = host.getVM(oldVersion, 1);
    VM currentServer = host.getVM(VersionManager.CURRENT_VERSION, 2);

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

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      oldLocator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

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
}
