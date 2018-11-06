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
package org.apache.geode.internal.cache.rollingupgrade;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.VersionManager;

public class RollingUpgradeHARegionNameOnDifferentServerVersions
    extends RollingUpgrade2DUnitTestBase {

  @Test
  public void testHARegionNameOnDifferentServerVersions() {
    final Host host = Host.getHost(0);
    VM locator = host.getVM(oldVersion, 0);
    VM server1 = host.getVM(oldVersion, 1);
    VM server2 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    VM client = host.getVM(oldVersion, 3);

    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    int[] locatorPorts = new int[] {ports[0]};
    int[] csPorts = new int[] {ports[1], ports[2]};

    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

    String hostName = NetworkUtils.getServerHostName();
    String[] hostNames = new String[] {hostName};
    String locatorString = getLocatorString(locatorPorts);
    try {
      locator.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorString, false), true));

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      locator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server1, server2);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[0]), server1);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[1]), server2);

      invokeRunnableInVMs(
          invokeCreateClientCache(getClientSystemProperties(), hostNames, locatorPorts, true),
          client);

      // Get HARegion name on server1
      String server1HARegionName = server1.invoke(() -> getHARegionName());

      // Get HARegionName on server2
      String server2HARegionName = server2.invoke(() -> getHARegionName());

      // Verify they are equal
      assertEquals(server1HARegionName, server2HARegionName);
    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2, client);
    }
  }
}
