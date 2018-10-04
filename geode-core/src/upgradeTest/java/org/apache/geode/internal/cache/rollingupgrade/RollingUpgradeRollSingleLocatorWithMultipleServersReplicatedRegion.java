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
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;

public class RollingUpgradeRollSingleLocatorWithMultipleServersReplicatedRegion
    extends RollingUpgrade2DUnitTestBase {

  @Test
  public void testRollSingleLocatorWithMultipleServersReplicatedRegion() throws Exception {
    final Host host = Host.getHost(0);
    VM locator = host.getVM(oldVersion, 0);
    VM server2 = host.getVM(oldVersion, 1);
    VM server3 = host.getVM(oldVersion, 2);

    final String objectType = "strings";
    final String regionName = "aRegion";

    RegionShortcut shortcut = RegionShortcut.REPLICATE;

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    DistributedTestUtils.deleteLocatorStateFile(locatorPorts[0]);

    // configure all class loaders for each vm

    String hostName = NetworkUtils.getServerHostName();
    String locatorString = getLocatorString(locatorPorts);
    try {
      locator.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorString), true));

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      locator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server2, server3);

      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), server2, server3);

      putAndVerify(objectType, server2, regionName, 0, 10, server3);
      locator = rollLocatorToCurrent(locator, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);

      server2 = rollServerToCurrentAndCreateRegion(server2, shortcut, regionName, locatorPorts);
      putAndVerify(objectType, server2, regionName, 5, 15, server3);
      putAndVerify(objectType, server3, regionName, 10, 20, server2);

      server3 = rollServerToCurrentAndCreateRegion(server3, shortcut, regionName, locatorPorts);
      putAndVerify(objectType, server2, regionName, 15, 25, server3);
      putAndVerify(objectType, server3, regionName, 20, 30, server2);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server2, server3);
    }
  }


}
