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

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;

public class RollingUpgradeRollLocatorsWithOldServer extends RollingUpgrade2DUnitTestBase {


  /**
   * starts 3 locators and 1 server rolls all 3 locators and then the server
   */
  @Test
  public void testRollLocatorsWithOldServer() {
    final Host host = Host.getHost(0);
    VM locator1 = host.getVM(oldVersion, 0);
    VM locator2 = host.getVM(oldVersion, 1);
    VM server4 = host.getVM(oldVersion, 3);

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

    String hostName = NetworkUtils.getServerHostName();
    String locatorString = getLocatorString(locatorPorts);
    try {
      locator1.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorString), true));

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      locator1.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      locator2.invoke(invokeStartLocator(hostName, locatorPorts[1], getTestMethodName(),
          getLocatorProperties(locatorString), false));

      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server4);

      locator1 = rollLocatorToCurrent(locator1, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);
      locator2 = rollLocatorToCurrent(locator2, hostName, locatorPorts[1], getTestMethodName(),
          locatorString);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator1, locator2);
      invokeRunnableInVMs(true, invokeCloseCache(), server4);
    }
  }

}
