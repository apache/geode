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
import static org.junit.Assert.fail;

import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;

public class RollingUpgradeConcurrentPutsReplicated extends RollingUpgrade2DUnitTestBase {

  /**
   * Starts 2 servers with old classloader puts in one server while the other bounces verifies
   * values are present in bounced server puts in the newly started/bounced server and bounces the
   * other server verifies values are present in newly bounced server
   */
  @Test
  public void testConcurrentPutsReplicated() {
    Host host = Host.getHost(0);
    VM locator = host.getVM(oldVersion, 1);
    VM server1 = host.getVM(oldVersion, 2);
    VM server2 = host.getVM(oldVersion, 3);

    final String objectType = "strings";
    final String regionName = "aRegion";

    RegionShortcut shortcut = RegionShortcut.REPLICATE;

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    String hostName = NetworkUtils.getServerHostName();
    String locatorString = getLocatorString(locatorPorts);

    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

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

      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server1, server2);
      // invokeRunnableInVMs(invokeAssertVersion(oldOrdinal), server1, server2);
      // create region
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), server1, server2);

      // async puts through server 2
      AsyncInvocation asyncPutsThroughOld =
          server2.invokeAsync(new CacheSerializableRunnable("async puts") {
            public void run2() {
              try {
                for (int i = 0; i < 500; i++) {
                  put(RollingUpgrade2DUnitTestBase.cache, regionName, "" + i, "VALUE(" + i + ")");
                }
              } catch (Exception e) {
                fail("error putting");
              }
            }
          });
      locator = rollLocatorToCurrent(locator, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);

      server1 = rollServerToCurrentAndCreateRegion(server1, shortcut, regionName, locatorPorts);
      ThreadUtils.join(asyncPutsThroughOld, 30000);

      // verifyValues in server1
      verifyValues(objectType, regionName, 0, 500, server1);

      // aync puts through server 1
      AsyncInvocation asyncPutsThroughNew =
          server1.invokeAsync(new CacheSerializableRunnable("async puts") {
            public void run2() {
              try {
                for (int i = 250; i < 750; i++) {
                  put(RollingUpgrade2DUnitTestBase.cache, regionName, "" + i, "VALUE(" + i + ")");
                }
              } catch (Exception e) {
                fail("error putting");
              }
            }
          });
      server2 = rollServerToCurrentAndCreateRegion(server2, shortcut, regionName, locatorPorts);
      ThreadUtils.join(asyncPutsThroughNew, 30000);

      // verifyValues in server2
      verifyValues(objectType, regionName, 250, 750, server2);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2);
    }
  }


}
