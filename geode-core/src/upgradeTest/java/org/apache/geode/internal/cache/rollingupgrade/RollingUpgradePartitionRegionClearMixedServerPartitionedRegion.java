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

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.version.VersionManager;

public class RollingUpgradePartitionRegionClearMixedServerPartitionedRegion
    extends RollingUpgrade2DUnitTestBase {


  @Test
  public void testPutAndGetMixedServerPartitionedRegion() throws Exception {
    doTestPutAndGetMixedServers("dataserializable", true, oldVersion);
  }

  /**
   * This test starts up multiple servers from the current code base and multiple servers from the
   * old version and executes puts and gets on a new server and old server and verifies that the
   * results are present. Note that the puts have overlapping region keys just to test new puts and
   * replaces
   */
  @Override
  void doTestPutAndGetMixedServers(String objectType, boolean partitioned, String oldVersion)
      throws Exception {
    VM currentServer1 = VM.getVM(VersionManager.CURRENT_VERSION, 0);
    VM oldServerAndLocator = VM.getVM(oldVersion, 1);
    VM currentServer2 = VM.getVM(VersionManager.CURRENT_VERSION, 2);
    VM oldServer2 = VM.getVM(oldVersion, 3);

    String regionName = "aRegion";

    RegionShortcut shortcut = RegionShortcut.REPLICATE;
    if (partitioned) {
      shortcut = RegionShortcut.PARTITION;
    }

    String serverHostName = NetworkUtils.getServerHostName();
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    oldServerAndLocator.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(port));
    try {
      Properties props = getSystemProperties();
      props.remove(DistributionConfig.LOCATORS_NAME);

      // Fire up the locator and server
      invokeRunnableInVMs(invokeStartLocatorAndServer(serverHostName, port, props),
          oldServerAndLocator);

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      oldServerAndLocator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      props.put(DistributionConfig.LOCATORS_NAME, serverHostName + "[" + port + "]");

      // create the cache in all the server VMs.
      invokeRunnableInVMs(invokeCreateCache(props), oldServer2, currentServer1, currentServer2);

      // spin up current version servers
      currentServer1
          .invoke(invokeAssertVersion(VersionManager.getInstance().getCurrentVersionOrdinal()));
      currentServer2
          .invoke(invokeAssertVersion(VersionManager.getInstance().getCurrentVersionOrdinal()));

      // create region
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), currentServer1, currentServer2,
          oldServerAndLocator, oldServer2);

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      oldServerAndLocator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      // put some data in the region to make sure there is something to clear.
      putAndVerify(objectType, currentServer1, regionName, 0, 10, currentServer2,
          oldServerAndLocator, oldServer2);

      // invoke Partition Region Clear and verify we didn't touch the old servers.
      prClearVerifyOldServersUnaffected(objectType, currentServer1, regionName, 0, 10,
          currentServer2, oldServerAndLocator, oldServer2);
    } finally {
      invokeRunnableInVMs(true, invokeCloseCache(), currentServer1, currentServer2,
          oldServerAndLocator, oldServer2);
    }
  }
}
