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
package org.apache.geode.cache.lucene;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;

public class RollingUpgradeReindexShouldBeSuccessfulWhenAllServersRollToCurrentVersion
    extends LuceneSearchWithRollingUpgradeDUnit {

  @Test
  public void luceneReindexShouldBeSuccessfulWhenAllServersRollToCurrentVersion() throws Exception {
    final Host host = Host.getHost(0);
    VM locator1 = host.getVM(oldVersion, 0);
    VM server1 = host.getVM(oldVersion, 1);
    VM server2 = host.getVM(oldVersion, 2);

    final String regionName = "aRegion";
    RegionShortcut shortcut = RegionShortcut.PARTITION_REDUNDANT;

    int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    DistributedTestUtils.deleteLocatorStateFile(locatorPort);

    String hostName = NetworkUtils.getServerHostName(host);
    String locatorString = getLocatorString(locatorPort);
    String regionType = "partitionedRedundant";
    try {
      locator1.invoke(
          invokeStartLocator(hostName, locatorPort, getLocatorPropertiesPre91(locatorString)));
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(new int[] {locatorPort})), server1,
          server2);

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      locator1.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      locator1 =
          rollLocatorToCurrent(locator1, hostName, locatorPort, getTestMethodName(), locatorString);

      server1 = rollServerToCurrentAndCreateRegionOnly(server1, regionType, null, shortcut.name(),
          regionName, new int[] {locatorPort});

      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut.name()), server2);
      try {
        server1.invoke(() -> createLuceneIndexOnExistingRegion(cache, regionName, INDEX_NAME));
        fail();
      } catch (Exception exception) {
        if (!exception.getCause().getCause().getMessage()
            .contains("are not the same Apache Geode version")) {
          exception.printStackTrace();
          fail();
        }
      }

      int expectedRegionSize = 10;
      putSerializableObject(server1, regionName, 0, expectedRegionSize);

      server2 = rollServerToCurrentAndCreateRegionOnly(server2, regionType, null, shortcut.name(),
          regionName, new int[] {locatorPort});


      AsyncInvocation ai1 = server1
          .invokeAsync(() -> createLuceneIndexOnExistingRegion(cache, regionName, INDEX_NAME));

      AsyncInvocation ai2 = server2
          .invokeAsync(() -> createLuceneIndexOnExistingRegion(cache, regionName, INDEX_NAME));

      ai1.join();
      ai2.join();

      ai1.checkException();
      ai2.checkException();

      expectedRegionSize += 10;
      putSerializableObjectAndVerifyLuceneQueryResult(server2, regionName, expectedRegionSize, 15,
          25, server1, server2);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator1);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2);
    }
  }


}
