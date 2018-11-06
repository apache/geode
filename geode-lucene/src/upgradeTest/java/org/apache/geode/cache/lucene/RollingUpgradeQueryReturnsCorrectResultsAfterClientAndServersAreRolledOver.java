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

import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;

public class RollingUpgradeQueryReturnsCorrectResultsAfterClientAndServersAreRolledOver
    extends LuceneSearchWithRollingUpgradeDUnit {

  @Test
  public void luceneQueryReturnsCorrectResultsAfterClientAndServersAreRolledOver()
      throws Exception {
    final Host host = Host.getHost(0);
    VM locator = host.getVM(oldVersion, 0);
    VM server2 = host.getVM(oldVersion, 1);
    VM server3 = host.getVM(oldVersion, 2);
    VM client = host.getVM(oldVersion, 3);

    final String regionName = "aRegion";
    String regionType = "partitionedRedundant";
    RegionShortcut shortcut = RegionShortcut.PARTITION_REDUNDANT;

    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    int[] locatorPorts = new int[] {ports[0]};
    int[] csPorts = new int[] {ports[1], ports[2]};

    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

    String hostName = NetworkUtils.getServerHostName(host);
    String[] hostNames = new String[] {hostName};
    String locatorString = getLocatorString(locatorPorts);
    try {
      locator.invoke(
          invokeStartLocator(hostName, locatorPorts[0], getLocatorPropertiesPre91(locatorString)));

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      locator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server2, server3);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[0]), server2);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[1]), server3);

      invokeRunnableInVMs(
          invokeCreateClientCache(getClientSystemProperties(), hostNames, locatorPorts, false),
          client);
      server2.invoke(() -> createLuceneIndex(cache, regionName, INDEX_NAME));
      server3.invoke(() -> createLuceneIndex(cache, regionName, INDEX_NAME));

      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut.name()), server2, server3);
      invokeRunnableInVMs(invokeCreateClientRegion(regionName, ClientRegionShortcut.PROXY), client);
      int expectedRegionSize = 10;
      putSerializableObjectAndVerifyLuceneQueryResult(client, regionName, expectedRegionSize, 0, 10,
          server3);
      expectedRegionSize += 10;
      putSerializableObjectAndVerifyLuceneQueryResult(server3, regionName, expectedRegionSize, 10,
          20, server2);
      locator = rollLocatorToCurrent(locator, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);

      server3 = rollServerToCurrentCreateLuceneIndexAndCreateRegion(server3, regionType, null,
          shortcut.name(), regionName, locatorPorts);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[1]), server3);
      expectedRegionSize += 10;
      putSerializableObjectAndVerifyLuceneQueryResult(client, regionName, expectedRegionSize, 20,
          30, server3, server2);
      expectedRegionSize += 10;
      putSerializableObjectAndVerifyLuceneQueryResult(server3, regionName, expectedRegionSize, 30,
          40, server2);

      server2 = rollServerToCurrentCreateLuceneIndexAndCreateRegion(server2, regionType, null,
          shortcut.name(), regionName, locatorPorts);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[0]), server2);
      expectedRegionSize += 10;
      putSerializableObjectAndVerifyLuceneQueryResult(client, regionName, expectedRegionSize, 40,
          50, server2, server3);
      expectedRegionSize += 10;
      putSerializableObjectAndVerifyLuceneQueryResult(server2, regionName, expectedRegionSize, 50,
          60, server3);

      client = rollClientToCurrentAndCreateRegion(client, ClientRegionShortcut.PROXY, regionName,
          hostNames, locatorPorts, false);
      expectedRegionSize += 10;
      putSerializableObjectAndVerifyLuceneQueryResult(client, regionName, expectedRegionSize, 60,
          70, server2, server3);
      expectedRegionSize += 10;
      putSerializableObjectAndVerifyLuceneQueryResult(server2, regionName, expectedRegionSize, 70,
          80, server3);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), client, server2, server3);
    }
  }

}
