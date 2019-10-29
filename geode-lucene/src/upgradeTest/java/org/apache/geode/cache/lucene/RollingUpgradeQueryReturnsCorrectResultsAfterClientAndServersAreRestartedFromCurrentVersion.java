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

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.VersionManager;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RollingUpgradeQueryReturnsCorrectResultsAfterClientAndServersAreRestartedFromCurrentVersion
    extends LuceneSearchWithRollingUpgradeTestBase {

  @Parameterized.Parameter()
  public Boolean reindex;

  @Parameterized.Parameter(1)
  public Boolean singleHopEnabled;

  @Parameterized.Parameters(name = "from_currentVersion, with reindex={0}, singleHopEnabled={1}")
  public static Collection<Object[]> data() {
    Collection<Object[]> rval = new ArrayList<>();
    rval.add(new Object[] {true, true});
    rval.add(new Object[] {true, false});
    rval.add(new Object[] {false, true});
    rval.add(new Object[] {false, false});
    return rval;
  }

  @Test
  public void luceneFunctionsShouldFailOverByRetryWhenRestartOneServerWithRebalance()
      throws Exception {
    // Since the changes relating to GEODE-7258 is not applied on 1.10.0,
    // use this test to roll from develop to develop to verify.
    final Host host = Host.getHost(0);
    VM locator = host.getVM(VersionManager.CURRENT_VERSION, 0);
    VM server1 = host.getVM(VersionManager.CURRENT_VERSION, 1);
    VM server2 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    VM client = host.getVM(VersionManager.CURRENT_VERSION, 3);

    final String regionName = "aRegion";
    String regionType = "partitionedRedundant";
    RegionShortcut shortcut = RegionShortcut.PARTITION_REDUNDANT;

    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    int[] locatorPorts = new int[] {ports[0]};
    int[] csPorts = new int[] {ports[1], ports[2]};

    locator.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(locatorPorts));

    String hostName = NetworkUtils.getServerHostName(host);
    String[] hostNames = new String[] {hostName};
    String locatorString = getLocatorString(locatorPorts);

    try {
      // Start locator, servers and client in old version
      locator.invoke(
          invokeStartLocator(hostName, locatorPorts[0], getLocatorPropertiesPre91(locatorString)));

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
          invokeCreateClientCache(getClientSystemProperties(), hostNames, locatorPorts, false,
              singleHopEnabled),
          client);

      // Create the index on the servers
      server1.invoke(() -> createLuceneIndex(cache, regionName, INDEX_NAME));
      server2.invoke(() -> createLuceneIndex(cache, regionName, INDEX_NAME));

      // Create the region on the servers and client
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut.name()), server1, server2);
      invokeRunnableInVMs(invokeCreateClientRegion(regionName, ClientRegionShortcut.PROXY), client);

      // Put objects on the client so that each bucket is created
      int numObjects = 113;
      putSerializableObject(client, regionName, 0, numObjects);

      // Execute a query on the client and verify the results. This also waits until flushed.
      client.invoke(() -> verifyLuceneQueryResults(regionName, numObjects));

      // Roll the locator and server 1 to current version
      locator = rollLocatorToCurrent(locator, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);
      server1 = rollServerToCurrentCreateLuceneIndexAndCreateRegion(server1, regionType, null,
          shortcut.name(), regionName, locatorPorts, reindex);

      // Execute a query on the client and verify the results. This also waits until flushed.
      client.invoke(() -> {
        updateClientSingleHopMetadata(regionName);
        verifyLuceneQueryResults(regionName, numObjects);
      });

      // Put some objects on the client. This will update the document to the latest lucene version
      putSerializableObject(client, regionName, 0, numObjects);

      // Execute a query on the client and verify the results. This also waits until flushed.
      client.invoke(() -> verifyLuceneQueryResults(regionName, numObjects));

      // Close server 1 cache. This will force server 2 (old version) to become primary
      invokeRunnableInVMs(true, invokeCloseCache(), server1);

      // Execute a query on the client and verify the results
      client.invoke(() -> verifyLuceneQueryResults(regionName, numObjects));
    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), client, server2);
    }
  }
}
