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

public class RollingUpgradeQueryReturnsCorrectResultsAfterClientAndServersAreRolledOverAllBucketsCreated
    extends LuceneSearchWithRollingUpgradeDUnit {

  @Test
  public void test()
      throws Exception {
    // This test verifies the upgrade from lucene 6 to 7 doesn't cause any issues. Without any
    // changes to accomodate this upgrade, this test will fail with an IndexFormatTooNewException.
    //
    // The main sequence in this test that causes the failure is:
    //
    // - start two servers with old version using Lucene 6
    // - roll one server to new version server using Lucene 7
    // - do puts into primary buckets in new server which creates entries in the fileAndChunk region
    // with Lucene 7 format
    // - stop the new version server which causes the old version server to become primary for those
    // buckets
    // - do a query which causes the IndexFormatTooNewException to be thrown
    final Host host = Host.getHost(0);
    VM locator = host.getVM(oldVersion, 0);
    VM server1 = host.getVM(oldVersion, 1);
    VM server2 = host.getVM(oldVersion, 2);
    VM client = host.getVM(oldVersion, 3);

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
      client.invoke(() -> verifyLuceneQueryResults(regionName, numObjects));

      // Put some objects on the client. This will update the document to the latest lucene version
      putSerializableObject(client, regionName, 0, numObjects);

      // Execute a query on the client and verify the results. This also waits until flushed.
      client.invoke(() -> verifyLuceneQueryResults(regionName, numObjects));

      // Close server 1 cache. This will force server 2 (old version) to become primary
      invokeRunnableInVMs(true, invokeCloseCache(), server1);

      // Execute a query on the client and verify the results
      client.invoke(() -> verifyLuceneQueryResults(regionName, numObjects));
    } finally {
      invokeRunnableInVMs(true, invokeCloseCache(), client, server2);
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
    }
  }
}
