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

import static org.apache.geode.test.dunit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({DistributedTest.class, BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class LuceneSearchWithRollingUpgradeDUnit extends JUnit4DistributedTestCase {


  @Parameterized.Parameters(name = "from_v{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    // Lucene Compatibility checks start with Apache Geode v1.2.0
    // Removing the versions older than v1.2.0
    result.removeIf(s -> Integer.parseInt(s) < 120);
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    } else {
      System.out.println("running against these versions: " + result);
    }
    return result;
  }

  private File[] testingDirs = new File[3];

  private static String INDEX_NAME = "index";

  private static String diskDir = "LuceneSearchWithRollingUpgradeDUnit";

  // Each vm will have a cache object
  private static Object cache;

  // the old version of Geode we're testing against
  private String oldVersion;

  private void deleteVMFiles() throws Exception {
    System.out.println("deleting files in vm" + VM.getCurrentVMNum());
    File pwd = new File(".");
    for (File entry : pwd.listFiles()) {
      try {
        if (entry.isDirectory()) {
          FileUtils.deleteDirectory(entry);
        } else {
          entry.delete();
        }
      } catch (Exception e) {
        System.out.println("Could not delete " + entry + ": " + e.getMessage());
      }
    }
  }

  private void deleteWorkingDirFiles() throws Exception {
    Invoke.invokeInEveryVM("delete files", () -> deleteVMFiles());
  }

  @Override
  public void postSetUp() throws Exception {
    deleteWorkingDirFiles();
    IgnoredException.addIgnoredException(
        "cluster configuration service not available|ConflictingPersistentDataException");
  }

  public LuceneSearchWithRollingUpgradeDUnit(String version) {
    oldVersion = version;
  }

  @Test
  public void luceneQueryReturnsCorrectResultsAfterServersRollOverOnPartitionRegion()
      throws Exception {
    executeLuceneQueryWithServerRollOvers("partitionedRedundant", oldVersion);
  }

  @Test
  public void luceneQueryReturnsCorrectResultsAfterServersRollOverOnPersistentPartitionRegion()
      throws Exception {
    executeLuceneQueryWithServerRollOvers("persistentPartitioned", oldVersion);
  }

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
          () -> Awaitility.await().atMost(65, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
              .until(() -> assertTrue(
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

  // 2 locator, 2 servers
  @Test
  public void luceneQueryReturnsCorrectResultAfterTwoLocatorsWithTwoServersAreRolled()
      throws Exception {
    final Host host = Host.getHost(0);
    VM locator1 = host.getVM(oldVersion, 0);
    VM locator2 = host.getVM(oldVersion, 1);
    VM server1 = host.getVM(oldVersion, 2);
    VM server2 = host.getVM(oldVersion, 3);

    final String regionName = "aRegion";
    RegionShortcut shortcut = RegionShortcut.PARTITION_REDUNDANT;
    String regionType = "partitionedRedundant";

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

    String hostName = NetworkUtils.getServerHostName(host);
    String locatorString = getLocatorString(locatorPorts);
    try {
      locator1.invoke(
          invokeStartLocator(hostName, locatorPorts[0], getLocatorPropertiesPre91(locatorString)));
      locator2.invoke(
          invokeStartLocator(hostName, locatorPorts[1], getLocatorPropertiesPre91(locatorString)));
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server1, server2);

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      locator1.invoke(
          () -> Awaitility.await().atMost(65, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
              .until(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));
      locator2.invoke(
          () -> Awaitility.await().atMost(65, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
              .until(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      server1.invoke(() -> createLuceneIndex(cache, regionName, INDEX_NAME));
      server2.invoke(() -> createLuceneIndex(cache, regionName, INDEX_NAME));

      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut.name()), server1, server2);
      int expectedRegionSize = 10;
      putSerializableObjectAndVerifyLuceneQueryResult(server1, regionName, expectedRegionSize, 0,
          10, server1, server2);
      locator1 = rollLocatorToCurrent(locator1, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);

      locator2 = rollLocatorToCurrent(locator2, hostName, locatorPorts[1], getTestMethodName(),
          locatorString);

      server1 = rollServerToCurrentCreateLuceneIndexAndCreateRegion(server1, regionType, null,
          shortcut.name(), regionName, locatorPorts);
      expectedRegionSize += 10;
      putSerializableObjectAndVerifyLuceneQueryResult(server2, regionName, expectedRegionSize, 15,
          25, server1, server2);
      expectedRegionSize += 5;
      putSerializableObjectAndVerifyLuceneQueryResult(server1, regionName, expectedRegionSize, 20,
          30, server1, server2);

      server2 = rollServerToCurrentCreateLuceneIndexAndCreateRegion(server2, regionType, null,
          shortcut.name(), regionName, locatorPorts);
      expectedRegionSize += 5;
      putSerializableObjectAndVerifyLuceneQueryResult(server2, regionName, expectedRegionSize, 25,
          35, server1, server2);
      expectedRegionSize += 5;
      putSerializableObjectAndVerifyLuceneQueryResult(server1, regionName, expectedRegionSize, 30,
          40, server1, server2);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator1, locator2);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2);
    }
  }


  public Properties getLocatorPropertiesPre91(String locatorsString) {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locatorsString);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, DUnitLauncher.logLevel);
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
    return props;
  }

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
          () -> Awaitility.await().atMost(65, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
              .until(() -> assertTrue(
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

  private VM rollClientToCurrentAndCreateRegion(VM oldClient, ClientRegionShortcut shortcut,
      String regionName, String[] hostNames, int[] locatorPorts, boolean subscriptionEnabled)
      throws Exception {
    VM rollClient = rollClientToCurrent(oldClient, hostNames, locatorPorts, subscriptionEnabled);
    // recreate region on "rolled" client
    invokeRunnableInVMs(invokeCreateClientRegion(regionName, shortcut), rollClient);
    return rollClient;
  }

  private VM rollClientToCurrent(VM oldClient, String[] hostNames, int[] locatorPorts,
      boolean subscriptionEnabled) throws Exception {
    oldClient.invoke(invokeCloseCache());
    VM rollClient = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, oldClient.getId());
    rollClient.invoke(invokeCreateClientCache(getClientSystemProperties(), hostNames, locatorPorts,
        subscriptionEnabled));
    rollClient.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
    return rollClient;
  }

  @Test
  public void luceneQueryReturnsCorrectResultsAfterClientAndServersAreRolledOverAllBucketsCreated()
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

    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

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
          () -> Awaitility.await().atMost(65, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
              .until(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server1, server2);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[0]), server1);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[1]), server2);
      invokeRunnableInVMs(
          invokeCreateClientCache(getClientSystemProperties(), hostNames, locatorPorts, false),
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
          shortcut.name(), regionName, locatorPorts);

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
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), client, server2);
    }
  }

  private CacheSerializableRunnable invokeCreateClientRegion(final String regionName,
      final ClientRegionShortcut shortcut) {
    return new CacheSerializableRunnable("execute: createClientRegion") {
      public void run2() {
        try {
          createClientRegion((GemFireCache) LuceneSearchWithRollingUpgradeDUnit.cache, regionName,
              shortcut);
        } catch (Exception e) {
          fail("Error creating client region", e);
        }
      }
    };
  }

  public static void createClientRegion(GemFireCache cache, String regionName,
      ClientRegionShortcut shortcut) throws Exception {
    ClientRegionFactory rf = ((ClientCache) cache).createClientRegionFactory(shortcut);
    rf.create(regionName);
  }

  private CacheSerializableRunnable invokeStartCacheServer(final int port) {
    return new CacheSerializableRunnable("execute: startCacheServer") {
      public void run2() {
        try {
          startCacheServer((GemFireCache) LuceneSearchWithRollingUpgradeDUnit.cache, port);
        } catch (Exception e) {
          fail("Error creating cache", e);
        }
      }
    };
  }

  public static void startCacheServer(GemFireCache cache, int port) throws Exception {
    CacheServer cacheServer = ((GemFireCacheImpl) cache).addCacheServer();
    cacheServer.setPort(port);
    cacheServer.start();
  }

  private CacheSerializableRunnable invokeCreateClientCache(final Properties systemProperties,
      final String[] hosts, final int[] ports, boolean subscriptionEnabled) {
    return new CacheSerializableRunnable("execute: createClientCache") {
      public void run2() {
        try {
          LuceneSearchWithRollingUpgradeDUnit.cache =
              createClientCache(systemProperties, hosts, ports, subscriptionEnabled);
        } catch (Exception e) {
          fail("Error creating client cache", e);
        }
      }
    };
  }

  public Properties getClientSystemProperties() {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0");
    return p;
  }


  public static ClientCache createClientCache(Properties systemProperties, String[] hosts,
      int[] ports, boolean subscriptionEnabled) throws Exception {
    ClientCacheFactory cf = new ClientCacheFactory(systemProperties);
    if (subscriptionEnabled) {
      cf.setPoolSubscriptionEnabled(true);
      cf.setPoolSubscriptionRedundancy(-1);
    }
    int hostsLength = hosts.length;
    for (int i = 0; i < hostsLength; i++) {
      cf.addPoolLocator(hosts[i], ports[i]);
    }

    return cf.create();
  }

  // We start an "old" locator and old servers
  // We roll the locator
  // Now we roll all the servers from old to new
  public void executeLuceneQueryWithServerRollOvers(String regionType, String startingVersion)
      throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(startingVersion, 0);
    VM server2 = host.getVM(startingVersion, 1);
    VM server3 = host.getVM(startingVersion, 2);
    VM locator = host.getVM(startingVersion, 3);


    String regionName = "aRegion";
    String shortcutName = null;
    if ((regionType.equals("partitionedRedundant"))) {
      shortcutName = RegionShortcut.PARTITION_REDUNDANT.name();
    } else if ((regionType.equals("persistentPartitioned"))) {
      shortcutName = RegionShortcut.PARTITION_PERSISTENT.name();
      for (int i = 0; i < testingDirs.length; i++) {
        testingDirs[i] = new File(diskDir, "diskStoreVM_" + String.valueOf(host.getVM(i).getId()))
            .getAbsoluteFile();
        if (!testingDirs[i].exists()) {
          System.out.println(" Creating diskdir for server: " + i);
          testingDirs[i].mkdirs();
        }
      }
    }

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    String hostName = NetworkUtils.getServerHostName(host);
    String locatorString = getLocatorString(locatorPorts);
    final Properties locatorProps = new Properties();
    // configure all class loaders for each vm

    try {
      locator.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          locatorString, locatorProps));
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server1, server2,
          server3);

      // Create Lucene Index
      server1.invoke(() -> createLuceneIndex(cache, regionName, INDEX_NAME));
      server2.invoke(() -> createLuceneIndex(cache, regionName, INDEX_NAME));
      server3.invoke(() -> createLuceneIndex(cache, regionName, INDEX_NAME));

      // create region
      if ((regionType.equals("persistentPartitioned"))) {
        for (int i = 0; i < testingDirs.length; i++) {
          CacheSerializableRunnable runnable =
              invokeCreatePersistentPartitionedRegion(regionName, testingDirs[i]);
          invokeRunnableInVMs(runnable, host.getVM(i));
        }
      } else {
        invokeRunnableInVMs(invokeCreateRegion(regionName, shortcutName), server1, server2,
            server3);
      }
      int expectedRegionSize = 10;
      putSerializableObjectAndVerifyLuceneQueryResult(server1, regionName, expectedRegionSize, 0,
          10, server2, server3);
      locator = rollLocatorToCurrent(locator, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);

      server1 = rollServerToCurrentCreateLuceneIndexAndCreateRegion(server1, regionType,
          testingDirs[0], shortcutName, regionName, locatorPorts);
      verifyLuceneQueryResultInEachVM(regionName, expectedRegionSize, server1);
      expectedRegionSize += 5;
      putSerializableObjectAndVerifyLuceneQueryResult(server1, regionName, expectedRegionSize, 5,
          15, server2, server3);
      expectedRegionSize += 5;
      putSerializableObjectAndVerifyLuceneQueryResult(server2, regionName, expectedRegionSize, 10,
          20, server1, server3);

      server2 = rollServerToCurrentCreateLuceneIndexAndCreateRegion(server2, regionType,
          testingDirs[1], shortcutName, regionName, locatorPorts);
      verifyLuceneQueryResultInEachVM(regionName, expectedRegionSize, server2);
      expectedRegionSize += 5;
      putSerializableObjectAndVerifyLuceneQueryResult(server2, regionName, expectedRegionSize, 15,
          25, server1, server3);
      expectedRegionSize += 5;
      putSerializableObjectAndVerifyLuceneQueryResult(server3, regionName, expectedRegionSize, 20,
          30, server2, server3);

      server3 = rollServerToCurrentCreateLuceneIndexAndCreateRegion(server3, regionType,
          testingDirs[2], shortcutName, regionName, locatorPorts);
      verifyLuceneQueryResultInEachVM(regionName, expectedRegionSize, server3);
      putSerializableObjectAndVerifyLuceneQueryResult(server3, regionName, expectedRegionSize, 15,
          25, server1, server2);
      putSerializableObjectAndVerifyLuceneQueryResult(server1, regionName, expectedRegionSize, 20,
          30, server1, server2, server3);


    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2, server3);
      if ((regionType.equals("persistentPartitioned"))) {
        deleteDiskStores();
      }
    }
  }

  private void putSerializableObjectAndVerifyLuceneQueryResult(VM putter, String regionName,
      int expectedRegionSize, int start, int end, VM... vms) throws Exception {
    // do puts
    putSerializableObject(putter, regionName, start, end);

    // verify present in others
    verifyLuceneQueryResultInEachVM(regionName, expectedRegionSize, vms);
  }

  private void putSerializableObject(VM putter, String regionName, int start, int end)
      throws Exception {
    for (int i = start; i < end; i++) {
      Class aClass = Thread.currentThread().getContextClassLoader()
          .loadClass("org.apache.geode.cache.query.data.Portfolio");
      Constructor portfolioConstructor = aClass.getConstructor(int.class);
      Object serializableObject = portfolioConstructor.newInstance(i);
      putter.invoke(invokePut(regionName, i, serializableObject));
    }
  }

  private void waitForRegionToHaveExpectedSize(String regionName, int expectedRegionSize) {
    Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
      try {
        Object region =
            cache.getClass().getMethod("getRegion", String.class).invoke(cache, regionName);
        int regionSize = (int) region.getClass().getMethod("size").invoke(region);
        assertEquals("Region size not as expected after 60 seconds", expectedRegionSize,
            regionSize);
      } catch (Exception e) {
        throw new RuntimeException();
      }

    });
  }

  private void verifyLuceneQueryResults(String regionName, int expectedRegionSize)
      throws Exception {
    Class luceneServiceProvider = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.cache.lucene.LuceneServiceProvider");
    Method getLuceneService = luceneServiceProvider.getMethod("get", GemFireCache.class);
    Object luceneService = getLuceneService.invoke(luceneServiceProvider, cache);
    luceneService.getClass()
        .getMethod("waitUntilFlushed", String.class, String.class, long.class, TimeUnit.class)
        .invoke(luceneService, INDEX_NAME, regionName, 60, TimeUnit.SECONDS);
    Method createLuceneQueryFactoryMethod =
        luceneService.getClass().getMethod("createLuceneQueryFactory");
    createLuceneQueryFactoryMethod.setAccessible(true);
    Object luceneQueryFactory = createLuceneQueryFactoryMethod.invoke(luceneService);
    Object luceneQuery = luceneQueryFactory.getClass()
        .getMethod("create", String.class, String.class, String.class, String.class)
        .invoke(luceneQueryFactory, INDEX_NAME, regionName, "active", "status");

    Collection resultsActive =
        (Collection) luceneQuery.getClass().getMethod("findKeys").invoke(luceneQuery);

    luceneQuery = luceneQueryFactory.getClass()
        .getMethod("create", String.class, String.class, String.class, String.class)
        .invoke(luceneQueryFactory, INDEX_NAME, regionName, "inactive", "status");

    Collection resultsInactive =
        (Collection) luceneQuery.getClass().getMethod("findKeys").invoke(luceneQuery);

    assertEquals("Result size not as expected ", expectedRegionSize,
        resultsActive.size() + resultsInactive.size());
  }

  private void verifyLuceneQueryResultInEachVM(String regionName, int expectedRegionSize,
      VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> waitForRegionToHaveExpectedSize(regionName, expectedRegionSize));
      vm.invoke(() -> verifyLuceneQueryResults(regionName, expectedRegionSize));
    }

  }

  private void invokeRunnableInVMs(CacheSerializableRunnable runnable, VM... vms) throws Exception {
    for (VM vm : vms) {
      vm.invoke(runnable);
    }
  }

  // Used to close cache and make sure we attempt on all vms even if some do not have a cache
  private void invokeRunnableInVMs(boolean catchErrors, CacheSerializableRunnable runnable,
      VM... vms) throws Exception {
    for (VM vm : vms) {
      try {
        vm.invoke(runnable);
      } catch (Exception e) {
        if (!catchErrors) {
          throw e;
        }
      }
    }
  }

  private VM rollServerToCurrent(VM oldServer, int[] locatorPorts) throws Exception {
    // Roll the server
    oldServer.invoke(invokeCloseCache());
    VM rollServer = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, oldServer.getId());
    rollServer.invoke(invokeCreateCache(locatorPorts == null ? getSystemPropertiesPost71()
        : getSystemPropertiesPost71(locatorPorts)));
    rollServer.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
    return rollServer;
  }

  private VM rollServerToCurrentCreateLuceneIndexAndCreateRegion(VM oldServer, String regionType,
      File diskdir, String shortcutName, String regionName, int[] locatorPorts) throws Exception {
    VM rollServer = rollServerToCurrent(oldServer, locatorPorts);
    rollServer.invoke(() -> createLuceneIndex(cache, regionName, INDEX_NAME));
    // recreate region on "rolled" server
    if ((regionType.equals("persistentPartitioned"))) {
      CacheSerializableRunnable runnable =
          invokeCreatePersistentPartitionedRegion(regionName, diskdir);
      invokeRunnableInVMs(runnable, rollServer);
    } else {
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcutName), rollServer);
    }
    rollServer.invoke(invokeRebalance());
    return rollServer;
  }

  private VM rollServerToCurrentAndCreateRegionOnly(VM oldServer, String regionType, File diskdir,
      String shortcutName, String regionName, int[] locatorPorts) throws Exception {
    VM rollServer = rollServerToCurrent(oldServer, locatorPorts);
    // recreate region on "rolled" server
    if ((regionType.equals("persistentPartitioned"))) {
      CacheSerializableRunnable runnable =
          invokeCreatePersistentPartitionedRegion(regionName, diskdir);
      invokeRunnableInVMs(runnable, rollServer);
    } else {
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcutName), rollServer);
    }
    rollServer.invoke(invokeRebalance());
    return rollServer;
  }

  private VM rollLocatorToCurrent(VM oldLocator, final String serverHostName, final int port,
      final String testName, final String locatorString) throws Exception {
    // Roll the locator
    oldLocator.invoke(invokeStopLocator());
    VM rollLocator = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, oldLocator.getId());
    final Properties props = new Properties();
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    rollLocator.invoke(invokeStartLocator(serverHostName, port, testName, locatorString, props));
    return rollLocator;
  }

  // Due to licensing changes
  public Properties getSystemPropertiesPost71() {
    Properties props = getSystemProperties();
    return props;
  }

  // Due to licensing changes
  public Properties getSystemPropertiesPost71(int[] locatorPorts) {
    Properties props = getSystemProperties(locatorPorts);
    return props;
  }

  public Properties getSystemProperties() {
    Properties props = DistributedTestUtils.getAllDistributedSystemProperties(new Properties());
    props.remove("disable-auto-reconnect");
    props.remove(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME);
    props.remove(DistributionConfig.LOCK_MEMORY_NAME);
    return props;
  }

  public Properties getSystemProperties(int[] locatorPorts) {
    Properties p = new Properties();
    String locatorString = getLocatorString(locatorPorts);
    p.setProperty("locators", locatorString);
    p.setProperty("mcast-port", "0");
    return p;
  }

  public static String getLocatorString(int locatorPort) {
    String locatorString = getDUnitLocatorAddress() + "[" + locatorPort + "]";
    return locatorString;
  }

  public static String getLocatorString(int[] locatorPorts) {
    StringBuilder locatorString = new StringBuilder();
    int numLocators = locatorPorts.length;
    for (int i = 0; i < numLocators; i++) {
      locatorString.append(getLocatorString(locatorPorts[i]));
      if (i + 1 < numLocators) {
        locatorString.append(",");
      }
    }
    return locatorString.toString();
  }

  private CacheSerializableRunnable invokeStartLocator(final String serverHostName, final int port,
      final String testName, final String locatorsString, final Properties props) {
    return new CacheSerializableRunnable("execute: startLocator") {
      public void run2() {
        try {
          startLocator(serverHostName, port, testName, locatorsString, props);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeStartLocator(final String serverHostName, final int port,
      final Properties props) {
    return new CacheSerializableRunnable("execute: startLocator") {
      public void run2() {
        try {
          startLocator(serverHostName, port, props);
        } catch (Exception e) {
          fail("Error starting locators", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCreateCache(final Properties systemProperties) {
    return new CacheSerializableRunnable("execute: createCache") {
      public void run2() {
        try {
          LuceneSearchWithRollingUpgradeDUnit.cache = createCache(systemProperties);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeAssertVersion(final short version) {
    return new CacheSerializableRunnable("execute: assertVersion") {
      public void run2() {
        try {
          assertVersion(LuceneSearchWithRollingUpgradeDUnit.cache, version);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCreateRegion(final String regionName,
      final String shortcutName) {
    return new CacheSerializableRunnable("execute: createRegion") {
      public void run2() {
        try {
          createRegion(LuceneSearchWithRollingUpgradeDUnit.cache, regionName, shortcutName);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCreatePersistentPartitionedRegion(final String regionName,
      final File diskstore) {
    return new CacheSerializableRunnable("execute: createPersistentPartitonedRegion") {
      public void run2() {
        try {
          createPersistentPartitonedRegion(LuceneSearchWithRollingUpgradeDUnit.cache, regionName,
              diskstore);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokePut(final String regionName, final Object key,
      final Object value) {
    return new CacheSerializableRunnable("execute: put") {
      public void run2() {
        try {
          put(LuceneSearchWithRollingUpgradeDUnit.cache, regionName, key, value);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeStopLocator() {
    return new CacheSerializableRunnable("execute: stopLocator") {
      public void run2() {
        try {
          stopLocator();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCloseCache() {
    return new CacheSerializableRunnable("execute: closeCache") {
      public void run2() {
        try {
          closeCache(LuceneSearchWithRollingUpgradeDUnit.cache);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeRebalance() {
    return new CacheSerializableRunnable("execute: rebalance") {
      public void run2() {
        try {
          rebalance(LuceneSearchWithRollingUpgradeDUnit.cache);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  public void deleteDiskStores() throws Exception {
    try {
      FileUtils.deleteDirectory(new File(diskDir).getAbsoluteFile());
    } catch (IOException e) {
      throw new Error("Error deleting files", e);
    }
  }

  public static Object createCache(Properties systemProperties) throws Exception {

    Class distConfigClass = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.distributed.internal.DistributionConfigImpl");
    boolean disableConfig = true;
    try {
      distConfigClass.getDeclaredField("useSharedConfiguration");
    } catch (NoSuchFieldException e) {
      disableConfig = false;
    }
    if (disableConfig) {
      systemProperties.put(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    }

    Class cacheFactoryClass = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.cache.CacheFactory");
    Constructor constructor = cacheFactoryClass.getConstructor(Properties.class);
    constructor.setAccessible(true);
    Object cacheFactory = constructor.newInstance(systemProperties);

    Method createMethod = cacheFactoryClass.getMethod("create");
    createMethod.setAccessible(true);
    Object cache = createMethod.invoke(cacheFactory);
    return cache;
  }

  public static Object getRegion(Object cache, String regionName) throws Exception {
    return cache.getClass().getMethod("getRegion", String.class).invoke(cache, regionName);
  }

  public static Object put(Object cache, String regionName, Object key, Object value)
      throws Exception {
    Object region = getRegion(cache, regionName);
    return region.getClass().getMethod("put", Object.class, Object.class).invoke(region, key,
        value);
  }

  public static void createRegion(Object cache, String regionName, String shortcutName)
      throws Exception {
    Class aClass = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.cache.RegionShortcut");
    Object[] enumConstants = aClass.getEnumConstants();
    Object shortcut = null;
    int length = enumConstants.length;
    for (int i = 0; i < length; i++) {
      Object constant = enumConstants[i];
      if (((Enum) constant).name().equals(shortcutName)) {
        shortcut = constant;
        break;
      }
    }

    Method createRegionFactoryMethod = cache.getClass().getMethod("createRegionFactory", aClass);
    createRegionFactoryMethod.setAccessible(true);
    Object regionFactory = createRegionFactoryMethod.invoke(cache, shortcut);
    Method createMethod = regionFactory.getClass().getMethod("create", String.class);
    createMethod.setAccessible(true);
    createMethod.invoke(regionFactory, regionName);
  }

  public static void createLuceneIndex(Object cache, String regionName, String indexName)
      throws Exception {
    Class luceneServiceProvider = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.cache.lucene.LuceneServiceProvider");
    Method getLuceneService = luceneServiceProvider.getMethod("get", GemFireCache.class);
    Object luceneService = getLuceneService.invoke(luceneServiceProvider, cache);
    Method createLuceneIndexFactoryMethod =
        luceneService.getClass().getMethod("createIndexFactory");
    createLuceneIndexFactoryMethod.setAccessible(true);
    Object luceneIndexFactory = createLuceneIndexFactoryMethod.invoke(luceneService);
    luceneIndexFactory.getClass().getMethod("addField", String.class).invoke(luceneIndexFactory,
        "status");
    luceneIndexFactory.getClass().getMethod("create", String.class, String.class)
        .invoke(luceneIndexFactory, indexName, regionName);
  }

  public static void createLuceneIndexOnExistingRegion(Object cache, String regionName,
      String indexName) throws Exception {
    Class luceneServiceProvider = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.cache.lucene.LuceneServiceProvider");
    Method getLuceneService = luceneServiceProvider.getMethod("get", GemFireCache.class);
    Object luceneService = getLuceneService.invoke(luceneServiceProvider, cache);
    Method createLuceneIndexFactoryMethod =
        luceneService.getClass().getMethod("createIndexFactory");
    createLuceneIndexFactoryMethod.setAccessible(true);
    Object luceneIndexFactory = createLuceneIndexFactoryMethod.invoke(luceneService);
    luceneIndexFactory.getClass().getMethod("addField", String.class).invoke(luceneIndexFactory,
        "status");
    luceneIndexFactory.getClass().getMethod("create", String.class, String.class, boolean.class)
        .invoke(luceneIndexFactory, indexName, regionName, true);
  }

  public static void createPersistentPartitonedRegion(Object cache, String regionName,
      File diskStore) throws Exception {
    Object store = cache.getClass().getMethod("findDiskStore", String.class).invoke(cache, "store");
    Class dataPolicyObject = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.cache.DataPolicy");
    Object dataPolicy = dataPolicyObject.getField("PERSISTENT_PARTITION").get(null);
    if (store == null) {
      Object dsf = cache.getClass().getMethod("createDiskStoreFactory").invoke(cache);
      dsf.getClass().getMethod("setMaxOplogSize", long.class).invoke(dsf, 1L);
      dsf.getClass().getMethod("setDiskDirs", File[].class).invoke(dsf,
          new Object[] {new File[] {diskStore.getAbsoluteFile()}});
      dsf.getClass().getMethod("create", String.class).invoke(dsf, "store");
    }
    Object rf = cache.getClass().getMethod("createRegionFactory").invoke(cache);
    rf.getClass().getMethod("setDiskStoreName", String.class).invoke(rf, "store");
    rf.getClass().getMethod("setDataPolicy", dataPolicy.getClass()).invoke(rf, dataPolicy);
    rf.getClass().getMethod("create", String.class).invoke(rf, regionName);
  }

  public static void assertVersion(Object cache, short ordinal) throws Exception {
    Class idmClass = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.distributed.internal.membership.InternalDistributedMember");
    Method getDSMethod = cache.getClass().getMethod("getDistributedSystem");
    getDSMethod.setAccessible(true);
    Object ds = getDSMethod.invoke(cache);

    Method getDistributedMemberMethod = ds.getClass().getMethod("getDistributedMember");
    getDistributedMemberMethod.setAccessible(true);
    Object member = getDistributedMemberMethod.invoke(ds);
    Method getVersionObjectMethod = member.getClass().getMethod("getVersionObject");
    getVersionObjectMethod.setAccessible(true);
    Object thisVersion = getVersionObjectMethod.invoke(member);
    Method getOrdinalMethod = thisVersion.getClass().getMethod("ordinal");
    getOrdinalMethod.setAccessible(true);
    short thisOrdinal = (Short) getOrdinalMethod.invoke(thisVersion);
    if (ordinal != thisOrdinal) {
      throw new Error(
          "Version ordinal:" + thisOrdinal + " was not the expected ordinal of:" + ordinal);
    }
  }

  public static void stopCacheServers(Object cache) throws Exception {
    Method getCacheServersMethod = cache.getClass().getMethod("getCacheServers");
    getCacheServersMethod.setAccessible(true);
    List cacheServers = (List) getCacheServersMethod.invoke(cache);
    Method stopMethod = null;
    for (Object cs : cacheServers) {
      if (stopMethod == null) {
        stopMethod = cs.getClass().getMethod("stop");
      }
      stopMethod.setAccessible(true);
      stopMethod.invoke(cs);
    }
  }

  public static void closeCache(Object cache) throws Exception {
    if (cache == null) {
      return;
    }
    Method isClosedMethod = cache.getClass().getMethod("isClosed");
    isClosedMethod.setAccessible(true);
    boolean cacheClosed = (Boolean) isClosedMethod.invoke(cache);
    if (cache != null && !cacheClosed) {
      stopCacheServers(cache);
      Method method = cache.getClass().getMethod("close");
      method.setAccessible(true);
      method.invoke(cache);
      long startTime = System.currentTimeMillis();
      while (!cacheClosed && System.currentTimeMillis() - startTime < 30000) {
        try {
          Thread.sleep(1000);
          Method cacheClosedMethod = cache.getClass().getMethod("isClosed");
          cacheClosedMethod.setAccessible(true);
          cacheClosed = (Boolean) cacheClosedMethod.invoke(cache);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  public static void rebalance(Object cache) throws Exception {
    Method getRMMethod = cache.getClass().getMethod("getResourceManager");
    getRMMethod.setAccessible(true);
    Object manager = getRMMethod.invoke(cache);

    Method createRebalanceFactoryMethod = manager.getClass().getMethod("createRebalanceFactory");
    createRebalanceFactoryMethod.setAccessible(true);
    Object rebalanceFactory = createRebalanceFactoryMethod.invoke(manager);
    Method m = rebalanceFactory.getClass().getMethod("start");
    m.setAccessible(true);
    Object op = m.invoke(rebalanceFactory);

    // Wait until the rebalance is complete
    try {
      Method getResultsMethod = op.getClass().getMethod("getResults");
      getResultsMethod.setAccessible(true);
      Object results = getResultsMethod.invoke(op);
      Method getTotalTimeMethod = results.getClass().getMethod("getTotalTime");
      getTotalTimeMethod.setAccessible(true);
      System.out.println("Took " + getTotalTimeMethod.invoke(results) + " milliseconds\n");
      Method getTotalBucketsMethod = results.getClass().getMethod("getTotalBucketTransferBytes");
      getTotalBucketsMethod.setAccessible(true);
      System.out.println("Transfered " + getTotalBucketsMethod.invoke(results) + "bytes\n");
    } catch (Exception e) {
      Thread.currentThread().interrupt();
      throw e;
    }
  }

  /**
   * Starts a locator with given configuration.
   */
  public static void startLocator(final String serverHostName, final int port,
      final String testName, final String locatorsString, final Properties props) throws Exception {
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locatorsString);
    Logger logger = LogService.getLogger();
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, logger.getLevel().name());

    InetAddress bindAddr;
    try {
      bindAddr = InetAddress.getByName(serverHostName);// getServerHostName(vm.getHost()));
    } catch (UnknownHostException uhe) {
      throw new Error("While resolving bind address ", uhe);
    }

    File logFile = new File(testName + "-locator" + port + ".log");
    Class locatorClass = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.distributed.Locator");
    Method startLocatorAndDSMethod =
        locatorClass.getMethod("startLocatorAndDS", int.class, File.class, InetAddress.class,
            Properties.class, boolean.class, boolean.class, String.class);
    startLocatorAndDSMethod.setAccessible(true);
    startLocatorAndDSMethod.invoke(null, port, logFile, bindAddr, props, true, true, null);
  }

  public static void startLocator(final String serverHostName, final int port, Properties props)
      throws Exception {


    InetAddress bindAddr = null;
    try {
      bindAddr = InetAddress.getByName(serverHostName);// getServerHostName(vm.getHost()));
    } catch (UnknownHostException uhe) {
      throw new Error("While resolving bind address ", uhe);
    }

    Locator.startLocatorAndDS(port, new File(""), bindAddr, props, true, true, null);
    Thread.sleep(5000); // bug in 1.0 - cluster config service not immediately available
  }

  public static void stopLocator() throws Exception {
    Class internalLocatorClass = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.distributed.internal.InternalLocator");
    Method locatorMethod = internalLocatorClass.getMethod("getLocator");
    locatorMethod.setAccessible(true);
    Object locator = locatorMethod.invoke(null);
    Method stopLocatorMethod = locator.getClass().getMethod("stop");
    stopLocatorMethod.setAccessible(true);
    stopLocatorMethod.invoke(locator);
  }

  /**
   * Get the port that the standard dunit locator is listening on.
   *
   * @return locator address
   */
  public static String getDUnitLocatorAddress() {
    return Host.getHost(0).getHostName();
  }

}
