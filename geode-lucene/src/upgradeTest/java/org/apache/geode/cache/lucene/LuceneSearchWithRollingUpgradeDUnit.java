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
import static org.apache.geode.test.dunit.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public abstract class LuceneSearchWithRollingUpgradeDUnit extends JUnit4DistributedTestCase {


  @Parameterized.Parameters(name = "from_v{0}, with reindex={1}")
  public static Collection<Object[]> data() {
    Collection<String> luceneVersions = getLuceneVersions();
    Collection<Object[]> rval = new ArrayList<>();
    luceneVersions.forEach(v -> {
      rval.add(new Object[] {v, true});
      rval.add(new Object[] {v, false});
    });
    return rval;
  }

  private static Collection<String> getLuceneVersions() {
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

  protected static String INDEX_NAME = "index";

  private static String diskDir = "LuceneSearchWithRollingUpgradeDUnit";

  // Each vm will have a cache object
  protected static Object cache;

  // the old version of Geode we're testing against
  @Parameterized.Parameter()
  public String oldVersion;

  @Parameterized.Parameter(1)
  public Boolean reindex;

  private void deleteVMFiles() {
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

  private void deleteWorkingDirFiles() {
    Invoke.invokeInEveryVM("delete files", () -> deleteVMFiles());
  }

  @Override
  public void postSetUp() {
    deleteWorkingDirFiles();
    IgnoredException.addIgnoredException(
        "cluster configuration service not available|ConflictingPersistentDataException");
  }


  Properties getLocatorPropertiesPre91(String locatorsString) {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locatorsString);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, DUnitLauncher.logLevel);
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
    return props;
  }

  VM rollClientToCurrentAndCreateRegion(VM oldClient, ClientRegionShortcut shortcut,
      String regionName, String[] hostNames, int[] locatorPorts, boolean subscriptionEnabled) {
    VM rollClient = rollClientToCurrent(oldClient, hostNames, locatorPorts, subscriptionEnabled);
    // recreate region on "rolled" client
    invokeRunnableInVMs(invokeCreateClientRegion(regionName, shortcut), rollClient);
    return rollClient;
  }

  private VM rollClientToCurrent(VM oldClient, String[] hostNames, int[] locatorPorts,
      boolean subscriptionEnabled) {
    oldClient.invoke(invokeCloseCache());
    VM rollClient = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, oldClient.getId());
    rollClient.invoke(invokeCreateClientCache(getClientSystemProperties(), hostNames, locatorPorts,
        subscriptionEnabled));
    rollClient.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
    return rollClient;
  }

  CacheSerializableRunnable invokeCreateClientRegion(final String regionName,
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

  private static void createClientRegion(GemFireCache cache, String regionName,
      ClientRegionShortcut shortcut) {
    ClientRegionFactory rf = ((ClientCache) cache).createClientRegionFactory(shortcut);
    rf.create(regionName);
  }

  CacheSerializableRunnable invokeStartCacheServer(final int port) {
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

  private static void startCacheServer(GemFireCache cache, int port) throws Exception {
    CacheServer cacheServer = ((GemFireCacheImpl) cache).addCacheServer();
    cacheServer.setPort(port);
    cacheServer.start();
  }

  CacheSerializableRunnable invokeCreateClientCache(final Properties systemProperties,
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

  Properties getClientSystemProperties() {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0");
    return p;
  }


  private static ClientCache createClientCache(Properties systemProperties, String[] hosts,
      int[] ports, boolean subscriptionEnabled) {
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
  void executeLuceneQueryWithServerRollOvers(String regionType, String startingVersion)
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

  void putSerializableObjectAndVerifyLuceneQueryResult(VM putter, String regionName,
      int expectedRegionSize, int start, int end, VM... vms) throws Exception {
    // do puts
    putSerializableObject(putter, regionName, start, end);

    // verify present in others
    verifyLuceneQueryResultInEachVM(regionName, expectedRegionSize, vms);
  }

  void putSerializableObject(VM putter, String regionName, int start, int end)
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
    await().untilAsserted(() -> {
      Object region =
          cache.getClass().getMethod("getRegion", String.class).invoke(cache, regionName);
      int regionSize = (int) region.getClass().getMethod("size").invoke(region);
      assertEquals("Region size not as expected after 60 seconds", expectedRegionSize,
          regionSize);
    });
  }

  void verifyLuceneQueryResults(String regionName, int expectedRegionSize)
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

    Collection resultsActive = executeLuceneQuery(luceneQuery);

    luceneQuery = luceneQueryFactory.getClass()
        .getMethod("create", String.class, String.class, String.class, String.class)
        .invoke(luceneQueryFactory, INDEX_NAME, regionName, "inactive", "status");

    Collection resultsInactive = executeLuceneQuery(luceneQuery);

    assertEquals("Result size not as expected ", expectedRegionSize,
        resultsActive.size() + resultsInactive.size());
  }

  private Collection executeLuceneQuery(Object luceneQuery)
      throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    Collection results = null;
    int retryCount = 10;
    while (true) {
      try {
        results = (Collection) luceneQuery.getClass().getMethod("findKeys").invoke(luceneQuery);
        break;
      } catch (Exception ex) {
        if (!ex.getCause().getMessage().contains("currently indexing")) {
          throw ex;
        }
        if (--retryCount == 0) {
          throw ex;
        }
      }
    }
    return results;

  }

  private void verifyLuceneQueryResultInEachVM(String regionName, int expectedRegionSize,
      VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> waitForRegionToHaveExpectedSize(regionName, expectedRegionSize));
      vm.invoke(() -> verifyLuceneQueryResults(regionName, expectedRegionSize));
    }

  }

  void invokeRunnableInVMs(CacheSerializableRunnable runnable, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(runnable);
    }
  }

  // Used to close cache and make sure we attempt on all vms even if some do not have a cache
  void invokeRunnableInVMs(boolean catchErrors, CacheSerializableRunnable runnable,
      VM... vms) {
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

  private VM rollServerToCurrent(VM oldServer, int[] locatorPorts) {
    // Roll the server
    oldServer.invoke(invokeCloseCache());
    VM rollServer = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, oldServer.getId());
    rollServer.invoke(invokeCreateCache(locatorPorts == null ? getSystemPropertiesPost71()
        : getSystemPropertiesPost71(locatorPorts)));
    rollServer.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
    return rollServer;
  }

  VM rollServerToCurrentCreateLuceneIndexAndCreateRegion(VM oldServer, String regionType,
      File diskdir, String shortcutName, String regionName, int[] locatorPorts) {
    VM rollServer = rollServerToCurrent(oldServer, locatorPorts);
    return createLuceneIndexAndRegionOnRolledServer(regionType, diskdir, shortcutName, regionName,
        rollServer);
  }

  private VM createLuceneIndexAndRegionOnRolledServer(String regionType, File diskdir,
      String shortcutName, String regionName, VM rollServer) {

    Boolean serializeIt = reindex;
    rollServer.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = serializeIt);
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

  VM rollServerToCurrentAndCreateRegionOnly(VM oldServer, String regionType, File diskdir,
      String shortcutName, String regionName, int[] locatorPorts) {
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

  VM rollLocatorToCurrent(VM oldLocator, final String serverHostName, final int port,
      final String testName, final String locatorString) {
    // Roll the locator
    oldLocator.invoke(invokeStopLocator());
    VM rollLocator = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, oldLocator.getId());
    final Properties props = new Properties();
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    rollLocator.invoke(invokeStartLocator(serverHostName, port, testName, locatorString, props));
    return rollLocator;
  }

  // Due to licensing changes
  private Properties getSystemPropertiesPost71() {
    Properties props = getSystemProperties();
    return props;
  }

  // Due to licensing changes
  private Properties getSystemPropertiesPost71(int[] locatorPorts) {
    Properties props = getSystemProperties(locatorPorts);
    return props;
  }

  private Properties getSystemProperties() {
    Properties props = DistributedTestUtils.getAllDistributedSystemProperties(new Properties());
    props.remove("disable-auto-reconnect");
    props.remove(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME);
    props.remove(DistributionConfig.LOCK_MEMORY_NAME);
    return props;
  }

  Properties getSystemProperties(int[] locatorPorts) {
    Properties p = new Properties();
    String locatorString = getLocatorString(locatorPorts);
    p.setProperty("locators", locatorString);
    p.setProperty("mcast-port", "0");
    return p;
  }

  static String getLocatorString(int locatorPort) {
    String locatorString = getDUnitLocatorAddress() + "[" + locatorPort + "]";
    return locatorString;
  }

  static String getLocatorString(int[] locatorPorts) {
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

  CacheSerializableRunnable invokeStartLocator(final String serverHostName, final int port,
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

  CacheSerializableRunnable invokeCreateCache(final Properties systemProperties) {
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

  CacheSerializableRunnable invokeCreateRegion(final String regionName,
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

  CacheSerializableRunnable invokeStopLocator() {
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

  CacheSerializableRunnable invokeCloseCache() {
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

  private void deleteDiskStores() {
    try {
      FileUtils.deleteDirectory(new File(diskDir).getAbsoluteFile());
    } catch (IOException e) {
      throw new Error("Error deleting files", e);
    }
  }

  private static Object createCache(Properties systemProperties) throws Exception {

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

  private static Object getRegion(Object cache, String regionName) throws Exception {
    return cache.getClass().getMethod("getRegion", String.class).invoke(cache, regionName);
  }

  private static Object put(Object cache, String regionName, Object key, Object value)
      throws Exception {
    Object region = getRegion(cache, regionName);
    return region.getClass().getMethod("put", Object.class, Object.class).invoke(region, key,
        value);
  }

  private static void createRegion(Object cache, String regionName, String shortcutName)
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

  static void createLuceneIndex(Object cache, String regionName, String indexName)
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

  static void createLuceneIndexOnExistingRegion(Object cache, String regionName,
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

  private static void createPersistentPartitonedRegion(Object cache, String regionName,
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

  private static void assertVersion(Object cache, short ordinal) throws Exception {
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

  private static void stopCacheServers(Object cache) throws Exception {
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

  private static void closeCache(Object cache) throws Exception {
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

  private static void rebalance(Object cache) throws Exception {
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
  private static void startLocator(final String serverHostName, final int port,
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

  private static void startLocator(final String serverHostName, final int port, Properties props)
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

  private static void stopLocator() throws Exception {
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
  private static String getDUnitLocatorAddress() {
    return Host.getHost(0).getHostName();
  }

}
