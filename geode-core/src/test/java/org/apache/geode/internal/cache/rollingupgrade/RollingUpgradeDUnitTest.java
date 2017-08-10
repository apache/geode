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

import org.apache.commons.io.FileUtils;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.Version;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * RollingUpgrade dunit tests are distributed among subclasses of RollingUpgradeDUnitTest to avoid
 * spurious "hangs" being declared by Hydra.
 *
 * This test will not run properly in eclipse at this point due to having to bounce vms Currently,
 * bouncing vms is necessary because we are starting gemfire with different class loaders and the
 * class loaders are lingering (possibly due to daemon threads - the vm itself is still running)
 * 
 * Note: to run in eclipse, I had to copy over the jg-magic-map.txt file into my GEMFIRE_OUTPUT
 * location, in the same directory as #MagicNumberReader otherwise the two systems were unable to
 * talk to one another due to one using a magic number and the other not. Also turnOffBounce will
 * need to be set to true so that bouncing a vm doesn't lead to a NPE.
 * 
 * @author jhuynh
 */

@Category({DistributedTest.class, BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RollingUpgradeDUnitTest extends JUnit4DistributedTestCase {

  @Parameterized.Parameters
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    } else {
      System.out.println("running against these versions: " + result);
    }
    return result;
  }

  private File[] testingDirs = new File[3];

  private static String diskDir = "RollingUpgradeDUnitTest";

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

  public RollingUpgradeDUnitTest(String version) {
    oldVersion = version;
  }

  @Test
  public void testRollServersOnReplicatedRegion_dataserializable() throws Exception {
    doTestRollAll("replicate", "dataserializable", oldVersion);
  }

  @Test
  public void testRollServersOnPartitionedRegion_dataserializable() throws Exception {
    doTestRollAll("partitionedRedundant", "dataserializable", oldVersion);
  }

  @Test
  public void testRollServersOnPersistentRegion_dataserializable() throws Exception {
    doTestRollAll("persistentReplicate", "dataserializable", oldVersion);
  }


  // We start an "old" locator and old servers
  // We roll the locator
  // Now we roll all the servers from old to new
  public void doTestRollAll(String regionType, String objectType, String startingVersion)
      throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(startingVersion, 0);
    VM server2 = host.getVM(startingVersion, 1);
    VM server3 = host.getVM(startingVersion, 2);
    VM locator = host.getVM(startingVersion, 3);


    String regionName = "aRegion";
    String shortcutName = RegionShortcut.REPLICATE.name();
    if (regionType.equals("replicate")) {
      shortcutName = RegionShortcut.REPLICATE.name();
    } else if ((regionType.equals("partitionedRedundant"))) {
      shortcutName = RegionShortcut.PARTITION_REDUNDANT.name();
    } else if ((regionType.equals("persistentReplicate"))) {
      shortcutName = RegionShortcut.PARTITION_PERSISTENT.name();
      for (int i = 0; i < testingDirs.length; i++) {
        testingDirs[i] = new File(diskDir, "diskStoreVM_" + String.valueOf(host.getVM(i).getPid()))
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

      // create region
      if ((regionType.equals("persistentReplicate"))) {
        for (int i = 0; i < testingDirs.length; i++) {
          CacheSerializableRunnable runnable =
              invokeCreatePersistentReplicateRegion(regionName, testingDirs[i]);
          invokeRunnableInVMs(runnable, host.getVM(i));
        }
      } else {
        invokeRunnableInVMs(invokeCreateRegion(regionName, shortcutName), server1, server2,
            server3);
      }

      putAndVerify(objectType, server1, regionName, 0, 10, server2, server3);
      locator = rollLocatorToCurrent(locator, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);

      server1 = rollServerToCurrentAndCreateRegion(server1, regionType, testingDirs[0],
          shortcutName, regionName, locatorPorts);
      verifyValues(objectType, regionName, 0, 10, server1);
      putAndVerify(objectType, server1, regionName, 5, 15, server2, server3);
      putAndVerify(objectType, server2, regionName, 10, 20, server1, server3);

      server2 = rollServerToCurrentAndCreateRegion(server2, regionType, testingDirs[1],
          shortcutName, regionName, locatorPorts);
      verifyValues(objectType, regionName, 0, 10, server2);
      putAndVerify(objectType, server2, regionName, 15, 25, server1, server3);
      putAndVerify(objectType, server3, regionName, 20, 30, server2, server3);

      server3 = rollServerToCurrentAndCreateRegion(server3, regionType, testingDirs[2],
          shortcutName, regionName, locatorPorts);
      verifyValues(objectType, regionName, 0, 10, server3);
      putAndVerify(objectType, server3, regionName, 15, 25, server1, server2);
      putAndVerify(objectType, server1, regionName, 20, 30, server1, server2, server3);


    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2, server3);
      if ((regionType.equals("persistentReplicate"))) {
        deleteDiskStores();
      }
    }
  }

  // ******** TEST HELPER METHODS ********/
  private void putAndVerify(String objectType, VM putter, String regionName, int start, int end,
      VM check1, VM check2, VM check3) throws Exception {
    if (objectType.equals("strings")) {
      putStringsAndVerify(putter, regionName, start, end, check1, check2, check3);
    } else if (objectType.equals("serializable")) {
      putSerializableAndVerify(putter, regionName, start, end, check1, check2, check3);
    } else if (objectType.equals("dataserializable")) {
      putDataSerializableAndVerify(putter, regionName, start, end, check1, check2, check3);
    } else {
      throw new Error("Not a valid test object type");
    }
  }

  // ******** TEST HELPER METHODS ********/
  private void putAndVerify(String objectType, VM putter, String regionName, int start, int end,
      VM check1, VM check2) throws Exception {
    if (objectType.equals("strings")) {
      putStringsAndVerify(putter, regionName, start, end, check1, check2);
    } else if (objectType.equals("serializable")) {
      putSerializableAndVerify(putter, regionName, start, end, check1, check2);
    } else if (objectType.equals("dataserializable")) {
      putDataSerializableAndVerify(putter, regionName, start, end, check1, check2);
    } else {
      throw new Error("Not a valid test object type");
    }
  }

  private void putStringsAndVerify(VM putter, final String regionName, final int start,
      final int end, VM... vms) {
    for (int i = start; i < end; i++) {
      putter.invoke(invokePut(regionName, "" + i, "VALUE(" + i + ")"));
    }

    threadSleep();

    // verify present in others
    for (VM vm : vms) {
      vm.invoke(invokeAssertEntriesEqual(regionName, start, end));
    }
  }

  private void putSerializableAndVerify(VM putter, String regionName, int start, int end,
      VM... vms) {
    for (int i = start; i < end; i++) {
      putter.invoke(invokePut(regionName, "" + i, new Properties()));
    }

    threadSleep();

    // verify present in others
    for (VM vm : vms) {
      vm.invoke(invokeAssertEntriesExist(regionName, start, end));
    }
  }

  private void putDataSerializableAndVerify(VM putter, String regionName, int start, int end,
      VM... vms) throws Exception {
    for (int i = start; i < end; i++) {
      Class aClass = Thread.currentThread().getContextClassLoader()
          .loadClass("org.apache.geode.cache.ExpirationAttributes");
      Constructor constructor = aClass.getConstructor(int.class);
      Object testDataSerializable = constructor.newInstance(i);
      putter.invoke(invokePut(regionName, "" + i, testDataSerializable));
    }

    threadSleep();

    // verify present in others
    for (VM vm : vms) {
      vm.invoke(invokeAssertEntriesExist(regionName, start, end));
    }
  }

  private void verifyValues(String objectType, String regionName, int start, int end, VM... vms) {
    threadSleep();
    if (objectType.equals("strings")) {
      for (VM vm : vms) {
        vm.invoke(invokeAssertEntriesEqual(regionName, start, end));
      }
    } else if (objectType.equals("serializable")) {
      for (VM vm : vms) {
        vm.invoke(invokeAssertEntriesExist(regionName, start, end));
      }
    } else if (objectType.equals("dataserializable")) {
      for (VM vm : vms) {
        vm.invoke(invokeAssertEntriesExist(regionName, start, end));
      }
    }
  }

  // Oddly the puts will return and for some reason the other vms have yet to recieve or process the
  // put?
  private void threadSleep() {
    try {
      Thread.sleep(250);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void query(String queryString, int numExpectedResults, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(invokeAssertQueryResults(queryString, numExpectedResults));
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
    VM rollServer = Host.getHost(0).getVM(oldServer.getPid()); // gets a vm with the current version
    rollServer.invoke(invokeCreateCache(locatorPorts == null ? getSystemPropertiesPost71()
        : getSystemPropertiesPost71(locatorPorts)));
    rollServer.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
    return rollServer;
  }

  /*
   * @param rollServer
   * 
   * @param createRegionMethod
   * 
   * @param regionName
   * 
   * @param locatorPorts if null, uses dunit locator
   * 
   * @throws Exception
   */
  private void rollServerToCurrentAndCreateRegion(VM rollServer, String shortcutName,
      String regionName, int[] locatorPorts) throws Exception {
    rollServerToCurrent(rollServer, locatorPorts);
    // recreate region on "rolled" server
    invokeRunnableInVMs(invokeCreateRegion(regionName, shortcutName), rollServer);
    rollServer.invoke(invokeRebalance());
  }

  private VM rollServerToCurrentAndCreateRegion(VM oldServer, String regionType, File diskdir,
      String shortcutName, String regionName, int[] locatorPorts) throws Exception {
    VM rollServer = rollServerToCurrent(oldServer, locatorPorts);
    // recreate region on "rolled" server
    if ((regionType.equals("persistentReplicate"))) {
      CacheSerializableRunnable runnable =
          invokeCreatePersistentReplicateRegion(regionName, diskdir);
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
    VM rollLocator = Host.getHost(0).getVM(oldLocator.getPid()); // gets a VM with current version
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

  public Properties getClientSystemProperties() {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0");
    return p;
  }

  public static String getLocatorString(int locatorPort) {
    String locatorString = getDUnitLocatorAddress() + "[" + locatorPort + "]";
    return locatorString;
  }

  public static String getLocatorString(int[] locatorPorts) {
    String locatorString = "";
    int numLocators = locatorPorts.length;
    for (int i = 0; i < numLocators; i++) {
      locatorString += getLocatorString(locatorPorts[i]);
      if (i + 1 < numLocators) {
        locatorString += ",";
      }
    }
    return locatorString;
  }


  private List<URL> addFile(File file) throws MalformedURLException {
    ArrayList<URL> urls = new ArrayList<URL>();
    if (file.isDirectory()) {
      // Do not want to start cache with sample code xml
      if (file.getName().contains("SampleCode")) {
        return urls;
      } else {
        File[] files = file.listFiles();
        for (File afile : files) {
          urls.addAll(addFile(afile));
        }
      }
    } else {
      URL url = file.toURI().toURL();
      urls.add(url);
    }
    return urls;
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

  private CacheSerializableRunnable invokeStartLocatorAndServer(final String serverHostName,
      final int port, final String testName, final Properties systemProperties) {
    return new CacheSerializableRunnable("execute: startLocator") {
      public void run2() {
        try {
          systemProperties.put(DistributionConfig.START_LOCATOR_NAME,
              "" + serverHostName + "[" + port + "]");
          RollingUpgradeDUnitTest.cache = createCache(systemProperties);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCreateCache(final Properties systemProperties) {
    return new CacheSerializableRunnable("execute: createCache") {
      public void run2() {
        try {
          RollingUpgradeDUnitTest.cache = createCache(systemProperties);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCreateClientCache(final Properties systemProperties,
      final String[] hosts, final int[] ports) {
    return new CacheSerializableRunnable("execute: createClientCache") {
      public void run2() {
        try {
          RollingUpgradeDUnitTest.cache = createClientCache(systemProperties, hosts, ports);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeStartCacheServer(final int port) {
    return new CacheSerializableRunnable("execute: startCacheServer") {
      public void run2() {
        try {
          startCacheServer(RollingUpgradeDUnitTest.cache, port);
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
          assertVersion(RollingUpgradeDUnitTest.cache, version);
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
          createRegion(RollingUpgradeDUnitTest.cache, regionName, shortcutName);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCreatePersistentReplicateRegion(final String regionName,
      final File diskstore) {
    return new CacheSerializableRunnable("execute: createPersistentReplicateRegion") {
      public void run2() {
        try {
          createPersistentReplicateRegion(RollingUpgradeDUnitTest.cache, regionName, diskstore);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCreateClientRegion(final String regionName,
      final String shortcutName) {
    return new CacheSerializableRunnable("execute: createClientRegion") {
      public void run2() {
        try {
          createClientRegion(RollingUpgradeDUnitTest.cache, regionName, shortcutName);
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
          put(RollingUpgradeDUnitTest.cache, regionName, key, value);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeAssertEntriesEqual(final String regionName,
      final int start, final int end) {
    return new CacheSerializableRunnable("execute: assertEntriesEqual") {
      public void run2() {
        try {
          for (int i = start; i < end; i++) {
            assertEntryEquals(RollingUpgradeDUnitTest.cache, regionName, "" + i,
                "VALUE(" + i + ")");
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeAssertEntriesExist(final String regionName,
      final int start, final int end) {
    return new CacheSerializableRunnable("execute: assertEntryExists") {
      public void run2() {
        try {
          for (int i = start; i < end; i++) {
            assertEntryExists(RollingUpgradeDUnitTest.cache, regionName, "" + i);
          }
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
          closeCache(RollingUpgradeDUnitTest.cache);
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
          rebalance(RollingUpgradeDUnitTest.cache);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeAssertQueryResults(final String queryString,
      final int numExpected) {
    return new CacheSerializableRunnable("execute: assertQueryResults") {
      public void run2() {
        try {
          assertQueryResults(RollingUpgradeDUnitTest.cache, queryString, numExpected);
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

    // systemProperties.put(DistributionConfig.LOG_FILE_NAME,
    // "rollingUpgradeCacheVM" + VM.getCurrentVMNum() + ".log");

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
    Object cache = null;
    cache = createMethod.invoke(cacheFactory);
    return cache;
  }

  public static void startCacheServer(Object cache, int port) throws Exception {
    Method addCacheServerMethod = cache.getClass().getMethod("addCacheServer");
    addCacheServerMethod.setAccessible(true);
    Object cacheServer = addCacheServerMethod.invoke(cache);

    Method setPortMethod = cacheServer.getClass().getMethod("setPort", int.class);
    setPortMethod.setAccessible(true);
    setPortMethod.invoke(cacheServer, port);

    Method startMethod = cacheServer.getClass().getMethod("start");
    startMethod.setAccessible(true);
    startMethod.invoke(cacheServer);
  }

  public static Object createClientCache(Properties systemProperties, String[] hosts, int[] ports)
      throws Exception {
    Class aClass = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.cache.client.ClientCacheFactory");
    Constructor constructor = aClass.getConstructor(Properties.class);
    constructor.setAccessible(true);

    Object ccf = constructor.newInstance(systemProperties);
    Method addPoolLocatorMethod = aClass.getMethod("addPoolLocator", String.class, int.class);
    addPoolLocatorMethod.setAccessible(true);
    int hostsLength = hosts.length;
    for (int i = 0; i < hostsLength; i++) {
      addPoolLocatorMethod.invoke(ccf, hosts[i], ports[i]);
    }

    Method createMethod = aClass.getMethod("create");
    createMethod.setAccessible(true);
    Object cache = createMethod.invoke(ccf);

    return cache;
  }

  public static boolean assertRegionExists(Object cache, String regionName) throws Exception {
    Object region = cache.getClass().getMethod("getRegion", String.class).invoke(cache, regionName);
    if (region == null) {
      throw new Error("Region: " + regionName + " does not exist");
    }
    return true;
  }

  public static Object getRegion(Object cache, String regionName) throws Exception {
    return cache.getClass().getMethod("getRegion", String.class).invoke(cache, regionName);
  }

  public static boolean assertEntryEquals(Object cache, String regionName, Object key, Object value)
      throws Exception {
    assertRegionExists(cache, regionName);
    Object region = getRegion(cache, regionName);
    Object regionValue = region.getClass().getMethod("get", Object.class).invoke(region, key);
    if (regionValue == null) {
      System.out.println("region value does not exist for key: " + key);
      throw new Error("Region value does not exist for key:" + key);
    }
    if (!regionValue.equals(value)) {
      System.out.println("Entry for key:" + key + " does not equal value: " + value);
      throw new Error("Entry for key:" + key + " does not equal value: " + value);
    }
    return true;
  }

  public static boolean assertEntryExists(Object cache, String regionName, Object key)
      throws Exception {
    assertRegionExists(cache, regionName);
    Object region = getRegion(cache, regionName);
    Object regionValue = region.getClass().getMethod("get", Object.class).invoke(region, key);
    if (regionValue == null) {
      System.out.println("Entry for key:" + key + " does not exist");
      throw new Error("Entry for key:" + key + " does not exist");
    }
    return true;
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

  public static void createPartitionedRegion(Object cache, String regionName) throws Exception {
    createRegion(cache, regionName, RegionShortcut.PARTITION.name());
  }

  public static void createPartitionedRedundantRegion(Object cache, String regionName)
      throws Exception {
    createRegion(cache, regionName, RegionShortcut.PARTITION_REDUNDANT.name());
  }

  public static void createReplicatedRegion(Object cache, String regionName) throws Exception {
    createRegion(cache, regionName, RegionShortcut.REPLICATE.name());
  }

  // Assumes a client cache is passed
  public static void createClientRegion(Object cache, String regionName, String shortcutName)
      throws Exception {
    Class aClass = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.cache.client.ClientRegionShortcut");
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
    Object clientRegionFactory = cache.getClass()
        .getMethod("createClientRegionFactory", shortcut.getClass()).invoke(cache, shortcut);
    clientRegionFactory.getClass().getMethod("create", String.class).invoke(clientRegionFactory,
        regionName);
  }

  public static void createRegion(String regionName, Object regionFactory) throws Exception {
    regionFactory.getClass().getMethod("create", String.class).invoke(regionFactory, regionName);
  }

  public static void createPersistentReplicateRegion(Object cache, String regionName,
      File diskStore) throws Exception {
    Object store = cache.getClass().getMethod("findDiskStore", String.class).invoke(cache, "store");
    Class dataPolicyObject = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.cache.DataPolicy");
    Object dataPolicy = dataPolicyObject.getField("PERSISTENT_REPLICATE").get(null);
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

  public static void assertQueryResults(Object cache, String queryString, int numExpectedResults) {
    try {
      Method getQSMethod = cache.getClass().getMethod("getQueryService");
      getQSMethod.setAccessible(true);
      Object qs = getQSMethod.invoke(cache);
      Method newQueryMethod = qs.getClass().getMethod("newQuery", String.class);
      newQueryMethod.setAccessible(true);
      Object query = newQueryMethod.invoke(qs, queryString);
      Method executeMethod = query.getClass().getMethod("execute");
      executeMethod.setAccessible(true);
      Object results = executeMethod.invoke(query);

      Method sizeMethod = results.getClass().getMethod("size");
      sizeMethod.setAccessible(true);
      int numResults = (Integer) sizeMethod.invoke(results);

      if (numResults != numExpectedResults) {
        System.out.println("Num Results was:" + numResults);
        throw new Error("Num results:" + numResults + " != num expected:" + numExpectedResults);
      }
    } catch (Exception e) {
      throw new Error("Query Exception", e);
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
    cache = null;
  }

  public static void rebalance(Object cache) throws Exception {
    Method getRMMethod = cache.getClass().getMethod("getResourceManager");
    getRMMethod.setAccessible(true);
    Object manager = getRMMethod.invoke(cache);

    Method createRebalanceFactoryMethod = manager.getClass().getMethod("createRebalanceFactory");
    createRebalanceFactoryMethod.setAccessible(true);
    Object rebalanceFactory = createRebalanceFactoryMethod.invoke(manager);
    Object op = null;
    Method m = rebalanceFactory.getClass().getMethod("start");
    m.setAccessible(true);
    op = m.invoke(rebalanceFactory);

    // Wait until the rebalance is completex
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
   * 
   * @param props TODO
   */
  public static void startLocator(final String serverHostName, final int port,
      final String testName, final String locatorsString, final Properties props) throws Exception {
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locatorsString);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());

    InetAddress bindAddr = null;
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
   * @return
   */
  public static String getDUnitLocatorAddress() {
    return Host.getHost(0).getHostName();
  }

}

