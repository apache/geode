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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
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
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

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

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public abstract class RollingUpgradeDUnitTest extends JUnit4DistributedTestCase {
  @Parameterized.Parameters(name = "from_v{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    } else {
      System.out.println("running against these versions: " + result);
    }
    return result;
  }

  private File[] testingDirs = new File[2];

  private static String diskDir = "RollingUpgradeDUnitTest";

  // Each vm will have a cache object
  private static Cache cache;

  // the old version of Geode we're testing against
  @Parameterized.Parameter
  public String oldVersion;

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
  public void postSetUp() throws Exception {
    deleteWorkingDirFiles();
    IgnoredException.addIgnoredException(
        "cluster configuration service not available|ConflictingPersistentDataException");
  }

  // We start an "old" locator and old servers
  // We roll the locator
  // Now we roll all the servers from old to new
  void doTestRollAll(String regionType, String objectType, String startingVersion)
      throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(startingVersion, 0); // testingDirs[0]
    VM server2 = host.getVM(startingVersion, 1); // testingDirs[1]
    VM locator = host.getVM(startingVersion, 2); // locator must be last


    String regionName = "aRegion";
    String shortcutName = RegionShortcut.REPLICATE.name();
    if (regionType.equals("replicate")) {
      shortcutName = RegionShortcut.REPLICATE.name();
    } else if ((regionType.equals("partitionedRedundant"))) {
      shortcutName = RegionShortcut.PARTITION_REDUNDANT.name();
    } else if ((regionType.equals("persistentReplicate"))) {
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
          locatorString, locatorProps, true));

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      locator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server1, server2);

      // create region
      if ((regionType.equals("persistentReplicate"))) {
        for (int i = 0; i < testingDirs.length; i++) {
          CacheSerializableRunnable runnable =
              invokeCreatePersistentReplicateRegion(regionName, testingDirs[i]);
          invokeRunnableInVMs(runnable, host.getVM(i));
        }
      } else {
        invokeRunnableInVMs(invokeCreateRegion(regionName, shortcutName), server1, server2);
      }

      putAndVerify(objectType, server1, regionName, 0, 10, server2);
      locator = rollLocatorToCurrent(locator, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);

      server1 = rollServerToCurrentAndCreateRegion(server1, regionType, testingDirs[0],
          shortcutName, regionName, locatorPorts);
      verifyValues(objectType, regionName, 0, 10, server1);
      putAndVerify(objectType, server1, regionName, 5, 15, server2);
      putAndVerify(objectType, server2, regionName, 10, 20, server1);

      server2 = rollServerToCurrentAndCreateRegion(server2, regionType, testingDirs[1],
          shortcutName, regionName, locatorPorts);
      verifyValues(objectType, regionName, 0, 10, server2);
      putAndVerify(objectType, server2, regionName, 15, 25, server1);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2);
      if ((regionType.equals("persistentReplicate"))) {
        deleteDiskStores();
      }
    }
  }

  // ******** TEST HELPER METHODS ********/
  private void putAndVerify(String objectType, VM putter, String regionName, int start, int end,
      VM... checkVMs) throws Exception {
    if (objectType.equals("strings")) {
      putStringsAndVerify(putter, regionName, start, end, checkVMs);
    } else if (objectType.equals("serializable")) {
      putSerializableAndVerify(putter, regionName, start, end, checkVMs);
    } else if (objectType.equals("dataserializable")) {
      putDataSerializableAndVerify(putter, regionName, start, end, checkVMs);
    } else {
      throw new Error("Not a valid test object type");
    }
  }

  private void putStringsAndVerify(VM putter, final String regionName, final int start,
      final int end, VM... vms) {
    for (int i = start; i < end; i++) {
      putter.invoke(invokePut(regionName, "" + i, "VALUE(" + i + ")"));
    }

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

    // verify present in others
    for (VM vm : vms) {
      vm.invoke(invokeAssertEntriesExist(regionName, start, end));
    }
  }

  private void verifyValues(String objectType, String regionName, int start, int end, VM... vms) {
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

  private void invokeRunnableInVMs(CacheSerializableRunnable runnable, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(runnable);
    }
  }

  // Used to close cache and make sure we attempt on all vms even if some do not have a cache
  private void invokeRunnableInVMs(boolean catchErrors, CacheSerializableRunnable runnable,
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

  private VM rollServerToCurrentAndCreateRegion(VM oldServer, String regionType, File diskdir,
      String shortcutName, String regionName, int[] locatorPorts) {
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
      final String testName, final String locatorString) {
    // Roll the locator
    oldLocator.invoke(invokeStopLocator());
    VM rollLocator = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, oldLocator.getId());
    final Properties props = new Properties();
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    rollLocator
        .invoke(invokeStartLocator(serverHostName, port, testName, locatorString, props, false));
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

  private Properties getSystemProperties(int[] locatorPorts) {
    Properties p = new Properties();
    String locatorString = getLocatorString(locatorPorts);
    p.setProperty("locators", locatorString);
    p.setProperty("mcast-port", "0");
    return p;
  }

  private static String getLocatorString(int locatorPort) {
    String locatorString = getDUnitLocatorAddress() + "[" + locatorPort + "]";
    return locatorString;
  }

  private static String getLocatorString(int[] locatorPorts) {
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


  private CacheSerializableRunnable invokeStartLocator(final String serverHostName, final int port,
      final String testName, final String locatorsString, final Properties props,
      boolean fastStart) {
    return new CacheSerializableRunnable("execute: startLocator") {
      public void run2() {
        try {
          startLocator(serverHostName, port, testName, locatorsString, props, fastStart);
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

  private void deleteDiskStores() {
    try {
      FileUtils.deleteDirectory(new File(diskDir).getAbsoluteFile());
    } catch (IOException e) {
      throw new Error("Error deleting files", e);
    }
  }

  private static Cache createCache(Properties systemProperties) throws Exception {

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

    cache = new CacheFactory(systemProperties).create();
    return cache;
  }

  private static boolean assertRegionExists(Cache cache, String regionName) {
    Region region = cache.getRegion(regionName);
    if (region == null) {
      throw new Error("Region: " + regionName + " does not exist");
    }
    return true;
  }

  private static Region getRegion(Cache cache, String regionName) {
    return cache.getRegion(regionName);
  }

  private static boolean assertEntryEquals(Cache cache, String regionName, Object key,
      Object value) {
    assertRegionExists(cache, regionName);
    Region region = getRegion(cache, regionName);
    Object regionValue = region.get(key);
    if (regionValue == null) {
      throw new Error("Region value does not exist for key:" + key);
    }
    if (!regionValue.equals(value)) {
      throw new Error("Entry for key:" + key + " does not equal value: " + value);
    }
    return true;
  }

  private static boolean assertEntryExists(Cache cache, String regionName, Object key) {
    assertRegionExists(cache, regionName);
    Region region = getRegion(cache, regionName);
    if (!region.keySet().contains(key)) {
      throw new Error("Entry for key:" + key + " does not exist");
    }
    return true;
  }

  private static Object put(Cache cache, String regionName, Object key, Object value) {
    Region region = getRegion(cache, regionName);
    return region.put(key, value);
  }

  private static void createRegion(Cache cache, String regionName, String shortcutName)
      throws Exception {
    Class aClass = Thread.currentThread().getContextClassLoader()
        .loadClass("org.apache.geode.cache.RegionShortcut");
    Object[] enumConstants = aClass.getEnumConstants();
    RegionShortcut shortcut = null;
    int length = enumConstants.length;
    for (int i = 0; i < length; i++) {
      Object constant = enumConstants[i];
      if (((Enum) constant).name().equals(shortcutName)) {
        shortcut = (RegionShortcut) constant;
        break;
      }
    }

    cache.createRegionFactory(shortcut).create(regionName);
  }

  public static void createPartitionedRegion(Cache cache, String regionName) throws Exception {
    createRegion(cache, regionName, RegionShortcut.PARTITION.name());
  }

  public static void createReplicatedRegion(Cache cache, String regionName) throws Exception {
    createRegion(cache, regionName, RegionShortcut.REPLICATE.name());
  }

  public static void createRegion(String regionName, Object regionFactory) throws Exception {
    regionFactory.getClass().getMethod("create", String.class).invoke(regionFactory, regionName);
  }

  private static void createPersistentReplicateRegion(Cache cache, String regionName,
      File diskStore) {

    DiskStore store = cache.findDiskStore("store");
    if (store == null) {
      cache.createDiskStoreFactory().setMaxOplogSize(1L)
          .setDiskDirs(new File[] {diskStore.getAbsoluteFile()}).create("store");
    }
    cache.createRegionFactory().setDiskStoreName("store")
        .setDataPolicy(DataPolicy.PERSISTENT_REPLICATE).setScope(Scope.DISTRIBUTED_ACK)
        .create(regionName);
  }

  private static void assertVersion(Cache cache, short ordinal) {
    DistributedSystem ds = cache.getDistributedSystem();
    InternalDistributedMember member = (InternalDistributedMember) ds.getDistributedMember();
    Version thisVersion = member.getVersionObject();
    short thisOrdinal = thisVersion.ordinal();
    if (ordinal != thisOrdinal) {
      throw new Error(
          "Version ordinal:" + thisOrdinal + " was not the expected ordinal of:" + ordinal);
    }
  }

  private static void stopCacheServers(Cache cache) {
    for (CacheServer server : cache.getCacheServers()) {
      server.stop();
    }
  }

  private static void closeCache(final Cache cache) {
    if (cache == null) {
      return;
    }
    if (!cache.isClosed()) {
      stopCacheServers(cache);
      cache.close();
      long startTime = System.currentTimeMillis();
      await().until(() -> cache.isClosed());
    }
  }

  private static void rebalance(Cache cache) throws Exception {
    RebalanceOperation op = cache.getResourceManager().createRebalanceFactory().start();

    // Wait until the rebalance is complete
    try {
      RebalanceResults results = op.getResults();
      long totalTime = results.getTotalTime();
      System.out.println("Took " + totalTime + " milliseconds");
      System.out.println("Transfered " + results.getTotalBucketTransferBytes() + "bytes");
    } catch (Exception e) {
      Thread.currentThread().interrupt();
      throw e;
    }
  }

  /**
   * Starts a locator with given configuration.
   */
  private static void startLocator(final String serverHostName, final int port,
      final String testName, final String locatorsString, final Properties props, boolean fastStart)
      throws Exception {
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locatorsString);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());

    InetAddress bindAddr = null;
    try {
      bindAddr = InetAddress.getByName(serverHostName);// getServerHostName(vm.getHost()));
    } catch (UnknownHostException uhe) {
      throw new Error("While resolving bind address ", uhe);
    }

    if (fastStart) {
      System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
    }
    try {
      Locator.startLocatorAndDS(port, new File(""), bindAddr, props, true, true, null);
    } finally {
      if (fastStart) {
        System.getProperties().remove(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
      }
    }
  }

  private static void stopLocator() {
    Locator.getLocator().stop();
  }

  /**
   * Get the port that the standard dunit locator is listening on.
   */
  private static String getDUnitLocatorAddress() {
    return Host.getHost(0).getHostName();
  }

}
