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
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexCreationException;
import org.apache.geode.cache.query.MultiIndexCreationException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.DiskInitFile;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.Oplog;
import org.apache.geode.internal.cache.Oplog.OPLOG_TYPE;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.test.dunit.Assert;
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

/**
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
public abstract class RollingUpgrade2DUnitTestBase extends JUnit4DistributedTestCase {

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

  File[] testingDirs = new File[3];


  protected static String diskDir = "RollingUpgrade2DUnitTestBase";

  // Each vm will have a cache object
  protected static GemFireCache cache;

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

  @Override
  public void postSetUp() throws Exception {
    Invoke.invokeInEveryVM("delete files", () -> deleteVMFiles());
    IgnoredException.addIgnoredException(
        "cluster configuration service not available|ConflictingPersistentDataException");
  }


  HashSet<String> verifyOplogHeader(File dir, HashSet<String> oldFiles) throws IOException {
    if (oldFiles != null) {
      for (String file : oldFiles) {
        System.out.println("Known old format file: " + file);
      }
    }

    File[] files = dir.listFiles();
    HashSet<String> verified = new HashSet<>();
    HashSet<String> oldFilesFound = new HashSet<>();
    for (File file : files) {
      String name = file.getName();
      byte[] expect = new byte[Oplog.OPLOG_MAGIC_SEQ_REC_SIZE];
      byte OPLOG_MAGIC_SEQ_ID = 92; // value of Oplog.OPLOG_MAGIC_SEQ_ID
      if (name.endsWith(".crf")) {
        expect[0] = OPLOG_MAGIC_SEQ_ID;
        System.arraycopy(OPLOG_TYPE.CRF.getBytes(), 0, expect, 1, OPLOG_TYPE.getLen());
        verified.add(".crf");
      } else if (name.endsWith(".drf")) {
        expect[0] = OPLOG_MAGIC_SEQ_ID;
        System.arraycopy(OPLOG_TYPE.DRF.getBytes(), 0, expect, 1, OPLOG_TYPE.getLen());
        verified.add(".drf");
      } else if (name.endsWith(".if")) {
        expect[0] = DiskInitFile.OPLOG_MAGIC_SEQ_ID;
        System.arraycopy(OPLOG_TYPE.IF.getBytes(), 0, expect, 1, OPLOG_TYPE.getLen());
        verified.add(".if");
      } else {
        System.out.println("Ignored: " + file);
        continue;
      }
      expect[expect.length - 1] = 21; // EndOfRecord

      byte[] buf = new byte[Oplog.OPLOG_MAGIC_SEQ_REC_SIZE];

      FileInputStream fis = new FileInputStream(file);
      int count = fis.read(buf, 0, 8);
      fis.close();

      assertEquals(8, count);
      if (oldFiles == null) {
        System.out.println("Verifying old format file: " + file);
        assertFalse(Arrays.equals(expect, buf));
        oldFilesFound.add(name);
      } else {
        if (oldFiles.contains(name)) {
          System.out.println("Verifying old format file: " + file);
          assertFalse(Arrays.equals(expect, buf));
        } else {
          System.out.println("Verifying new format file: " + file);
          assertTrue(Arrays.equals(expect, buf));
        }
      }
    }
    assertTrue(3 <= verified.size());
    return oldFilesFound;
  }


  /**
   * This test starts up multiple servers from the current code base and multiple servers from the
   * old version and executes puts and gets on a new server and old server and verifies that the
   * results are present. Note that the puts have overlapping region keys just to test new puts and
   * replaces
   */
  void doTestPutAndGetMixedServers(String objectType, boolean partitioned, String oldVersion)
      throws Exception {
    final Host host = Host.getHost(0);
    VM currentServer1 = host.getVM(VersionManager.CURRENT_VERSION, 0);
    VM oldServerAndLocator = host.getVM(oldVersion, 1);
    VM currentServer2 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    VM oldServer2 = host.getVM(oldVersion, 3);

    String regionName = "aRegion";

    RegionShortcut shortcut = RegionShortcut.REPLICATE;
    if (partitioned) {
      shortcut = RegionShortcut.PARTITION;
    }

    String serverHostName = NetworkUtils.getServerHostName();
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    DistributedTestUtils.deleteLocatorStateFile(port);
    try {
      Properties props = getSystemProperties();
      props.remove(DistributionConfig.LOCATORS_NAME);
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

      invokeRunnableInVMs(invokeCreateCache(props), oldServer2, currentServer1, currentServer2);

      currentServer1.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      currentServer2.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));

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

      putAndVerify(objectType, currentServer1, regionName, 0, 10, currentServer2,
          oldServerAndLocator, oldServer2);
      putAndVerify(objectType, oldServerAndLocator, regionName, 5, 15, currentServer1,
          currentServer2, oldServer2);

    } finally {
      invokeRunnableInVMs(true, invokeCloseCache(), currentServer1, currentServer2,
          oldServerAndLocator, oldServer2);
    }
  }

  void doTestQueryMixedServers(boolean partitioned, String oldVersion) throws Exception {
    final Host host = Host.getHost(0);
    VM currentServer1 = host.getVM(VersionManager.CURRENT_VERSION, 0);
    VM oldServer = host.getVM(oldVersion, 1);
    VM currentServer2 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    VM oldServerAndLocator = host.getVM(oldVersion, 3);

    String regionName = "cqs";

    RegionShortcut shortcut = RegionShortcut.REPLICATE;
    if (partitioned) {
      shortcut = RegionShortcut.PARTITION;
    }

    String serverHostName = NetworkUtils.getServerHostName();
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    try {
      Properties props = getSystemProperties();
      props.remove(DistributionConfig.LOCATORS_NAME);
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
      invokeRunnableInVMs(invokeCreateCache(props), currentServer1, currentServer2, oldServer);

      currentServer1.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      currentServer2.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));

      // create region
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), currentServer1, currentServer2,
          oldServer, oldServerAndLocator);

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      oldServerAndLocator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      putDataSerializableAndVerify(currentServer1, regionName, 0, 100, currentServer2, oldServer,
          oldServerAndLocator);
      query("Select * from /" + regionName + " p where p.timeout > 0L", 99, currentServer1,
          currentServer2, oldServer, oldServerAndLocator);

    } finally {
      invokeRunnableInVMs(invokeCloseCache(), currentServer1, currentServer2, oldServer,
          oldServerAndLocator);
    }
  }

  void doTestCreateIndexes(boolean createMultiIndexes, boolean partitioned,
      String oldVersion) throws Exception {
    final Host host = Host.getHost(0);
    final VM currentServer1 = host.getVM(VersionManager.CURRENT_VERSION, 0);
    final VM oldServer = host.getVM(oldVersion, 1);
    final VM currentServer2 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    final VM oldServerAndLocator = host.getVM(oldVersion, 3);

    String regionName = "cqs";

    RegionShortcut shortcut = RegionShortcut.REPLICATE;
    if (partitioned) {
      shortcut = RegionShortcut.PARTITION;
    }

    String serverHostName = NetworkUtils.getServerHostName();
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    try {
      Properties props = getSystemProperties();
      props.remove(DistributionConfig.LOCATORS_NAME);
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
      invokeRunnableInVMs(invokeCreateCache(props), currentServer1, currentServer2, oldServer);

      currentServer1.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      currentServer2.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));

      // create region
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), currentServer1, currentServer2,
          oldServer, oldServerAndLocator);

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      oldServerAndLocator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      putDataSerializableAndVerify(currentServer1, regionName, 0, 100, currentServer2, oldServer,
          oldServerAndLocator);
      if (createMultiIndexes) {
        doCreateIndexes("/" + regionName, currentServer1);
      } else {
        doCreateIndex("/" + regionName, oldServer);
      }
    } finally {
      invokeRunnableInVMs(invokeCloseCache(), currentServer1, currentServer2, oldServer,
          oldServerAndLocator);
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
  void putAndVerify(String objectType, VM putter, String regionName, int start, int end,
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

  private void putStringsAndVerify(VM putter, String regionName, int start, int end, VM... vms) {
    for (int i = start; i < end; i++) {
      putter.invoke(invokePut(regionName, "" + i, "VALUE(" + i + ")"));
    }

    // verify present in others
    for (VM vm : vms) {
      vm.invoke(invokeAssertEntriesCorrect(regionName, start, end));
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

  void putDataSerializableAndVerify(VM putter, String regionName, int start, int end,
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

  void verifyValues(String objectType, String regionName, int start, int end, VM... vms) {
    if (objectType.equals("strings")) {
      for (VM vm : vms) {
        vm.invoke(invokeAssertEntriesCorrect(regionName, start, end));
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

  void executeFunctionAndVerify(String functionId, String dsClassName,
      DistributedMember... members) {
    Set<DistributedMember> membersSet = new HashSet<>();
    Collections.addAll(membersSet, members);
    Execution execution = FunctionService.onMembers(membersSet).withArgs(dsClassName);
    ResultCollector rc = execution.execute(functionId);
    List result = (List) rc.getResult();
    assertEquals(membersSet.size(), result.size());
    for (Iterator i = result.iterator(); i.hasNext();) {
      assertTrue(i.next().getClass().getName().equals(dsClassName));
    }
  }

  protected void query(String queryString, int numExpectedResults, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(invokeAssertQueryResults(queryString, numExpectedResults));
    }
  }

  private void doCreateIndexes(String regionPath, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(invokeCreateIndexes(regionPath));
    }
  }

  private void doCreateIndex(String regionPath, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(invokeCreateIndex(regionPath));
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

  private VM rollClientToCurrent(VM oldClient, String[] hostNames, int[] locatorPorts,
      boolean subscriptionEnabled) {
    oldClient.invoke(invokeCloseCache());
    VM rollClient = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, oldClient.getId());
    rollClient.invoke(invokeCreateClientCache(getClientSystemProperties(), hostNames, locatorPorts,
        subscriptionEnabled));
    rollClient.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
    return rollClient;
  }

  VM rollServerToCurrentAndCreateRegion(VM oldServer, RegionShortcut shortcut,
      String regionName, int[] locatorPorts) {
    VM newServer = rollServerToCurrent(oldServer, locatorPorts);
    // recreate region on "rolled" server
    invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), newServer);
    newServer.invoke(invokeRebalance());
    return newServer;
  }

  VM rollServerToCurrentAndCreateRegion(VM oldServer, String regionType, File diskdir,
      RegionShortcut shortcut, String regionName, int[] locatorPorts) {
    VM rollServer = rollServerToCurrent(oldServer, locatorPorts);
    // recreate region on "rolled" server
    if ((regionType.equals("persistentReplicate"))) {
      CacheSerializableRunnable runnable =
          invokeCreatePersistentReplicateRegion(regionName, diskdir);
      invokeRunnableInVMs(runnable, rollServer);
    } else {
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), rollServer);
    }
    rollServer.invoke(invokeRebalance());
    return rollServer;
  }

  VM rollClientToCurrentAndCreateRegion(VM oldClient, ClientRegionShortcut shortcut,
      String regionName, String[] hostNames, int[] locatorPorts, boolean subscriptionEnabled) {
    VM rollClient = rollClientToCurrent(oldClient, hostNames, locatorPorts, subscriptionEnabled);
    // recreate region on "rolled" client
    invokeRunnableInVMs(invokeCreateClientRegion(regionName, shortcut), rollClient);
    return rollClient;
  }

  VM rollLocatorToCurrent(VM rollLocator, final String serverHostName, final int port,
      final String testName, final String locatorString) {
    // Roll the locator
    rollLocator.invoke(invokeStopLocator());
    VM newLocator = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, rollLocator.getId());
    newLocator.invoke(invokeStartLocator(serverHostName, port, testName,
        getLocatorProperties(locatorString), false));
    return newLocator;
  }

  // Due to licensing changes
  private Properties getSystemPropertiesPost71() {
    Properties props = getSystemProperties();
    props.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
    props.put(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    return props;
  }

  // Due to licensing changes
  private Properties getSystemPropertiesPost71(int[] locatorPorts) {
    Properties props = getSystemProperties(locatorPorts);
    props.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
    props.put(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    return props;
  }

  public Properties getSystemProperties() {
    Properties props = DistributedTestUtils.getAllDistributedSystemProperties(new Properties());
    props.remove("disable-auto-reconnect");
    props.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
    props.put(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    props.remove(DistributionConfig.LOAD_CLUSTER_CONFIG_FROM_DIR_NAME);
    props.remove(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME);
    props.remove(DistributionConfig.LOCK_MEMORY_NAME);
    return props;
  }

  public Properties getSystemProperties(int[] locatorPorts) {
    Properties props = new Properties();
    String locatorString = getLocatorString(locatorPorts);
    props.setProperty("locators", locatorString);
    props.setProperty("mcast-port", "0");
    props.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
    props.put(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    props.remove(DistributionConfig.LOAD_CLUSTER_CONFIG_FROM_DIR_NAME);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, DUnitLauncher.logLevel);
    return props;
  }

  Properties getClientSystemProperties() {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0");
    p.setProperty(DistributionConfig.LOG_LEVEL_NAME, DUnitLauncher.logLevel);
    return p;
  }

  private static String getLocatorString(int locatorPort) {
    return getDUnitLocatorAddress() + "[" + locatorPort + "]";
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

  CacheSerializableRunnable invokeStartLocator(final String serverHostName, final int port,
      final String testName, final Properties props, boolean fastStart) {
    return new CacheSerializableRunnable("execute: startLocator") {
      public void run2() {
        try {
          startLocator(serverHostName, port, props, fastStart);
        } catch (Exception e) {
          fail("Error starting locators", e);
        }
      }
    };
  }

  CacheSerializableRunnable invokeStartLocatorAndServer(final String serverHostName,
      final int port, final Properties systemProperties) {
    return new CacheSerializableRunnable("execute: startLocator") {
      public void run2() {
        try {
          systemProperties.put(DistributionConfig.START_LOCATOR_NAME,
              "" + serverHostName + "[" + port + "]");
          systemProperties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
          RollingUpgrade2DUnitTestBase.cache = createCache(systemProperties);
          Thread.sleep(5000); // bug in 1.0 - cluster config service not immediately available
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
          RollingUpgrade2DUnitTestBase.cache = createCache(systemProperties);
        } catch (Exception e) {
          fail("Error creating cache", e);
        }
      }
    };
  }

  CacheSerializableRunnable invokeCreateClientCache(final Properties systemProperties,
      final String[] hosts, final int[] ports, boolean subscriptionEnabled) {
    return new CacheSerializableRunnable("execute: createClientCache") {
      public void run2() {
        try {
          RollingUpgrade2DUnitTestBase.cache =
              createClientCache(systemProperties, hosts, ports, subscriptionEnabled);
        } catch (Exception e) {
          fail("Error creating client cache", e);
        }
      }
    };
  }

  CacheSerializableRunnable invokeStartCacheServer(final int port) {
    return new CacheSerializableRunnable("execute: startCacheServer") {
      public void run2() {
        try {
          startCacheServer(RollingUpgrade2DUnitTestBase.cache, port);
        } catch (Exception e) {
          fail("Error creating cache", e);
        }
      }
    };
  }

  CacheSerializableRunnable invokeAssertVersion(final short version) {
    return new CacheSerializableRunnable("execute: assertVersion") {
      public void run2() {
        try {
          assertVersion(RollingUpgrade2DUnitTestBase.cache, version);
        } catch (Exception e) {
          fail("Error asserting version", e);
        }
      }
    };
  }

  CacheSerializableRunnable invokeCreateRegion(final String regionName,
      final RegionShortcut shortcut) {
    return new CacheSerializableRunnable("execute: createRegion") {
      public void run2() {
        try {
          createRegion(RollingUpgrade2DUnitTestBase.cache, regionName, shortcut);
        } catch (Exception e) {
          fail("Error createRegion", e);
        }
      }
    };
  }

  CacheSerializableRunnable invokeCreatePersistentReplicateRegion(final String regionName,
      final File diskstore) {
    return new CacheSerializableRunnable("execute: createPersistentReplicateRegion") {
      public void run2() {
        try {
          createPersistentReplicateRegion(RollingUpgrade2DUnitTestBase.cache, regionName,
              diskstore);
        } catch (Exception e) {
          fail("Error createPersistentReplicateRegion", e);
        }
      }
    };
  }

  CacheSerializableRunnable invokeCreateClientRegion(final String regionName,
      final ClientRegionShortcut shortcut) {
    return new CacheSerializableRunnable("execute: createClientRegion") {
      public void run2() {
        try {
          createClientRegion(RollingUpgrade2DUnitTestBase.cache, regionName, shortcut);
        } catch (Exception e) {
          fail("Error creating client region", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokePut(final String regionName, final Object key,
      final Object value) {
    return new CacheSerializableRunnable("execute: put(" + key + "," + value + ")") {
      public void run2() {
        try {
          put(RollingUpgrade2DUnitTestBase.cache, regionName, key, value);
        } catch (Exception e) {
          fail("Error put", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeAssertEntriesCorrect(final String regionName,
      final int start, final int end) {
    return new CacheSerializableRunnable("execute: assertEntriesCorrect") {
      public void run2() {
        try {
          assertEntriesCorrect(RollingUpgrade2DUnitTestBase.cache, regionName, start, end);
        } catch (Exception e) {
          fail("Error asserting equals", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeAssertEntriesExist(final String regionName,
      final int start, final int end) {
    return new CacheSerializableRunnable("execute: assertEntryExists") {
      public void run2() {
        try {
          assertEntryExists(RollingUpgrade2DUnitTestBase.cache, regionName, start, end);
        } catch (Exception e) {
          fail("Error asserting exists", e);
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
          fail("Error stopping locator", e);
        }
      }
    };
  }

  CacheSerializableRunnable invokeCloseCache() {
    return new CacheSerializableRunnable("execute: closeCache") {
      public void run2() {
        try {
          closeCache(RollingUpgrade2DUnitTestBase.cache);
        } catch (Exception e) {
          fail("Error closing cache", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeRebalance() {
    return new CacheSerializableRunnable("execute: rebalance") {
      public void run2() {
        try {
          rebalance(RollingUpgrade2DUnitTestBase.cache);
        } catch (Exception e) {
          fail("Error rebalancing", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeAssertQueryResults(final String queryString,
      final int numExpected) {
    return new CacheSerializableRunnable("execute: assertQueryResults") {
      public void run2() {
        try {
          assertQueryResults(RollingUpgrade2DUnitTestBase.cache, queryString, numExpected);
        } catch (Exception e) {
          fail("Error asserting query results", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCreateIndexes(final String regionPath) {
    return new CacheSerializableRunnable("invokeCreateIndexes") {
      public void run2() {
        try {
          createIndexes(regionPath, RollingUpgrade2DUnitTestBase.cache);
        } catch (Exception e) {
          fail("Error creating indexes ", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCreateIndex(final String regionPath) {
    return new CacheSerializableRunnable("invokeCreateIndexes") {
      public void run2() {
        try {
          createIndex(regionPath, RollingUpgrade2DUnitTestBase.cache);
        } catch (Exception e) {
          fail("Error creating indexes ", e);
        }
      }
    };
  }

  CacheSerializableRunnable invokeRegisterFunction(final Function function) {
    return new CacheSerializableRunnable("invokeRegisterFunction") {
      public void run2() {
        try {
          registerFunction(function);
        } catch (Exception e) {
          fail("Error registering function ", e);
        }
      }
    };
  }

  void deleteDiskStores() {
    try {
      FileUtils.deleteDirectory(new File(diskDir).getAbsoluteFile());
    } catch (IOException e) {
      throw new Error("Error deleting files", e);
    }
  }

  private static Cache createCache(Properties systemProperties) {
    systemProperties.setProperty(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    if (Version.CURRENT_ORDINAL < 75) {
      systemProperties.remove("validate-serializable-objects");
      systemProperties.remove("serializable-object-filter");
    }
    CacheFactory cf = new CacheFactory(systemProperties);
    return cf.create();
  }

  private static void startCacheServer(GemFireCache cache, int port) throws Exception {
    CacheServer cacheServer = ((GemFireCacheImpl) cache).addCacheServer();
    cacheServer.setPort(port);
    cacheServer.start();
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

    ClientCache clientCache = cf.create();
    // the pool is lazily created starting in 1.5.0. Here we ask for the pool so it
    // will be instantiated
    clientCache.getDefaultPool();
    return clientCache;
  }

  private static boolean assertRegionExists(GemFireCache cache, String regionName) {
    Region region = cache.getRegion(regionName);
    if (region == null) {
      throw new Error("Region: " + regionName + " does not exist");
    }
    return true;
  }

  private static Region getRegion(GemFireCache cache, String regionName) {
    return cache.getRegion(regionName);
  }

  public static DistributedMember getDistributedMember() {
    return cache.getDistributedSystem().getDistributedMember();
  }

  private static boolean assertEntriesCorrect(GemFireCache cache, String regionName, int start,
      int end) {
    assertRegionExists(cache, regionName);
    Region region = getRegion(cache, regionName);
    for (int i = start; i < end; i++) {
      String key = "" + i;
      Object regionValue = region.get(key);
      if (regionValue == null) {
        fail("Region value does not exist for key:" + key);
      }
      String value = "VALUE(" + i + ")";
      if (!regionValue.equals(value)) {
        fail("Entry for key:" + key + " does not equal value: " + value);
      }
    }
    return true;
  }

  private static boolean assertEntryExists(GemFireCache cache, String regionName, int start,
      int end) {
    assertRegionExists(cache, regionName);
    Region region = getRegion(cache, regionName);
    for (int i = start; i < end; i++) {
      String key = "" + i;
      Object regionValue = region.get(key);
      if (regionValue == null) {
        fail("Entry for key:" + key + " does not exist");
      }
    }
    return true;
  }

  public static Object put(GemFireCache cache, String regionName, Object key, Object value) {
    Region region = getRegion(cache, regionName);
    System.out.println(regionName + ".put(" + key + "," + value + ")");
    Object result = region.put(key, value);
    System.out.println("returned " + result);
    return result;
  }

  private static void createRegion(GemFireCache cache, String regionName, RegionShortcut shortcut) {
    RegionFactory rf = ((GemFireCacheImpl) cache).createRegionFactory(shortcut);
    System.out.println("created region " + rf.create(regionName));
  }

  // Assumes a client cache is passed
  private static void createClientRegion(GemFireCache cache, String regionName,
      ClientRegionShortcut shortcut) {
    ClientRegionFactory rf = ((ClientCache) cache).createClientRegionFactory(shortcut);
    rf.create(regionName);
  }

  private static void createPersistentReplicateRegion(GemFireCache cache, String regionName,
      File diskStore) {
    DiskStore store = cache.findDiskStore("store");
    if (store == null) {
      DiskStoreFactory factory = cache.createDiskStoreFactory();
      factory.setMaxOplogSize(1L);
      factory.setDiskDirs(new File[] {diskStore.getAbsoluteFile()});
      factory.create("store");
    }
    RegionFactory rf = ((GemFireCacheImpl) cache).createRegionFactory();
    rf.setDiskStoreName("store");
    rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    rf.create(regionName);
  }

  private static void assertVersion(GemFireCache cache, short ordinal) {
    DistributedSystem system = cache.getDistributedSystem();
    int thisOrdinal =
        ((InternalDistributedMember) system.getDistributedMember()).getVersionObject().ordinal();
    if (ordinal != thisOrdinal) {
      throw new Error(
          "Version ordinal:" + thisOrdinal + " was not the expected ordinal of:" + ordinal);
    }
  }

  private static void assertQueryResults(GemFireCache cache, String queryString,
      int numExpectedResults) {
    try {
      QueryService qs = cache.getQueryService();
      Query query = qs.newQuery(queryString);
      SelectResults results = (SelectResults) query.execute();
      int numResults = results.size();
      if (numResults != numExpectedResults) {
        System.out.println("Num Results was:" + numResults);
        throw new Error("Num results:" + numResults + " != num expected:" + numExpectedResults);
      }
    } catch (Exception e) {
      throw new Error("Query Exception ", e);
    }
  }

  private static void createIndexes(String regionPath, GemFireCache cache) throws Exception {
    QueryService service = cache.getQueryService();
    service.defineIndex("statusIndex", "status", regionPath);
    service.defineIndex("IDIndex", "ID", regionPath);
    service.defineIndex("secIdIndex", "pos.secId", regionPath + " p, p.positions.values pos");
    try {
      service.createDefinedIndexes();
      fail("Index creation should have failed");
    } catch (MultiIndexCreationException e) {
      Assert.assertEquals("3 exceptions should have be present in the exceptionsMap.", 3,
          e.getExceptionsMap().values().size());
      for (Exception ex : e.getExceptionsMap().values()) {
        Assert.assertTrue("Index creation should have been failed with IndexCreationException ",
            ex instanceof IndexCreationException);
        Assert.assertEquals("Incorrect exception message ",
            "Indexes should not be created when there are older versions of gemfire in the cluster.",
            ex.getMessage());
      }
    } catch (Exception e) {
      throw new Exception(
          "Only MultiIndexCreationException should have been thrown and not " + e.getClass(), e);
    }
  }

  private static void createIndex(String regionPath, GemFireCache cache) {
    try {
      QueryService service = cache.getQueryService();
      service.createIndex("statusIndex", "status", regionPath);
      service.createIndex("IDIndex", "ID", regionPath);
      service.createIndex("secIdIndex", "pos.secId", regionPath + " p, p.positions.values pos");

      Collection<Index> indexes = service.getIndexes();
      int numResults = indexes.size();

      if (numResults != 3) {
        System.out.println("Num Results was:" + numResults);
        throw new Error("Num indexes created:" + numResults + " != num expected:" + 3);
      }

    } catch (Exception e) {
      throw new Error("Exception ", e);
    }
  }

  private static void registerFunction(Function function) {
    FunctionService.registerFunction(function);
  }

  private static void stopCacheServers(GemFireCache cache) {
    List<CacheServer> servers = ((Cache) cache).getCacheServers();
    for (CacheServer server : servers) {
      server.stop();
    }
  }

  private static void closeCache(GemFireCache cache) {
    if (cache == null) {
      return;
    }
    boolean cacheClosed = cache.isClosed();
    if (!cacheClosed) {
      stopCacheServers(cache);
      cache.close();
    }
    cache = null;
  }

  private static void rebalance(Object cache) throws Exception {
    RebalanceOperation op =
        ((GemFireCache) cache).getResourceManager().createRebalanceFactory().start();

    // Wait until the rebalance is complete
    RebalanceResults results = op.getResults();
    Method getTotalTimeMethod = results.getClass().getMethod("getTotalTime");
    getTotalTimeMethod.setAccessible(true);
    System.out.println("Took " + results.getTotalTime() + " milliseconds\n");
    System.out.println("Transfered " + results.getTotalBucketTransferBytes() + "bytes\n");
  }

  Properties getLocatorProperties(String locatorsString) {
    return getLocatorProperties(locatorsString, true);
  }

  Properties getLocatorProperties(String locatorsString, boolean enableCC) {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locatorsString);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, DUnitLauncher.logLevel);
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, enableCC + "");
    return props;
  }

  /**
   * Starts a locator with given configuration.
   */
  private static void startLocator(final String serverHostName, final int port, Properties props,
      boolean fastStart) throws Exception {


    InetAddress bindAddr = null;
    try {
      bindAddr = InetAddress.getByName(serverHostName);
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
    if (Version.CURRENT == Version.GFE_90) {
      Thread.sleep(5000); // bug in 1.0 - cluster config service not immediately available
    }
  }

  private static void stopLocator() {
    InternalLocator.getLocator().stop();
  }

  /**
   * Get the port that the standard dunit locator is listening on.
   *
   */
  private static String getDUnitLocatorAddress() {
    return Host.getHost(0).getHostName();
  }

  String getHARegionName() {
    InternalCache internalCache = (InternalCache) cache;
    await().untilAsserted(() -> {
      assertEquals(1, internalCache.getCacheServers().size());
      CacheServerImpl bs = (CacheServerImpl) (internalCache.getCacheServers().iterator().next());
      assertEquals(1, bs.getAcceptor().getCacheClientNotifier().getClientProxies().size());
    });
    CacheServerImpl bs = (CacheServerImpl) internalCache.getCacheServers().iterator().next();
    CacheClientProxy ccp =
        bs.getAcceptor().getCacheClientNotifier().getClientProxies().iterator().next();
    return ccp.getHARegion().getName();
  }

  public static class GetDataSerializableFunction implements Function, DataSerializable {

    GetDataSerializableFunction() {}

    @Override
    public void execute(FunctionContext context) {
      String dsClassName = (String) context.getArguments();
      try {
        Class aClass = Thread.currentThread().getContextClassLoader().loadClass(dsClassName);
        Constructor constructor = aClass.getConstructor();
        context.getResultSender().lastResult(constructor.newInstance());
      } catch (Exception e) {
        throw new FunctionException(e);
      }
    }

    @Override
    public String getId() {
      return GetDataSerializableFunction.class.getName();
    }

    @Override
    public void toData(DataOutput out) throws IOException {

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    }
  }
}
