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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.ServerVersionMismatchException;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.VersionManager;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RollingUpgradePartitionRegionClearMixedServerPartitionedRegion
    extends JUnit4DistributedTestCase {

  protected static final Logger logger = LogService.getLogger();
  protected static GemFireCache cache;
  protected static ClientCache clientcache;

  @Parameter
  public String oldVersion;

  @Parameters(name = "from_v{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    } else {
      System.out.println("running against these versions: " + result);
    }
    return result;
  }

  @Test
  public void testPutAndGetMixedServerPartitionedRegion() throws Exception {
    doTestPutAndGetMixedServers(oldVersion);
  }

  /**
   * This test starts up multiple servers from the current code base and multiple servers from the
   * old version and executes puts and gets on a new server and old server and verifies that the
   * results are present. Note that the puts have overlapping region keys just to test new puts and
   * replaces
   */
  void doTestPutAndGetMixedServers(String oldVersion)
      throws Exception {
    VM currentServer1 = VM.getVM(VersionManager.CURRENT_VERSION, 0);
    VM oldServerAndLocator = VM.getVM(oldVersion, 1);
    VM currentServer2 = VM.getVM(VersionManager.CURRENT_VERSION, 2);
    VM oldServer2 = VM.getVM(oldVersion, 3);

    String regionName = "aRegion";

    final String serverHostName = NetworkUtils.getServerHostName();
    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    oldServerAndLocator.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(port));
    try {
      final Properties props = getSystemProperties();
      props.remove(DistributionConfig.LOCATORS_NAME);

      // Fire up the locator and server
      oldServerAndLocator.invoke(() -> {
        props.put(DistributionConfig.START_LOCATOR_NAME,
            "" + serverHostName + "[" + port + "]");
        props.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
        cache = createCache(props);
        Thread.sleep(5000); // bug in 1.0 - cluster config service not immediately available
      });

      props.put(DistributionConfig.LOCATORS_NAME, serverHostName + "[" + port + "]");

      // create the cache in all the server VMs.
      for (VM vm : Arrays.asList(oldServer2, currentServer1, currentServer2)) {
        vm.invoke(() -> {
          cache = createCache(props);
        });
      }
      // spin up current version servers
      for (VM vm : Arrays.asList(currentServer1, currentServer2)) {
        vm.invoke(
            () -> assertVersion(cache, VersionManager.getInstance().getCurrentVersionOrdinal()));
      }

      // create region
      for (VM vm : Arrays.asList(currentServer1, currentServer2, oldServerAndLocator, oldServer2)) {
        vm.invoke(() -> createRegion(cache, regionName));
      }

      // put some data in the region to make sure there is something to clear.
      putDataSerializableAndVerify(currentServer1, regionName, currentServer2, oldServerAndLocator,
          oldServer2);

      // invoke Partition Region Clear and verify we didn't touch the old servers.

      currentServer1.invoke(() -> {
        assertRegionExists(cache, regionName);
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);

        Throwable thrown = catchThrowable(region::clear);
        assertThat(thrown).isInstanceOf(ServerVersionMismatchException.class);

      });
    } finally {
      for (VM vm : Arrays.asList(currentServer1, currentServer2, oldServerAndLocator, oldServer2)) {
        vm.invoke(
            () -> closeCache(RollingUpgradePartitionRegionClearMixedServerPartitionedRegion.cache));
      }
    }
  }

  @Test
  public void TestClientServerGetsUnsupportedExceptionWhenPRClearInvoked() throws Exception {
    doTestClientServerGetsUnsupportedExceptionWhenPRClearInvoked(oldVersion);
  }

  void doTestClientServerGetsUnsupportedExceptionWhenPRClearInvoked(String oldVersion)
      throws Exception {

    VM client = VM.getVM(VersionManager.CURRENT_VERSION, 0);
    VM locator = VM.getVM(VersionManager.CURRENT_VERSION, 1);
    VM currentServer = VM.getVM(VersionManager.CURRENT_VERSION, 2);
    VM oldServer2 = VM.getVM(oldVersion, 3);

    for (VM vm : Arrays.asList(locator, currentServer, client)) {
      vm.invoke(() -> System.setProperty("gemfire.allow_old_members_to_join_for_testing", "true"));
    }

    String regionName = "aRegion";

    final String serverHostName = NetworkUtils.getServerHostName();
    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    locator.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(port));
    try {
      final Properties props = getSystemProperties();
      props.remove(DistributionConfig.LOCATORS_NAME);

      // Fire up the locator and server
      locator.invoke(() -> {
        props.put(DistributionConfig.START_LOCATOR_NAME,
            "" + serverHostName + "[" + port + "]");
        props.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
        cache = createCache(props);
      });

      props.put(DistributionConfig.LOCATORS_NAME, serverHostName + "[" + port + "]");

      // create the cache in all the server VMs.
      for (VM vm : Arrays.asList(oldServer2, currentServer)) {
        vm.invoke(() -> {
          props.setProperty(DistributionConfig.NAME_NAME, "vm" + VM.getVMId());
          cache = createCache(props);
        });
      }
      int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

      oldServer2.invoke(() -> startCacheServer(cache, ports[0]));
      currentServer.invoke(() -> startCacheServer(cache, ports[1]));

      // create region
      for (VM vm : Arrays.asList(currentServer, locator, oldServer2)) {
        vm.invoke(() -> createRegion(cache, regionName));
      }

      // put some data in the region to make sure there is something to clear.
      putDataSerializableAndVerify(currentServer, regionName, locator, oldServer2);

      // invoke Partition Region Clear from the client and verify the exception.
      client.invoke(() -> {
        clientcache = new ClientCacheFactory().addPoolServer(serverHostName, ports[1]).create();
        Region<Object, Object> clientRegion = clientcache.createClientRegionFactory(
            ClientRegionShortcut.PROXY).create(regionName);

        clientRegion.put("key", "value");

        Throwable thrown = catchThrowable(clientRegion::clear);
        assertThat(thrown).isInstanceOf(ServerOperationException.class);
        assertThat(thrown).hasCauseInstanceOf(ServerVersionMismatchException.class);
        ServerVersionMismatchException serverVersionMismatchException =
            (ServerVersionMismatchException) thrown.getCause();
        assertThat(serverVersionMismatchException.getMessage()).contains("vm3");
      });

    } finally {

      for (VM vm : Arrays.asList(currentServer, locator, oldServer2)) {
        vm.invoke(() -> closeCache(cache));
      }

      client.invoke(() -> {
        if (cache != null && !clientcache.isClosed()) {
          clientcache.close(false);
        }
      });
    }
  }

  private String getLocatorString(int locatorPort) {
    return getDUnitLocatorAddress() + "[" + locatorPort + "]";
  }

  public String getLocatorString(int[] locatorPorts) {
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

  private Cache createCache(Properties systemProperties) {
    systemProperties.setProperty(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    if (VersionManager.getInstance().getCurrentVersionOrdinal() < 75) {
      systemProperties.remove("validate-serializable-objects");
      systemProperties.remove("serializable-object-filter");
    }
    CacheFactory cf = new CacheFactory(systemProperties);
    return cf.create();
  }

  private void startCacheServer(GemFireCache cache, int port) throws Exception {
    CacheServer cacheServer = ((GemFireCacheImpl) cache).addCacheServer();
    cacheServer.setPort(port);
    cacheServer.start();
  }

  protected void assertRegionExists(GemFireCache cache, String regionName) {
    Region<Object, Object> region = cache.getRegion(regionName);
    if (region == null) {
      throw new Error("Region: " + regionName + " does not exist");
    }
  }

  private void assertEntryExists(GemFireCache cache, String regionName) {
    assertRegionExists(cache, regionName);
    Region<Object, Object> region = cache.getRegion(regionName);
    for (int i = 0; i < 10; i++) {
      String key = "" + i;
      Object regionValue = region.get(key);
      assertThat(regionValue).describedAs("Entry for key:" + key + " does not exist").isNotNull();
    }
  }

  public void put(GemFireCache cache, String regionName, Object key, Object value) {
    Region<Object, Object> region = cache.getRegion(regionName);
    System.out.println(regionName + ".put(" + key + "," + value + ")");
    Object result = region.put(key, value);
    System.out.println("returned " + result);
  }

  private void createRegion(GemFireCache cache, String regionName) {
    RegionFactory<Object, Object> rf = ((GemFireCacheImpl) cache).createRegionFactory(
        RegionShortcut.PARTITION);
    System.out.println("created region " + rf.create(regionName));
  }

  void assertVersion(GemFireCache cache, short ordinal) {
    DistributedSystem system = cache.getDistributedSystem();
    int thisOrdinal =
        ((InternalDistributedMember) system.getDistributedMember()).getVersion()
            .ordinal();
    if (ordinal != thisOrdinal) {
      throw new Error(
          "Version ordinal:" + thisOrdinal + " was not the expected ordinal of:" + ordinal);
    }
  }

  private void closeCache(GemFireCache cache) {
    if (cache == null) {
      return;
    }
    boolean cacheClosed = cache.isClosed();
    if (!cacheClosed) {
      List<CacheServer> servers = ((Cache) cache).getCacheServers();
      for (CacheServer server : servers) {
        server.stop();
      }
      cache.close();
    }
  }

  /**
   * Get the port that the standard dunit locator is listening on.
   *
   */
  private String getDUnitLocatorAddress() {
    return Host.getHost(0).getHostName();
  }

  private void deleteVMFiles() {
    System.out.println("deleting files in vm" + VM.getVMId());
    File pwd = new File(".");
    for (File entry : pwd.listFiles()) {
      try {
        if (entry.isDirectory()) {
          FileUtils.deleteDirectory(entry);
        } else {
          if (!entry.delete()) {
            System.out.println("Could not delete " + entry);
          }
        }
      } catch (Exception e) {
        System.out.println("Could not delete " + entry + ": " + e.getMessage());
      }
    }
  }

  @Override
  public void postSetUp() {
    Invoke.invokeInEveryVM("delete files", this::deleteVMFiles);
    IgnoredException.addIgnoredException(
        "cluster configuration service not available|ConflictingPersistentDataException");
  }


  void putDataSerializableAndVerify(VM putter, String regionName,
      VM... vms) throws Exception {
    for (int i = 0; i < 10; i++) {
      Class aClass = Thread.currentThread().getContextClassLoader()
          .loadClass("org.apache.geode.cache.ExpirationAttributes");
      Constructor constructor = aClass.getConstructor(int.class);
      Object testDataSerializable = constructor.newInstance(i);
      int finalI = i;
      putter.invoke(() -> put(cache, regionName, "" + finalI, testDataSerializable));
    }

    // verify present in others
    for (VM vm : vms) {
      vm.invoke(() -> assertEntryExists(cache, regionName));
    }
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
}
