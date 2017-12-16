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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.DiskInitFile;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.Oplog;
import org.apache.geode.internal.cache.Oplog.OPLOG_TYPE;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.DistributedTest;
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
@Category({DistributedTest.class, BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RollingUpgrade2DUnitTest extends JUnit4DistributedTestCase {

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

  // just a test flag that can be set when trying to run a test in eclipse and avoiding IllegalState
  // or NPE due to bouncing
  private boolean turnOffBounce = false;

  private File[] testingDirs = new File[3];

  // This will be the classloader for each specific VM if the
  // VM requires a classloader to load an older gemfire jar version/tests
  // Use this classloader to obtain a #RollingUpgradeUtils class and
  // execute the helper methods from that class
  private static ClassLoader classLoader;

  private static String diskDir = "RollingUpgrade2DUnitTest";

  // Each vm will have a cache object
  private static GemFireCache cache;

  private String oldVersion;

  public RollingUpgrade2DUnitTest(String version) {
    oldVersion = version;
  }

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

  @Override
  public void postSetUp() throws Exception {
    Invoke.invokeInEveryVM("delete files", () -> deleteVMFiles());
    IgnoredException.addIgnoredException(
        "cluster configuration service not available|ConflictingPersistentDataException");
  }


  @Test
  public void testRollSingleLocatorWithMultipleServersReplicatedRegion() throws Exception {
    doTestRollSingleLocatorWithMultipleServers(false, oldVersion);
  }

  // 1 locator, 3 servers
  public void doTestRollSingleLocatorWithMultipleServers(boolean partitioned, String oldVersion)
      throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(oldVersion, 0);
    VM server2 = host.getVM(oldVersion, 1);
    VM server3 = host.getVM(oldVersion, 2);
    VM server4 = host.getVM(oldVersion, 3);

    final String objectType = "strings";
    final String regionName = "aRegion";

    RegionShortcut shortcut = RegionShortcut.REPLICATE;
    if (partitioned) {
      shortcut = RegionShortcut.PARTITION_REDUNDANT;
    }

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    DistributedTestUtils.deleteLocatorStateFile(locatorPorts[0]);

    // configure all class loaders for each vm

    String hostName = NetworkUtils.getServerHostName(host);
    String locatorString = getLocatorString(locatorPorts);
    try {
      server1.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorString)));
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server2, server3,
          server4);
      // invokeRunnableInVMs(invokeAssertVersion(oldOrdinal), server2, server3, server4);
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), server2, server3, server4);

      putAndVerify(objectType, server2, regionName, 0, 10, server3, server4);
      server1 = rollLocatorToCurrent(server1, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);

      server2 = rollServerToCurrentAndCreateRegion(server2, shortcut, regionName, locatorPorts);
      putAndVerify(objectType, server2, regionName, 5, 15, server3, server4);
      putAndVerify(objectType, server3, regionName, 10, 20, server2, server4);

      server3 = rollServerToCurrentAndCreateRegion(server3, shortcut, regionName, locatorPorts);
      putAndVerify(objectType, server2, regionName, 15, 25, server3, server4);
      putAndVerify(objectType, server3, regionName, 20, 30, server2, server4);

      server4 = rollServerToCurrentAndCreateRegion(server4, shortcut, regionName, locatorPorts);
      putAndVerify(objectType, server4, regionName, 25, 35, server3, server2);
      putAndVerify(objectType, server3, regionName, 30, 40, server2, server4);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), server1);
      invokeRunnableInVMs(true, invokeCloseCache(), server2, server3, server4);
    }
  }

  /**
   * Replicated regions
   */
  @Test
  public void testRollTwoLocatorsWithTwoServers() throws Exception {
    doTestRollTwoLocatorsWithTwoServers(false, oldVersion);
  }

  // 2 locator, 2 servers
  public void doTestRollTwoLocatorsWithTwoServers(boolean partitioned, String oldVersion)
      throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(oldVersion, 0);
    VM server2 = host.getVM(oldVersion, 1);
    VM server3 = host.getVM(oldVersion, 2);
    VM server4 = host.getVM(oldVersion, 3);

    final String objectType = "strings";
    final String regionName = "aRegion";

    RegionShortcut shortcut = RegionShortcut.REPLICATE;
    if (partitioned) {
      shortcut = RegionShortcut.PARTITION_REDUNDANT;
    }

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

    String hostName = NetworkUtils.getServerHostName(host);
    String locatorString = getLocatorString(locatorPorts);
    try {
      server1.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorString)));
      server2.invoke(invokeStartLocator(hostName, locatorPorts[1], getTestMethodName(),
          getLocatorProperties(locatorString)));
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server3, server4);
      // invokeRunnableInVMs(invokeAssertVersion(oldOrdinal), server3, server4);
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), server3, server4);

      putAndVerify(objectType, server3, regionName, 0, 10, server3, server4);
      server1 = rollLocatorToCurrent(server1, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);

      server2 = rollLocatorToCurrent(server2, hostName, locatorPorts[1], getTestMethodName(),
          locatorString);

      server3 = rollServerToCurrentAndCreateRegion(server3, shortcut, regionName, locatorPorts);
      putAndVerify(objectType, server4, regionName, 15, 25, server3, server4);
      putAndVerify(objectType, server3, regionName, 20, 30, server3, server4);

      server4 = rollServerToCurrentAndCreateRegion(server4, shortcut, regionName, locatorPorts);
      putAndVerify(objectType, server4, regionName, 25, 35, server3, server4);
      putAndVerify(objectType, server3, regionName, 30, 40, server3, server4);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), server1, server2);
      invokeRunnableInVMs(true, invokeCloseCache(), server3, server4);
    }
  }

  @Test
  public void testClients() throws Exception {
    doTestClients(false, oldVersion);
  }

  public void doTestClients(boolean partitioned, String oldVersion) throws Exception {
    final Host host = Host.getHost(0);
    VM locator = host.getVM(oldVersion, 0);
    VM server2 = host.getVM(oldVersion, 1);
    VM server3 = host.getVM(oldVersion, 2);
    VM client = host.getVM(oldVersion, 3);

    final String objectType = "strings";
    final String regionName = "aRegion";

    RegionShortcut shortcut = RegionShortcut.REPLICATE;
    if (partitioned) {
      shortcut = RegionShortcut.PARTITION_REDUNDANT;
    }

    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    int[] locatorPorts = new int[] {ports[0]};
    int[] csPorts = new int[] {ports[1], ports[2]};

    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

    String hostName = NetworkUtils.getServerHostName(host);
    String[] hostNames = new String[] {hostName};
    String locatorString = getLocatorString(locatorPorts);
    try {
      locator.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorString)));
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server2, server3);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[0]), server2);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[1]), server3);

      invokeRunnableInVMs(
          invokeCreateClientCache(getClientSystemProperties(), hostNames, locatorPorts, false),
          client);
      // invokeRunnableInVMs(invokeAssertVersion(oldOrdinal), server2, server3, client);
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), server2, server3);
      invokeRunnableInVMs(invokeCreateClientRegion(regionName, ClientRegionShortcut.PROXY), client);

      putAndVerify(objectType, client, regionName, 0, 10, server3, client);
      putAndVerify(objectType, server3, regionName, 100, 110, server3, client);
      locator = rollLocatorToCurrent(locator, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);

      server3 = rollServerToCurrentAndCreateRegion(server3, shortcut, regionName, locatorPorts);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[1]), server3);
      putAndVerify(objectType, client, regionName, 15, 25, server3, client);
      putAndVerify(objectType, server3, regionName, 20, 30, server3, client);

      server2 = rollServerToCurrentAndCreateRegion(server2, shortcut, regionName, locatorPorts);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[0]), server2);
      putAndVerify(objectType, client, regionName, 25, 35, server2, client);
      putAndVerify(objectType, server2, regionName, 30, 40, server3, client);

      client = rollClientToCurrentAndCreateRegion(client, ClientRegionShortcut.PROXY, regionName,
          hostNames, locatorPorts, false);
      putAndVerify(objectType, client, regionName, 35, 45, server2, server3);
      putAndVerify(objectType, server2, regionName, 40, 50, server3, client);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server2, server3, client);
    }
  }

  /**
   * starts 3 locators and 1 server rolls all 3 locators and then the server
   */
  @Test
  public void testRollLocatorsWithOldServer() throws Exception {
    doTestRollLocatorsWithOldServer(oldVersion);
  }

  public void doTestRollLocatorsWithOldServer(String oldVersion) throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(oldVersion, 0);
    VM server2 = host.getVM(oldVersion, 1);
    VM server3 = host.getVM(oldVersion, 2);
    VM server4 = host.getVM(oldVersion, 3);

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

    String hostName = NetworkUtils.getServerHostName(host);
    String locatorString = getLocatorString(locatorPorts);
    try {
      server1.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorString)));
      server2.invoke(invokeStartLocator(hostName, locatorPorts[1], getTestMethodName(),
          getLocatorProperties(locatorString)));
      server3.invoke(invokeStartLocator(hostName, locatorPorts[2], getTestMethodName(),
          getLocatorProperties(locatorString)));
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server4);
      // invokeRunnableInVMs(invokeAssertVersion(oldOrdinal), server4);

      server1 = rollLocatorToCurrent(server1, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);
      server2 = rollLocatorToCurrent(server2, hostName, locatorPorts[1], getTestMethodName(),
          locatorString);
      server3 = rollLocatorToCurrent(server3, hostName, locatorPorts[2], getTestMethodName(),
          locatorString);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), server1, server2, server3);
      invokeRunnableInVMs(true, invokeCloseCache(), server4);
    }
  }

  /**
   * A test that will start 4 locators and rolls each one
   */
  @Test
  public void testRollLocators() throws Exception {
    doTestRollLocators(oldVersion);
  }

  public void doTestRollLocators(String oldVersion) throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(oldVersion, 0);
    VM server2 = host.getVM(oldVersion, 1);
    VM server3 = host.getVM(oldVersion, 2);
    VM server4 = host.getVM(oldVersion, 3);

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(4);
    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

    String hostName = NetworkUtils.getServerHostName(host);
    String locatorString = getLocatorString(locatorPorts);
    try {
      server1.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorString)));
      server2.invoke(invokeStartLocator(hostName, locatorPorts[1], getTestMethodName(),
          getLocatorProperties(locatorString)));
      server3.invoke(invokeStartLocator(hostName, locatorPorts[2], getTestMethodName(),
          getLocatorProperties(locatorString)));
      server4.invoke(invokeStartLocator(hostName, locatorPorts[3], getTestMethodName(),
          getLocatorProperties(locatorString)));

      server1 = rollLocatorToCurrent(server1, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);
      server2 = rollLocatorToCurrent(server2, hostName, locatorPorts[1], getTestMethodName(),
          locatorString);
      server3 = rollLocatorToCurrent(server3, hostName, locatorPorts[2], getTestMethodName(),
          locatorString);
      server4 = rollLocatorToCurrent(server4, hostName, locatorPorts[3], getTestMethodName(),
          locatorString);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), server1, server2, server3, server4);
    }

  }

  /**
   * Starts 2 servers with old classloader puts in one server while the other bounces verifies
   * values are present in bounced server puts in the newly started/bounced server and bounces the
   * other server verifies values are present in newly bounced server
   */
  @Test
  public void testConcurrentPutsReplicated() throws Exception {
    doTestConcurrent(false, oldVersion);
  }

  public void doTestConcurrent(boolean partitioned, String oldVersion) throws Exception {
    Host host = Host.getHost(0);
    VM locator = host.getVM(oldVersion, 1);
    VM server1 = host.getVM(oldVersion, 2);
    VM server2 = host.getVM(oldVersion, 3);

    final String objectType = "strings";
    final String regionName = "aRegion";

    RegionShortcut shortcut = RegionShortcut.REPLICATE;
    if (partitioned) {
      shortcut = RegionShortcut.PARTITION_REDUNDANT;
    }

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    String hostName = NetworkUtils.getServerHostName(host);
    String locatorString = getLocatorString(locatorPorts);

    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

    try {
      locator.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorString)));
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server1, server2);
      // invokeRunnableInVMs(invokeAssertVersion(oldOrdinal), server1, server2);
      // create region
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), server1, server2);

      // async puts through server 2
      AsyncInvocation asyncPutsThroughOld =
          server2.invokeAsync(new CacheSerializableRunnable("async puts") {
            public void run2() {
              try {
                for (int i = 0; i < 500; i++) {
                  put(RollingUpgrade2DUnitTest.cache, regionName, "" + i, "VALUE(" + i + ")");
                }
              } catch (Exception e) {
                fail("error putting");
              }
            }
          });
      locator = rollLocatorToCurrent(locator, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);

      server1 = rollServerToCurrentAndCreateRegion(server1, shortcut, regionName, locatorPorts);
      ThreadUtils.join(asyncPutsThroughOld, 30000);

      // verifyValues in server1
      verifyValues(objectType, regionName, 0, 500, server1);

      // aync puts through server 1
      AsyncInvocation asyncPutsThroughNew =
          server1.invokeAsync(new CacheSerializableRunnable("async puts") {
            public void run2() {
              try {
                for (int i = 250; i < 750; i++) {
                  put(RollingUpgrade2DUnitTest.cache, regionName, "" + i, "VALUE(" + i + ")");
                }
              } catch (Exception e) {
                fail("error putting");
              }
            }
          });
      server2 = rollServerToCurrentAndCreateRegion(server2, shortcut, regionName, locatorPorts);
      ThreadUtils.join(asyncPutsThroughNew, 30000);

      // verifyValues in server2
      verifyValues(objectType, regionName, 250, 750, server2);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2);
    }
  }

  // TODO file a JIRA ticket
  // java.lang.AssertionError
  // at org.junit.Assert.fail(Assert.java:86)
  // at org.junit.Assert.assertTrue(Assert.java:41)
  // at org.junit.Assert.assertFalse(Assert.java:64)
  // at org.junit.Assert.assertFalse(Assert.java:74)
  // at
  // org.apache.geode.internal.cache.rollingupgrade.RollingUpgrade2DUnitTest.verifyOplogHeader(RollingUpgrade2DUnitTest.java:633)
  // at
  // org.apache.geode.internal.cache.rollingupgrade.RollingUpgrade2DUnitTest.testOplogMagicSeqBackwardCompactibility(RollingUpgrade2DUnitTest.java:568)

  @Ignore("GEODE-2355: test fails consistently")
  @Test
  public void testOplogMagicSeqBackwardCompactibility() throws Exception {
    String objectType = "strings";
    String regionType = "persistentReplicate";


    final Host host = Host.getHost(0);
    VM server1 = host.getVM(oldVersion, 0);
    VM server2 = host.getVM(oldVersion, 1);
    VM server3 = host.getVM(oldVersion, 2);
    VM locator = host.getVM(oldVersion, 3);

    String regionName = "aRegion";
    RegionShortcut shortcut = RegionShortcut.REPLICATE_PERSISTENT;
    for (int i = 0; i < testingDirs.length; i++) {
      testingDirs[i] = new File(diskDir, "diskStoreVM_" + String.valueOf(host.getVM(i).getId()))
          .getAbsoluteFile();
      if (!testingDirs[i].exists()) {
        System.out.println(" Creating diskdir for server: " + i);
        testingDirs[i].mkdirs();
      }
    }

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    String hostName = NetworkUtils.getServerHostName(host);
    String locatorsString = getLocatorString(locatorPorts);

    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

    try {
      locator.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorsString)));

      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server1, server2,
          server3);
      // invokeRunnableInVMs(invokeAssertVersion(oldOrdinal), server1, server2, server3);
      // create region
      for (int i = 0; i < testingDirs.length; i++) {
        CacheSerializableRunnable runnable =
            invokeCreatePersistentReplicateRegion(regionName, testingDirs[i]);
        invokeRunnableInVMs(runnable, host.getVM(i));
      }

      putAndVerify("strings", server1, regionName, 0, 10, server2, server3);
      // before upgrade headers will be absent
      HashSet<String> oldFormatFiles = verifyOplogHeader(testingDirs[0], null);
      locator = rollLocatorToCurrent(locator, hostName, locatorPorts[0], getTestMethodName(),
          locatorsString);
      server1 = rollServerToCurrentAndCreateRegion(server1, regionType, testingDirs[0], shortcut,
          regionName, locatorPorts);
      System.out.println(verifyOplogHeader(testingDirs[0], oldFormatFiles));
      verifyValues(objectType, regionName, 0, 10, server1);
      putAndVerify(objectType, server1, regionName, 5, 15, server2, server3);
      putAndVerify(objectType, server2, regionName, 10, 20, server1, server3);
      System.out.println(verifyOplogHeader(testingDirs[0], oldFormatFiles));
      System.out.println(verifyOplogHeader(testingDirs[1], null));
    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2, server3);
      if ((regionType.equals("persistentReplicate"))) {
        deleteDiskStores();
      }
    }
  }

  private HashSet<String> verifyOplogHeader(File dir, HashSet<String> oldFiles) throws IOException {
    if (oldFiles != null) {
      for (String file : oldFiles) {
        System.out.println("Known old format file: " + file);
      }
    }

    File[] files = dir.listFiles();
    HashSet<String> verified = new HashSet<String>();
    HashSet<String> oldFilesFound = new HashSet<String>();
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
        // } else if (name.endsWith(".krf")) {
        // expect[0] = OPLOG_MAGIC_SEQ_ID;
        // System.arraycopy(OPLOG_TYPE.KRF.getBytes(), 0, expect, 1, OPLOG_TYPE.getLen());
        // verified.add(".krf");
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

  @Test
  public void testPutAndGetMixedServersReplicateRegion() throws Exception {
    doTestPutAndGetMixedServers("strings", false, oldVersion);
    doTestPutAndGetMixedServers("serializable", false, oldVersion);
    doTestPutAndGetMixedServers("dataserializable", false, oldVersion);
  }

  @Test
  public void testPutAndGetMixedServerPartitionedRegion() throws Exception {
    doTestPutAndGetMixedServers("strings", true, oldVersion);
    doTestPutAndGetMixedServers("serializable", true, oldVersion);
    doTestPutAndGetMixedServers("dataserializable", true, oldVersion);
  }

  /**
   * Demonstrate that an old process can't join a system that has upgraded locators. This is for
   * bugs #50510 and #50742.
   */
  @Test
  public void testOldMemberCantJoinRolledLocators() throws Exception {
    VM oldServer = Host.getHost(0).getVM(oldVersion, 1);
    Properties props = getSystemProperties(); // uses the DUnit locator
    try {
      oldServer.invoke(invokeCreateCache(props));
    } catch (RMIException e) {
      Throwable cause = e.getCause();
      if (cause != null && (cause instanceof AssertionError)) {
        cause = cause.getCause();
        if (cause != null && cause.getMessage() != null && !cause.getMessage().startsWith(
            "Rejecting the attempt of a member using an older version of the product to join the distributed system")) {
          throw e;
        }
      }
    }
  }



  /**
   * This test starts up multiple servers from the current code base and multiple servers from the
   * old version and executes puts and gets on a new server and old server and verifies that the
   * results are present. Note that the puts have overlapping region keys just to test new puts and
   * replaces
   */
  public void doTestPutAndGetMixedServers(String objectType, boolean partitioned, String oldVersion)
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

    String serverHostName = NetworkUtils.getServerHostName(Host.getHost(0));
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    DistributedTestUtils.deleteLocatorStateFile(port);
    try {
      Properties props = getSystemProperties();
      props.remove(DistributionConfig.LOCATORS_NAME);
      invokeRunnableInVMs(invokeStartLocatorAndServer(serverHostName, port, props),
          oldServerAndLocator);

      props.put(DistributionConfig.LOCATORS_NAME, serverHostName + "[" + port + "]");

      invokeRunnableInVMs(invokeCreateCache(props), oldServer2, currentServer1, currentServer2);

      currentServer1.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      currentServer2.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      // oldServerAndLocator.invoke(invokeAssertVersion(oldOrdinal));
      // oldServer2.invoke(invokeAssertVersion(oldOrdinal));

      // create region
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), currentServer1, currentServer2,
          oldServerAndLocator, oldServer2);

      putAndVerify(objectType, currentServer1, regionName, 0, 10, currentServer2,
          oldServerAndLocator, oldServer2);
      putAndVerify(objectType, oldServerAndLocator, regionName, 5, 15, currentServer1,
          currentServer2, oldServer2);

    } finally {
      invokeRunnableInVMs(true, invokeCloseCache(), currentServer1, currentServer2,
          oldServerAndLocator, oldServer2);
    }
  }

  @Test
  public void testQueryMixedServersOnReplicatedRegions() throws Exception {
    doTestQueryMixedServers(false, oldVersion);
  }

  @Test
  public void testQueryMixedServersOnPartitionedRegions() throws Exception {
    doTestQueryMixedServers(true, oldVersion);
  }

  // TODO file a JIRA ticket
  @Ignore("GEODE_2356: test fails when index creation succeeds")
  @Test
  public void testCreateMultiIndexesMixedServersOnPartitionedRegions() throws Exception {
    doTestCreateIndexes(true, true, oldVersion);
  }

  @Test
  public void testCreateIndexesMixedServersOnPartitionedRegions() throws Exception {
    doTestCreateIndexes(false, true, oldVersion);
  }

  public void doTestQueryMixedServers(boolean partitioned, String oldVersion) throws Exception {
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

    String serverHostName = NetworkUtils.getServerHostName(Host.getHost(0));
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    try {
      Properties props = getSystemProperties();
      props.remove(DistributionConfig.LOCATORS_NAME);
      invokeRunnableInVMs(invokeStartLocatorAndServer(serverHostName, port, props),
          oldServerAndLocator);

      props.put(DistributionConfig.LOCATORS_NAME, serverHostName + "[" + port + "]");
      invokeRunnableInVMs(invokeCreateCache(props), currentServer1, currentServer2, oldServer);

      currentServer1.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      currentServer2.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      // oldServer.invoke(invokeAssertVersion(oldOrdinal));
      // oldServerAndLocator.invoke(invokeAssertVersion(oldOrdinal));

      // create region
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), currentServer1, currentServer2,
          oldServer, oldServerAndLocator);

      putDataSerializableAndVerify(currentServer1, regionName, 0, 100, currentServer2, oldServer,
          oldServerAndLocator);
      query("Select * from /" + regionName + " p where p.timeout > 0L", 99, currentServer1,
          currentServer2, oldServer, oldServerAndLocator);

    } finally {
      invokeRunnableInVMs(invokeCloseCache(), currentServer1, currentServer2, oldServer,
          oldServerAndLocator);
    }
  }

  @Test
  public void testTracePRQuery() throws Exception {
    doTestTracePRQuery(true, oldVersion);
  }

  public void doTestTracePRQuery(boolean partitioned, String oldVersion) throws Exception {
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

    String serverHostName = NetworkUtils.getServerHostName(Host.getHost(0));
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    try {
      Properties props = getSystemProperties();
      props.remove(DistributionConfig.LOCATORS_NAME);
      invokeRunnableInVMs(invokeStartLocatorAndServer(serverHostName, port, props),
          oldServerAndLocator);

      props.put(DistributionConfig.LOCATORS_NAME, serverHostName + "[" + port + "]");
      invokeRunnableInVMs(invokeCreateCache(props), currentServer1, currentServer2, oldServer);

      currentServer1.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      currentServer2.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      // oldServer.invoke(invokeAssertVersion(oldOrdinal));
      // oldServerAndLocator.invoke(invokeAssertVersion(oldOrdinal));

      // create region
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), currentServer1, currentServer2,
          oldServer, oldServerAndLocator);

      putDataSerializableAndVerify(currentServer1, regionName, 0, 100, currentServer2, oldServer,
          oldServerAndLocator);
      query("<trace> Select * from /" + regionName + " p where p.timeout > 0L", 99, currentServer1,
          currentServer2, oldServer, oldServerAndLocator);

    } finally {
      invokeRunnableInVMs(invokeCloseCache(), currentServer1, currentServer2, oldServer,
          oldServerAndLocator);
    }
  }

  public void doTestCreateIndexes(boolean createMultiIndexes, boolean partitioned,
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

    String serverHostName = NetworkUtils.getServerHostName(Host.getHost(0));
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    try {
      Properties props = getSystemProperties();
      props.remove(DistributionConfig.LOCATORS_NAME);
      invokeRunnableInVMs(invokeStartLocatorAndServer(serverHostName, port, props),
          oldServerAndLocator);

      props.put(DistributionConfig.LOCATORS_NAME, serverHostName + "[" + port + "]");
      invokeRunnableInVMs(invokeCreateCache(props), currentServer1, currentServer2, oldServer);

      currentServer1.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      currentServer2.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      // oldServer.invoke(invokeAssertVersion(oldOrdinal));
      // oldServerAndLocator.invoke(invokeAssertVersion(oldOrdinal));

      // create region
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), currentServer1, currentServer2,
          oldServer, oldServerAndLocator);

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

  // This is test was used to test for changes on objects that used
  // java serialization. Left in just to have in case we want to do more manual testing
  // public void _testSerialization() throws Exception {
  // final Host host = Host.getHost(0);
  // final VM currentServer = host.getVM(0);
  // final VM oldServer = host.getVM(1);
  //
  // String oldVersionLocation = gemfireLocations[0].gemfireLocation;
  // short oldOrdinal = gemfireLocations[0].ordinal;
  //
  // //configure all class loaders for each vm
  // currentServer.invoke(configureClassLoaderForCurrent());
  // oldServer.invoke(configureClassLoader(oldVersionLocation));
  //
  // doTestSerialization(currentServer, oldServer);
  //
  // currentServer.invoke(configureClassLoaderForCurrent());
  // oldServer.invoke(configureClassLoader(oldVersionLocation));
  // doTestSerialization(oldServer, currentServer);
  // }
  //
  // public void doTestSerialization(VM vm1, VM vm2) throws Exception {
  //
  // try {
  // final byte[] bytes = (byte[])vm1.invoke(new SerializableCallable("serialize") {
  // public Object call() {
  // try {
  // ByteOutputStream byteArray = new ByteOutputStream();
  // ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
  // Thread.currentThread().setContextClassLoader(RollingUpgrade2DUnitTest.classLoader);
  // ClassLoader loader = RollingUpgrade2DUnitTest.classLoader;
  //
  // Class clazzSerializer = loader.loadClass("org.apache.geode.internal.InternalDataSerializer");
  // Method m = clazzSerializer.getMethod("writeObject", Object.class, DataOutput.class);
  // VersionedDataOutputStream stream = new VersionedDataOutputStream(byteArray, Version.GFE_7099);
  //
  // Class exceptionClass =
  // loader.loadClass("org.apache.geode.internal.cache.ForceReattemptException");
  // m.invoke(null, exceptionClass.getConstructor(String.class).newInstance("TEST ME"), stream);
  // m.invoke(null, exceptionClass.getConstructor(String.class).newInstance("TEST ME2"), stream);
  // Thread.currentThread().setContextClassLoader(ogLoader);
  // return byteArray.getBytes();
  // }
  // catch (Exception e) {
  // e.printStackTrace();
  //
  // fail("argh");
  // }
  // return null;
  // }
  // });
  //
  // vm2.invoke(new CacheSerializableRunnable("deserialize") {
  // public void run2() {
  // try {
  // ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
  // Thread.currentThread().setContextClassLoader(RollingUpgrade2DUnitTest.classLoader);
  // ClassLoader loader = RollingUpgrade2DUnitTest.classLoader;
  // Class clazz = loader.loadClass("org.apache.geode.internal.InternalDataSerializer");
  // Method m = clazz.getMethod("readObject", DataInput.class);
  // VersionedDataInputStream stream = new VersionedDataInputStream(new ByteArrayInputStream(bytes),
  // Version.GFE_71);
  //
  // Object e = m.invoke(null, stream);
  // Object e2 = m.invoke(null, stream);
  //
  // Class exceptionClass =
  // loader.loadClass("org.apache.geode.internal.cache.ForceReattemptException");
  // exceptionClass.cast(e);
  // exceptionClass.cast(e2);
  // Thread.currentThread().setContextClassLoader(ogLoader);
  // }
  // catch (Exception e) {
  // e.printStackTrace();
  // fail("argh");
  // }
  // }
  // });
  //
  // } finally {
  // invokeRunnableInVMs(true, resetClassLoader(), vm1, vm2);
  // bounceAll(vm1, vm2);
  // }
  // }



  @Test
  // This test verifies that an XmlEntity created in the current version serializes properly to
  // previous versions and vice versa.
  public void testVerifyXmlEntity() throws Exception {
    doTestVerifyXmlEntity(oldVersion);
  }

  private void doTestVerifyXmlEntity(String oldVersion) throws Exception {
    final Host host = Host.getHost(0);
    VM oldLocator = host.getVM(oldVersion, 0);
    VM oldServer = host.getVM(oldVersion, 1);
    VM currentServer1 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    VM currentServer2 = host.getVM(VersionManager.CURRENT_VERSION, 3);

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    String hostName = NetworkUtils.getServerHostName(host);
    String locatorsString = getLocatorString(locatorPorts);
    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

    try {
      // Start locator
      oldLocator.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorsString, false)));

      // Start servers
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), oldServer,
          currentServer1, currentServer2);
      currentServer1.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      currentServer2.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));

      // Get DistributedMembers of the servers
      DistributedMember oldServerMember = oldServer.invoke(() -> getDistributedMember());
      DistributedMember currentServer1Member = currentServer1.invoke(() -> getDistributedMember());
      DistributedMember currentServer2Member = currentServer2.invoke(() -> getDistributedMember());

      // Register function in all servers
      Function function = new GetDataSerializableFunction();
      invokeRunnableInVMs(invokeRegisterFunction(function), oldServer, currentServer1,
          currentServer2);

      // Execute the function in the old server against the other servers to verify the
      // DataSerializable can be serialized from a newer server to an older one.
      oldServer.invoke(() -> executeFunctionAndVerify(function.getId(),
          "org.apache.geode.management.internal.configuration.domain.XmlEntity",
          currentServer1Member, currentServer2Member));

      // Execute the function in a new server against the other servers to verify the
      // DataSerializable can be serialized from an older server to a newer one.
      currentServer1.invoke(() -> executeFunctionAndVerify(function.getId(),
          "org.apache.geode.management.internal.configuration.domain.XmlEntity", oldServerMember,
          currentServer2Member));
    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), oldLocator);
      invokeRunnableInVMs(true, invokeCloseCache(), oldServer, currentServer1, currentServer2);
    }
  }

  @Test
  public void testHARegionNameOnDifferentServerVersions() throws Exception {
    doTestHARegionNameOnDifferentServerVersions(false, oldVersion);
  }

  public void doTestHARegionNameOnDifferentServerVersions(boolean partitioned, String oldVersion)
      throws Exception {
    final Host host = Host.getHost(0);
    VM locator = host.getVM(oldVersion, 0);
    VM server1 = host.getVM(oldVersion, 1);
    VM server2 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    VM client = host.getVM(oldVersion, 3);

    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    int[] locatorPorts = new int[] {ports[0]};
    int[] csPorts = new int[] {ports[1], ports[2]};

    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

    String hostName = NetworkUtils.getServerHostName(host);
    String[] hostNames = new String[] {hostName};
    String locatorString = getLocatorString(locatorPorts);
    try {
      locator.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorString, false)));
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server1, server2);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[0]), server1);
      invokeRunnableInVMs(invokeStartCacheServer(csPorts[1]), server2);

      invokeRunnableInVMs(
          invokeCreateClientCache(getClientSystemProperties(), hostNames, locatorPorts, true),
          client);

      // Get HARegion name on server1
      String server1HARegionName = server1.invoke(() -> getHARegionName());

      // Get HARegionName on server2
      String server2HARegionName = server2.invoke(() -> getHARegionName());

      // Verify they are equal
      assertEquals(server1HARegionName, server2HARegionName);
    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2, client);
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

  private void executeFunctionAndVerify(String functionId, String dsClassName,
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

  private void query(String queryString, int numExpectedResults, VM... vms) {
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

  private VM rollClientToCurrent(VM oldClient, String[] hostNames, int[] locatorPorts,
      boolean subscriptionEnabled) throws Exception {
    oldClient.invoke(invokeCloseCache());
    VM rollClient = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, oldClient.getId());
    rollClient.invoke(invokeCreateClientCache(getClientSystemProperties(), hostNames, locatorPorts,
        subscriptionEnabled));
    rollClient.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
    return rollClient;
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
  private VM rollServerToCurrentAndCreateRegion(VM oldServer, RegionShortcut shortcut,
      String regionName, int[] locatorPorts) throws Exception {
    VM newServer = rollServerToCurrent(oldServer, locatorPorts);
    // recreate region on "rolled" server
    invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), newServer);
    newServer.invoke(invokeRebalance());
    return newServer;
  }

  private VM rollServerToCurrentAndCreateRegion(VM oldServer, String regionType, File diskdir,
      RegionShortcut shortcut, String regionName, int[] locatorPorts) throws Exception {
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

  /*
   * @param rollClient
   *
   * @param createRegionMethod
   *
   * @param regionName
   *
   * @param locatorPorts if null, uses dunit locator
   *
   * @throws Exception
   */
  private VM rollClientToCurrentAndCreateRegion(VM oldClient, ClientRegionShortcut shortcut,
      String regionName, String[] hostNames, int[] locatorPorts, boolean subscriptionEnabled)
      throws Exception {
    VM rollClient = rollClientToCurrent(oldClient, hostNames, locatorPorts, subscriptionEnabled);
    // recreate region on "rolled" client
    invokeRunnableInVMs(invokeCreateClientRegion(regionName, shortcut), rollClient);
    return rollClient;
  }

  private VM rollLocatorToCurrent(VM rollLocator, final String serverHostName, final int port,
      final String testName, final String locatorString) throws Exception {
    // Roll the locator
    rollLocator.invoke(invokeStopLocator());
    VM newLocator = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, rollLocator.getId());
    newLocator.invoke(
        invokeStartLocator(serverHostName, port, testName, getLocatorProperties(locatorString)));
    return newLocator;
  }

  // Due to licensing changes
  public Properties getSystemPropertiesPost71() {
    Properties props = getSystemProperties();
    props.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
    props.put(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    return props;
  }

  // Due to licensing changes
  public Properties getSystemPropertiesPost71(int[] locatorPorts) {
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
      final String testName, final Properties props) {
    return new CacheSerializableRunnable("execute: startLocator") {
      public void run2() {
        try {
          startLocator(serverHostName, port, testName, props);
        } catch (Exception e) {
          fail("Error starting locators", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeStartLocatorAndServer(final String serverHostName,
      final int port, final Properties systemProperties) {
    return new CacheSerializableRunnable("execute: startLocator") {
      public void run2() {
        try {
          systemProperties.put(DistributionConfig.START_LOCATOR_NAME,
              "" + serverHostName + "[" + port + "]");
          systemProperties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
          RollingUpgrade2DUnitTest.cache = createCache(systemProperties);
          Thread.sleep(5000); // bug in 1.0 - cluster config service not immediately available
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
          RollingUpgrade2DUnitTest.cache = createCache(systemProperties);
        } catch (Exception e) {
          fail("Error creating cache", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCreateClientCache(final Properties systemProperties,
      final String[] hosts, final int[] ports, boolean subscriptionEnabled) {
    return new CacheSerializableRunnable("execute: createClientCache") {
      public void run2() {
        try {
          RollingUpgrade2DUnitTest.cache =
              createClientCache(systemProperties, hosts, ports, subscriptionEnabled);
        } catch (Exception e) {
          fail("Error creating client cache", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeStartCacheServer(final int port) {
    return new CacheSerializableRunnable("execute: startCacheServer") {
      public void run2() {
        try {
          startCacheServer(RollingUpgrade2DUnitTest.cache, port);
        } catch (Exception e) {
          fail("Error creating cache", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeAssertVersion(final short version) {
    return new CacheSerializableRunnable("execute: assertVersion") {
      public void run2() {
        try {
          assertVersion(RollingUpgrade2DUnitTest.cache, version);
        } catch (Exception e) {
          fail("Error asserting version", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCreateRegion(final String regionName,
      final RegionShortcut shortcut) {
    return new CacheSerializableRunnable("execute: createRegion") {
      public void run2() {
        try {
          createRegion(RollingUpgrade2DUnitTest.cache, regionName, shortcut);
        } catch (Exception e) {
          fail("Error createRegion", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCreatePersistentReplicateRegion(final String regionName,
      final File diskstore) {
    return new CacheSerializableRunnable("execute: createPersistentReplicateRegion") {
      public void run2() {
        try {
          createPersistentReplicateRegion(RollingUpgrade2DUnitTest.cache, regionName, diskstore);
        } catch (Exception e) {
          fail("Error createPersistentReplicateRegion", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCreateClientRegion(final String regionName,
      final ClientRegionShortcut shortcut) {
    return new CacheSerializableRunnable("execute: createClientRegion") {
      public void run2() {
        try {
          createClientRegion(RollingUpgrade2DUnitTest.cache, regionName, shortcut);
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
          put(RollingUpgrade2DUnitTest.cache, regionName, key, value);
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
          assertEntriesCorrect(RollingUpgrade2DUnitTest.cache, regionName, start, end);
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
          assertEntryExists(RollingUpgrade2DUnitTest.cache, regionName, start, end);
        } catch (Exception e) {
          fail("Error asserting exists", e);
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
          fail("Error stopping locator", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeCloseCache() {
    return new CacheSerializableRunnable("execute: closeCache") {
      public void run2() {
        try {
          closeCache(RollingUpgrade2DUnitTest.cache);
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
          rebalance(RollingUpgrade2DUnitTest.cache);
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
          assertQueryResults(RollingUpgrade2DUnitTest.cache, queryString, numExpected);
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
          createIndexes(regionPath, RollingUpgrade2DUnitTest.cache);
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
          createIndex(regionPath, RollingUpgrade2DUnitTest.cache);
        } catch (Exception e) {
          fail("Error creating indexes ", e);
        }
      }
    };
  }

  private CacheSerializableRunnable invokeRegisterFunction(final Function function) {
    return new CacheSerializableRunnable("invokeRegisterFunction") {
      public void run2() {
        try {
          registerFunction(function, RollingUpgrade2DUnitTest.cache);
        } catch (Exception e) {
          fail("Error registering function ", e);
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

  public static Cache createCache(Properties systemProperties) throws Exception {
    systemProperties.setProperty(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    if (Version.CURRENT_ORDINAL < 75) {
      systemProperties.remove("validate-serializable-objects");
      systemProperties.remove("serializable-object-filter");
    }
    CacheFactory cf = new CacheFactory(systemProperties);
    return cf.create();
  }

  public static void startCacheServer(GemFireCache cache, int port) throws Exception {
    CacheServer cacheServer = ((GemFireCacheImpl) cache).addCacheServer();
    cacheServer.setPort(port);
    cacheServer.start();
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

  public static boolean assertRegionExists(GemFireCache cache, String regionName) throws Exception {
    Region region = cache.getRegion(regionName);
    if (region == null) {
      throw new Error("Region: " + regionName + " does not exist");
    }
    return true;
  }

  public static Region getRegion(GemFireCache cache, String regionName) throws Exception {
    return cache.getRegion(regionName);
  }

  public static DistributedMember getDistributedMember() {
    return cache.getDistributedSystem().getDistributedMember();
  }

  public static boolean assertEntriesCorrect(GemFireCache cache, String regionName, int start,
      int end) throws Exception {
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

  public static boolean assertEntryExists(GemFireCache cache, String regionName, int start, int end)
      throws Exception {
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

  public static Object put(GemFireCache cache, String regionName, Object key, Object value)
      throws Exception {
    Region region = getRegion(cache, regionName);
    System.out.println(regionName + ".put(" + key + "," + value + ")");
    Object result = region.put(key, value);
    System.out.println("returned " + result);
    return result;
  }

  public static void createRegion(GemFireCache cache, String regionName, RegionShortcut shortcut)
      throws Exception {
    RegionFactory rf = ((GemFireCacheImpl) cache).createRegionFactory(shortcut);
    System.out.println("created region " + rf.create(regionName));
  }

  public static void createPartitionedRegion(GemFireCache cache, String regionName)
      throws Exception {
    createRegion(cache, regionName, RegionShortcut.PARTITION);
  }

  public static void createPartitionedRedundantRegion(GemFireCache cache, String regionName)
      throws Exception {
    createRegion(cache, regionName, RegionShortcut.PARTITION_REDUNDANT);
  }

  public static void createReplicatedRegion(GemFireCache cache, String regionName)
      throws Exception {
    createRegion(cache, regionName, RegionShortcut.REPLICATE);
  }

  // Assumes a client cache is passed
  public static void createClientRegion(GemFireCache cache, String regionName,
      ClientRegionShortcut shortcut) throws Exception {
    ClientRegionFactory rf = ((ClientCache) cache).createClientRegionFactory(shortcut);
    rf.create(regionName);
  }

  public static void createRegion(String regionName, RegionFactory regionFactory) throws Exception {
    regionFactory.create(regionName);
  }

  public static void createPersistentReplicateRegion(GemFireCache cache, String regionName,
      File diskStore) throws Exception {
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

  public static void assertVersion(GemFireCache cache, short ordinal) throws Exception {
    DistributedSystem system = cache.getDistributedSystem();
    int thisOrdinal =
        ((InternalDistributedMember) system.getDistributedMember()).getVersionObject().ordinal();
    if (ordinal != thisOrdinal) {
      throw new Error(
          "Version ordinal:" + thisOrdinal + " was not the expected ordinal of:" + ordinal);
    }
  }

  public static void assertQueryResults(GemFireCache cache, String queryString,
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

  public static void createIndexes(String regionPath, GemFireCache cache) {
    try {
      QueryService service = cache.getQueryService();
      service.defineIndex("statusIndex", "status", regionPath);
      service.defineIndex("IDIndex", "ID", regionPath);
      service.defineIndex("secIdIndex", "pos.secId", regionPath + " p, p.positions.values pos");
      try {
        service.createDefinedIndexes();
        fail("Index creation should have failed");
      } catch (Exception e) {
        Assert.assertTrue(
            "Only MultiIndexCreationException should have been thrown and not " + e.getClass(),
            e instanceof MultiIndexCreationException);
        Assert.assertEquals("3 exceptions should have be present in the exceptionsMap.", 3,
            ((MultiIndexCreationException) e).getExceptionsMap().values().size());
        for (Exception ex : ((MultiIndexCreationException) e).getExceptionsMap().values()) {
          Assert.assertTrue("Index creation should have been failed with IndexCreationException ",
              ex instanceof IndexCreationException);
          Assert.assertEquals("Incorrect exception message ",
              LocalizedStrings.PartitionedRegion_INDEX_CREATION_FAILED_ROLLING_UPGRADE
                  .toLocalizedString(),
              ((IndexCreationException) ex).getMessage());
        }
      }
    } catch (Exception e) {
      throw new Error("Exception ", e);
    }
  }

  public static void createIndex(String regionPath, GemFireCache cache) {
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

  public static void registerFunction(Function function, GemFireCache cache) {
    FunctionService.registerFunction(function);
  }

  public static void stopCacheServers(GemFireCache cache) throws Exception {
    List<CacheServer> servers = ((Cache) cache).getCacheServers();
    for (CacheServer server : servers) {
      server.stop();
    }
  }

  public static void closeCache(GemFireCache cache) throws Exception {
    if (cache == null) {
      return;
    }
    boolean cacheClosed = cache.isClosed();
    if (cache != null && !cacheClosed) {
      stopCacheServers(cache);
      cache.close();
    }
    cache = null;
  }

  public static void rebalance(Object cache) throws Exception {
    RebalanceOperation op =
        ((GemFireCache) cache).getResourceManager().createRebalanceFactory().start();

    // Wait until the rebalance is completex
    RebalanceResults results = op.getResults();
    Method getTotalTimeMethod = results.getClass().getMethod("getTotalTime");
    getTotalTimeMethod.setAccessible(true);
    System.out.println("Took " + results.getTotalTime() + " milliseconds\n");
    System.out.println("Transfered " + results.getTotalBucketTransferBytes() + "bytes\n");
  }

  public Properties getLocatorProperties(String locatorsString) {
    return getLocatorProperties(locatorsString, true);
  }

  public Properties getLocatorProperties(String locatorsString, boolean enableCC) {
    Properties props = new Properties();
    // props.setProperty(DistributionConfig.NAME_NAME, getUniqueName());
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locatorsString);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, DUnitLauncher.logLevel);
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, enableCC + "");
    return props;
  }

  /**
   * Starts a locator with given configuration.
   */
  public static void startLocator(final String serverHostName, final int port,
      final String testName, Properties props) throws Exception {


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
    InternalLocator.getLocator().stop();
  }

  /**
   * Get the port that the standard dunit locator is listening on.
   *
   * @return
   */
  public static String getDUnitLocatorAddress() {
    return Host.getHost(0).getHostName();
  }

  private String getHARegionName() {
    assertEquals(1, ((GemFireCacheImpl) cache).getCacheServers().size());
    CacheServerImpl bs =
        (CacheServerImpl) ((GemFireCacheImpl) cache).getCacheServers().iterator().next();
    assertEquals(1, bs.getAcceptor().getCacheClientNotifier().getClientProxies().size());
    CacheClientProxy ccp =
        bs.getAcceptor().getCacheClientNotifier().getClientProxies().iterator().next();
    return ccp.getHARegion().getName();
  }

  public static class GetDataSerializableFunction implements Function, DataSerializable {

    public GetDataSerializableFunction() {}

    @Override
    public void execute(FunctionContext context) {
      String dsClassName = (String) context.getArguments();
      try {
        Class aClass = Thread.currentThread().getContextClassLoader().loadClass(dsClassName);
        Constructor constructor = aClass.getConstructor(new Class[0]);
        context.getResultSender().lastResult(constructor.newInstance(new Object[0]));
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
