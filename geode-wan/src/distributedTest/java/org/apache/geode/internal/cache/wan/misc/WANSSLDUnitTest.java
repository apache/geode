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
package org.apache.geode.internal.cache.wan.misc;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.DistributedTestUtils.getAllDistributedSystemProperties;
import static org.apache.geode.test.dunit.DistributedTestUtils.unregisterInstantiatorsInThisVM;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.Invoke.invokeInLocator;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallbackAdapter;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Disconnect;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.util.test.TestUtil;

@Category({WanTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class WANSSLDUnitTest implements Serializable {

  private static final Logger logger = LogService.getLogger();
  protected static Cache cache;
  private VM sendingLocator, sendingServer, receivingLocator, receivingServer;
  private int sendingLocatorPort, receivingLocatorPort;
  private String senderId = "ln";

  @Parameterized.Parameters(name = "dispatcherThreadCount={0}")
  public static Collection<Integer> data() {
    return new ArrayList<>(Arrays.asList(1, 3, 5));
  }

  @Parameterized.Parameter
  public int dispatcherThreadCount;

  /** This VM's connection to the distributed system */
  private static InternalDistributedSystem system;
  private String testMethodName;
  private String replicatedRegionName;

  @Rule
  public SerializableTestName testMethodNameWatcher = new SerializableTestName();

  @Rule
  public ClusterStartupRule csRule = new ClusterStartupRule();

  @Before
  public void before() {
    sendingLocator = csRule.getVM(0);
    receivingLocator = csRule.getVM(1);
    receivingServer = csRule.getVM(2);
    sendingServer = csRule.getVM(3);

    sendingLocatorPort = sendingLocator.invoke(() -> createFirstLocatorWithDSIdOne());
    receivingLocatorPort = receivingLocator.invoke(() -> createFirstRemoteLocatorWithDSIdTwo(sendingLocatorPort));

    // setup distributed test case
    // Make sure all VMs agree on nomenclature
    final String className = getClass().getCanonicalName();
    testMethodName = getSanitizedCurrentTestName();
    replicatedRegionName = testMethodName + "_RR";
    String methodName = testMethodName;
    setUpVM(getDefaultDiskStoreName(0, -1, className, testMethodName));

    for (int hostIndex = 0; hostIndex < Host.getHostCount(); hostIndex++) {
      Host host = Host.getHost(hostIndex);
      for (int vmIndex = 0; vmIndex < host.getVMCount(); vmIndex++) {
        final String vmDefaultDiskStoreName =
            getDefaultDiskStoreName(hostIndex, vmIndex, className, testMethodName);
        host.getVM(vmIndex).invoke("setupVM", () -> {
          testMethodName = methodName;
          replicatedRegionName = methodName + "_RR";
          setUpVM(vmDefaultDiskStoreName);
        });
      }
    }

    // setup WAN test case
    int nThreads = dispatcherThreadCount;
    Invoke.invokeInEveryVM(() -> dispatcherThreadCount = nThreads);

    IgnoredException.addIgnoredException("Connection refused");
    IgnoredException.addIgnoredException("Software caused connection abort");
    IgnoredException.addIgnoredException("Connection reset");
  }

  private Integer createFirstLocatorWithDSIdOne() {
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    startLocator(1, port, port, -1);
    return port;
  }

  private Integer createFirstRemoteLocatorWithDSIdTwo(int remoteLocPort) {
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    startLocator(2, port, port, remoteLocPort);
    return port;
  }

  private void createReplicatedRegion(String senderIds) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(InterruptedException.class.getName());
    IgnoredException exp2 =
        IgnoredException.addIgnoredException(GatewaySenderException.class.getName());
    try {
      AttributesFactory fact = new AttributesFactory();
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }
      fact.setDataPolicy(DataPolicy.REPLICATE);
      fact.setScope(Scope.DISTRIBUTED_ACK);
      fact.setOffHeap(false);
      Region r = cache.createRegionFactory(fact.create()).create(replicatedRegionName);
      assertNotNull(r);
    } finally {
      exp.remove();
      exp1.remove();
      exp2.remove();
    }
  }

  private void createCacheInVMs(Integer locatorPort, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> createCache(locatorPort));
    }
  }

  private static void startSender(String senderId) {
      cache.getGatewaySenders().stream()
          .filter(s -> s.getId().equals(senderId))
          .findFirst()
          .ifPresent(GatewaySender::start);
  }

  private static GatewaySenderFactory configureGateway(DiskStoreFactory dsf, File[] dirs1,
      String dsName, int numDispatchers) {

    InternalGatewaySenderFactory gateway =
        (InternalGatewaySenderFactory) cache.createGatewaySenderFactory();
    gateway.setParallel(false);
    gateway.setMaximumQueueMemory(100);
    gateway.setBatchSize(10);
    gateway.setBatchConflationEnabled(false);
    gateway.setManualStart(true);
    gateway.setDispatcherThreads(numDispatchers);
    gateway.setOrderPolicy(GatewaySender.DEFAULT_ORDER_POLICY);
    gateway.setLocatorDiscoveryCallback(new MyLocatorCallback());
    DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
    gateway.setDiskStoreName(store.getName());
    return gateway;
  }

  private void createSender(String dsName) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    try {
      File persistentDirectory =
          new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      persistentDirectory.mkdir();
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      File[] dirs1 = new File[] {persistentDirectory};
      GatewaySenderFactory gateway = configureGateway(dsf, dirs1, dsName,
          dispatcherThreadCount);
      gateway.create(dsName, 2);

    } finally {
      exln.remove();
    }
  }

  private int createReceiver() {
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(
          "Test " + testMethodName + " failed to start GatewayReceiver on port " + port, e);
    }
    return port;
  }


  private Properties getBasicMemberProperties() {
    Properties p = new Properties();
    p.put(ConfigurationProperties.LOG_LEVEL, "info");
    p.put(ConfigurationProperties.MCAST_PORT, "0");
    return p;
  }

  private void createCache(Integer locPort) {
    Properties props = getDistributedSystemProperties();

    props.putAll(getBasicMemberProperties());
    props.put(ConfigurationProperties.LOCATORS, "localhost[" + locPort + "]");

    InternalDistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
  }

  private void createCacheWithSSL(Integer locPort) {
    Properties props = getDistributedSystemProperties();
    props.putAll(getBasicMemberProperties());
    props.putAll(getSSLProperties());

    props.put(ConfigurationProperties.LOCATORS, "localhost[" + locPort + "]");

    logger.info("Starting cache ds with following properties \n" + props);

    InternalDistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
  }

  private Properties getSSLProperties() {
    Properties props = new Properties();
    props.put(ConfigurationProperties.GATEWAY_SSL_ENABLED, "true");
    props.put(ConfigurationProperties.GATEWAY_SSL_PROTOCOLS, "any");
    props.put(ConfigurationProperties.GATEWAY_SSL_CIPHERS, "any");
    props.put(ConfigurationProperties.GATEWAY_SSL_REQUIRE_AUTHENTICATION, "true");
    props.put(ConfigurationProperties.GATEWAY_SSL_KEYSTORE_TYPE, "jks");
    props.put(ConfigurationProperties.GATEWAY_SSL_KEYSTORE,
        TestUtil.getResourcePath(WANSSLDUnitTest.class,
            "/org/apache/geode/cache/client/internal/default.keystore"));
    props.put(ConfigurationProperties.GATEWAY_SSL_KEYSTORE_PASSWORD, "password");
    props.put(ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE,
        TestUtil.getResourcePath(WANSSLDUnitTest.class,
            "/org/apache/geode/cache/client/internal/default.keystore"));
    props.put(ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE_PASSWORD, "password");
    return props;
  }

  private int createReceiverWithSSL(int locPort) {
    Properties props = getDistributedSystemProperties();

    Properties p = getSSLProperties();
    props.putAll(p);
    props.putAll(getBasicMemberProperties());
    props.put(ConfigurationProperties.LOCATORS, "localhost[" + locPort + "]");

    logger.info("Starting cache ds with following properties \n" + props);

    InternalDistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      fail("Test " + getCurrentTestName() + " failed to start GatewayReceiver on port " + port);
    }
    return port;
  }

  private static void doPuts(String regionName, int numPuts) {
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(InterruptedException.class.getName());
    IgnoredException exp2 =
        IgnoredException.addIgnoredException(GatewaySenderException.class.getName());
    try {
      Region r = cache.getRegion(Region.SEPARATOR + regionName);
      assertNotNull(r);
      for (long i = 0; i < numPuts; i++) {
        r.put(i, "Value_" + i);
      }
    } finally {
      exp1.remove();
      exp2.remove();
    }
  }

  private static void checkQueueSizeIsZero(String senderId) {
    await().untilAsserted(() -> testQueueSize(senderId, 0));
  }

  private static void testQueueSize(String senderId, int numQueueEntries) {
    GatewaySender sender = cache.getGatewaySender(senderId);
    if (sender.isParallel()) {
      int totalSize = 0;
      Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      for (RegionQueue q : queues) {
        ConcurrentParallelGatewaySenderQueue prQ = (ConcurrentParallelGatewaySenderQueue) q;
        totalSize += prQ.size();
      }
      assertEquals(numQueueEntries, totalSize);
    } else {
      Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      int size = 0;
      for (RegionQueue q : queues) {
        size += q.size();
      }
      assertEquals(numQueueEntries, size);
    }
  }

  private static void validateRegionSize(String regionName, final int regionSize) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(CacheClosedException.class.getName());
    try {
      final Region r = cache.getRegion(Region.SEPARATOR + regionName);
      assertNotNull(r);
      if (regionSize != r.keySet().size()) {
        await()
            .untilAsserted(() -> assertEquals(
                "Expected region entries: " + regionSize + " but actual entries: "
                    + r.keySet().size() + " present region keyset " + r.keySet(),
                regionSize, r.keySet().size()));
      }
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  private static void verifySenderRunningState(String senderId) {
    GatewaySender sender = cache.getGatewaySender(senderId);
    assertTrue(sender.isRunning());
  }

  private static void verifySenderIsNotConnected(String senderId) {
    AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    assertThat(sender.getEventProcessor().getDispatcher().isConnectedToRemote()).isFalse();
  }

  private void cleanupVM() {
    if (Locator.hasLocator()) {
      Locator.getLocator().stop();
    }
    closeCache();
    cleanDiskDirs();
  }

  private static void deleteBACKUPDiskStoreFile(final File file) {
    if (file.getName().startsWith("BACKUPDiskStore-")
        || file.getName().startsWith(
        InternalConfigurationPersistenceService.CLUSTER_CONFIG_DISK_DIR_PREFIX)) {
      FileUtils.deleteQuietly(file);
    }
  }

  private static void cleanDiskDirs() {
    FileUtils.deleteQuietly(JUnit4CacheTestCase.getDiskDir());
    FileUtils.deleteQuietly(JUnit4CacheTestCase.getDiskDir());
    Arrays.stream(new File(".").listFiles()).forEach(WANSSLDUnitTest::deleteBACKUPDiskStoreFile);
  }

  private static void disconnectAllFromDS() {
    Disconnect.disconnectAllFromDS();
  }

  /**
   * Disconnects this VM from the distributed system
   */
  private static void disconnectFromDS() {
    if (system != null) {
      system.disconnect();
      system = null;
    }

    Disconnect.disconnectFromDS();
  }

  private static String getDefaultDiskStoreName(final int hostIndex, final int vmIndex,
      final String className, final String methodName) {
    return "DiskStore-" + String.valueOf(hostIndex) + "-" + String.valueOf(vmIndex) + "-"
        + className + "." + methodName;
  }

  private static void setUpVM(final String defaultDiskStoreName) {
    assertNotNull("defaultDiskStoreName must not be null", defaultDiskStoreName);
    GemFireCacheImpl.setDefaultDiskStoreName(defaultDiskStoreName);
    setUpCreationStackGenerator();
  }

  private static void setUpCreationStackGenerator() {
    // the following is moved from InternalDistributedSystem to fix #51058
    InternalDistributedSystem.TEST_CREATION_STACK_GENERATOR
        .set(config -> {
          StringBuilder sb = new StringBuilder();
          String[] validAttributeNames = config.getAttributeNames();
          for (String attName : validAttributeNames) {
            Object actualAtt = config.getAttributeObject(attName);
            String actualAttStr = actualAtt.toString();
            sb.append("  ");
            sb.append(attName);
            sb.append("=\"");
            if (actualAtt.getClass().isArray()) {
              actualAttStr = InternalDistributedSystem.arrayToString(actualAtt);
            }
            sb.append(actualAttStr);
            sb.append("\"");
            sb.append("\n");
          }
          return new Throwable(
              "Creating distributed system with the following configuration:\n" + sb.toString());
        });
  }

  private void cleanupAllVms() {
    tearDownVM();
    invokeInEveryVM("tearDownVM", () -> tearDownVM());
    invokeInLocator(() -> {
      DistributionMessageObserver.setInstance(null);
      unregisterInstantiatorsInThisVM();
    });
    DUnitLauncher.closeAndCheckForSuspects();
  }

  private void tearDownVM() {
    closeCache();
    DistributedRule.TearDown.tearDownInVM();
    cleanDiskDirs();
  }

  private void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
      cache = null;
    } else {
      if (isConnectedToDS()) {
        getSystem().disconnect();
      }
    }

    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null && !cache.isClosed()) {
      destroyRegions(cache);
      cache.close();
    }
  }

  private static void destroyRegions(final Cache cache) {
    if (cache != null && !cache.isClosed()) {
      // try to destroy the root regions first so that we clean up any persistent files.
      for (Region<?, ?> root : cache.rootRegions()) {
        String regionFullPath = root == null ? null : root.getFullPath();
        // for colocated regions you can't locally destroy a partitioned region.
        if (root.isDestroyed() || root instanceof HARegion || root instanceof PartitionedRegion) {
          continue;
        }
        try {
          root.localDestroyRegion("teardown");
        } catch (Throwable t) {
          logger.error("Failure during tearDown destroyRegions for " + regionFullPath, t);
        }
      }
    }
  }

  private static void tearDownCreationStackGenerator() {
    InternalDistributedSystem.TEST_CREATION_STACK_GENERATOR
        .set(InternalDistributedSystem.DEFAULT_CREATION_STACK_GENERATOR);
  }

  private static boolean getIsRegionSizeOne(String regionName) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    Wait.pause(2000);

    return r.size() == 1;
  }


  @After
  public final void tearDownDistributedTestCase() throws Exception {
    try {
      preTearDown();
      doTearDownDistributedTestCase();
    } finally {

      System.out.println(
          "\n\n[setup] END TEST " + getClass().getSimpleName() + "." + testMethodName + "\n\n");
    }
  }

  @Test
  public void testSenderSSLReceiverSSL() {
    receivingServer.invoke(() -> createReceiverWithSSL(receivingLocatorPort));

    sendingServer.invoke(() -> createCacheWithSSL(sendingLocatorPort));
    sendingServer.invoke(() -> createSender(senderId));

    receivingServer.invoke(() -> createReplicatedRegion(null));

    sendingServer.invoke(() -> startSender(senderId));
    sendingServer.invoke(() -> createReplicatedRegion(senderId));
    sendingServer.invoke(() -> doPuts(replicatedRegionName, 1000));

    receivingServer.invoke(() -> validateRegionSize(replicatedRegionName, 1000));
  }

  @Test
  public void testSenderNoSSLReceiverSSL() {
    IgnoredException.addIgnoredException("Unexpected IOException");
    IgnoredException.addIgnoredException("SSL Error");
    IgnoredException.addIgnoredException("Unrecognized SSL message");

    receivingServer.invoke(() -> createReceiverWithSSL(receivingLocatorPort));

    sendingServer.invoke(() -> createCache(sendingLocatorPort));
    sendingServer.invoke(() -> createSender(senderId));

    receivingServer.invoke(() -> createReplicatedRegion(null));

    sendingServer.invoke(() -> startSender(senderId));
    sendingServer.invoke(() -> verifySenderRunningState(senderId));
    sendingServer.invoke(() -> verifySenderIsNotConnected(senderId));
    sendingServer.invoke(() -> createReplicatedRegion(senderId));

    int numPuts = 10;
    sendingServer.invoke(() -> doPuts(replicatedRegionName, numPuts));
    sendingServer.invoke(() -> verifySenderRunningState(senderId));
    sendingServer.invoke(() -> verifySenderIsNotConnected(senderId));
    sendingServer.invoke(() -> testQueueSize(senderId, numPuts));

    // Stop the receiver
    receivingServer.invoke(() -> closeCache());
    receivingServer.invoke(() -> closeSocketCreatorFactory());

    // Restart the receiver with SSL disabled
    createCacheInVMs(receivingLocatorPort, receivingServer);
    receivingServer.invoke(() -> createReplicatedRegion(null));
    receivingServer.invoke(() -> createReceiver());

    // Wait for the queue to drain
    sendingServer.invoke(() -> checkQueueSizeIsZero(senderId));

    // Verify region size on receiver
    receivingServer.invoke(() -> validateRegionSize(replicatedRegionName, numPuts));
  }

  @Test
  public void testSenderSSLReceiverNoSSL() {
    IgnoredException.addIgnoredException("Acceptor received unknown");
    IgnoredException.addIgnoredException("failed accepting client");
    IgnoredException.addIgnoredException("Error in connecting to peer");
    IgnoredException.addIgnoredException("Remote host closed connection during handshake");

    createCacheInVMs(receivingLocatorPort, receivingServer);

    receivingServer.invoke(() -> createReceiver());

    sendingServer.invoke(() -> createCacheWithSSL(sendingLocatorPort));
    sendingServer.invoke(() -> createSender(senderId));

    receivingServer.invoke(() -> createReplicatedRegion(null));

    sendingServer.invoke(() -> startSender(senderId));
    sendingServer.invoke(() -> createReplicatedRegion(senderId));
    sendingServer.invoke(() -> doPuts(replicatedRegionName, 1));

    Boolean doesSizeMatch = receivingServer.invoke(() -> getIsRegionSizeOne(replicatedRegionName));

    assertFalse(doesSizeMatch);
  }

  private void closeSocketCreatorFactory() {
    SocketCreatorFactory.close();
  }


  private void startLocator(int dsId, int locatorPort, int startLocatorPort, int remoteLocPort) {
    Properties props = getDistributedSystemProperties();
    props.put(ConfigurationProperties.MCAST_PORT, "0");
    props.put(ConfigurationProperties.DISTRIBUTED_SYSTEM_ID, "" + dsId);
    props.put(ConfigurationProperties.LOCATORS, "localhost[" + locatorPort + "]");
    props.put(ConfigurationProperties.START_LOCATOR, "localhost[" + startLocatorPort
        + "],server=true,peer=true,hostname-for-clients=localhost");
    if (remoteLocPort != -1) {
      props.put(ConfigurationProperties.REMOTE_LOCATORS, "localhost[" + remoteLocPort + "]");
    }

    // Start start the locator with a LOCATOR_DM_TYPE and not a NORMAL_DM_TYPE
    System.setProperty(InternalLocator.FORCE_LOCATOR_DM_TYPE, "true");
    try {
      getSystem(props);
    } finally {
      System.clearProperty(InternalLocator.FORCE_LOCATOR_DM_TYPE);
    }
  }

  /* Method names are used to determine region and disk-store names.
   * @Parameterized adds brackets to the method names, which are illegal characters.
   * Strip them. */
  private String getSanitizedCurrentTestName() {
    return getCurrentTestName()
        .replace('[', '_')
        .replace(']', '_')
        .replace('=', '_');
  }

  private String getCurrentTestName() {
    return this.testMethodNameWatcher.getMethodName();
  }

  private InternalDistributedSystem getSystem(final Properties props) {
    // Setting the default disk store name is now done in setUp
    if (system == null) {
      system = InternalDistributedSystem.getAnyInstance();
    }

    if (system == null || !system.isConnected()) {
      // Figure out our distributed system properties
      Properties p = getAllDistributedSystemProperties(props);
      system = (InternalDistributedSystem) DistributedSystem.connect(p);
    } else {
      boolean needNewSystem = false;
      Properties activeProps = system.getConfig().toProperties();
      for (Entry<Object, Object> entry : props.entrySet()) {
        String key = (String) entry.getKey();
        if (key.startsWith("security-")) {
          continue;
        }
        String value = (String) entry.getValue();
        if (!value.equals(activeProps.getProperty(key))) {
          needNewSystem = true;
          getLogWriter().info("Forcing DS disconnect. For property " + key + " old value = "
              + activeProps.getProperty(key) + " new value = " + value);
          break;
        }
      }
      try {
        activeProps = system.getConfig().toSecurityProperties();
        for (Entry<Object, Object> entry : props.entrySet()) {
          String key = (String) entry.getKey();
          if (!key.startsWith("security-")) {
            continue;
          }
          String value = (String) entry.getValue();
          if (!value.equals(activeProps.getProperty(key))) {
            needNewSystem = true;
            getLogWriter().info("Forcing DS disconnect. For property " + key + " old value = "
                + activeProps.getProperty(key) + " new value = " + value);
            break;
          }
        }
      } catch (NoSuchMethodError e) {
        if (Version.CURRENT_ORDINAL >= 85) {
          throw new IllegalStateException("missing method", e);
        }
      }

      if (needNewSystem) {
        // the current system does not meet our needs to disconnect and
        // call recursively to get a new system.
        getLogWriter().info("Disconnecting from current DS in order to make a new one");
        disconnectFromDS();
        getSystem(props);
      }
    }
    return system;
  }

  private InternalDistributedSystem getSystem() {
    return getSystem(getDistributedSystemProperties());
  }

  /**
   * Returns whether or this VM is connected to a {@link org.apache.geode.distributed.DistributedSystem}.
   */
  private boolean isConnectedToDS() {
    return system != null && system.isConnected();
  }

  private Properties getDistributedSystemProperties() {
    Properties props = new Properties();
    props.put(ConfigurationProperties.OFF_HEAP_MEMORY_SIZE, "300m");
    return props;
  }

  private void doTearDownDistributedTestCase() {
    invokeInEveryVM("tearDownCreationStackGenerator",
        WANSSLDUnitTest::tearDownCreationStackGenerator);
    cleanupAllVms();
    disconnectAllFromDS();
  }

  /**
   * {@code preTearDown()} is invoked before {@link #doTearDownDistributedTestCase()}.
   *
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  private void preTearDown() throws Exception {
    cleanupVM();
    List<AsyncInvocation> invocations = new ArrayList<>();
    final Host host = getHost(0);
    for (int i = 0; i < host.getVMCount(); i++) {
      invocations.add(host.getVM(i).invokeAsync(() -> cleanupVM()));
    }
    for (AsyncInvocation invocation : invocations) {
      invocation.join();
      invocation.checkException();
    }
  }

  @SuppressWarnings("unused")
  private static class MyLocatorCallback extends LocatorDiscoveryCallbackAdapter {

    private final Set discoveredLocators = new HashSet();

    private final Set removedLocators = new HashSet();

    @Override
    public synchronized void locatorsDiscovered(List locators) {
      discoveredLocators.addAll(locators);
      notifyAll();
    }

    @Override
    public synchronized void locatorsRemoved(List locators) {
      removedLocators.addAll(locators);
      notifyAll();
    }

    boolean waitForDiscovery(InetSocketAddress locator, long time)
        throws InterruptedException {
      return waitFor(discoveredLocators, locator, time);
    }


    private synchronized boolean waitFor(Set set, InetSocketAddress locator, long time)
        throws InterruptedException {
      long remaining = time;
      long endTime = System.currentTimeMillis() + time;
      while (!set.contains(locator) && remaining >= 0) {
        wait(remaining);
        remaining = endTime - System.currentTimeMillis();
      }
      return set.contains(locator);
    }

    synchronized Set getDiscovered() {
      return new HashSet(discoveredLocators);
    }

  }
}
