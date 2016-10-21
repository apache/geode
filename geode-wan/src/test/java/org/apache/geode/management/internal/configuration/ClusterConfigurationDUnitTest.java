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
package org.apache.geode.management.internal.configuration;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.ClassBuilder;
import org.apache.geode.internal.JarClassLoader;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.internal.admin.remote.ShutdownAllRequest;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.Extension;
import org.apache.geode.internal.cache.extension.mock.MockCacheExtension;
import org.apache.geode.internal.cache.extension.mock.MockExtensionCommands;
import org.apache.geode.internal.cache.extension.mock.MockRegionExtension;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.internal.cache.xmlcache.XmlParser;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.HeadlessGfsh;
import org.apache.geode.management.internal.cli.commands.CliCommandTestBase;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.internal.FileUtil.delete;
import static org.apache.geode.internal.FileUtil.deleteMatching;
import static org.apache.geode.internal.lang.StringUtils.isBlank;
import static org.apache.geode.management.internal.cli.CliUtil.getAllNormalMembers;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Wait.waitForCriterion;
import static org.apache.commons.io.FileUtils.*;

@Category(DistributedTest.class)
public class ClusterConfigurationDUnitTest extends CliCommandTestBase {

  private static final int TIMEOUT = 10000;
  private static final int INTERVAL = 500;

  private static final String REPLICATE_REGION = "ReplicateRegion1";
  private static final String PARTITION_REGION = "PartitionRegion1";
  private static final String DISK_REGION1 = "DR1";
  private static final String INDEX1 = "ID1";
  private static final String INDEX2 = "ID2";
  private static final String GROUP1 = "G1";
  private static final String GROUP2 = "G2";
  private static final String JAR1 = "D1.jar";
  private static final String JAR2 = "D2.jar";
  private static final String JAR3 = "D3.jar";
  private static final String AsyncEventQueue1 = "Q1";

  private static final String dataMember = "DataMember";
  private static final String newMember = "NewMember";

  private static Set<String> serverNames = new HashSet<>();
  private static Set<String> jarFileNames = new HashSet<>();

  private transient ClassBuilder classBuilder = new ClassBuilder();

  @Override
  public final void postSetUpCliCommandTestBase() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  public final void preTearDownCliCommandTestBase() throws Exception {
    shutdownAll();

    serverNames.clear();
    jarFileNames.clear();
  }

  @Category(FlakyTest.class) // GEODE-1335
  @Test
  public void testConfigDistribution() throws Exception {
    addIgnoredException("could not get remote locator");
    addIgnoredException("EntryDestroyedException");

    String workingDir = this.temporaryFolder.getRoot().getCanonicalPath() + File.separator;

    Object[] result = setup();
    final int locatorPort = (Integer) result[0];
    final String jmxHost = (String) result[1];
    final int jmxPort = (Integer) result[2];
    final int httpPort = (Integer) result[3];
    final String locatorString = "localHost[" + locatorPort + "]";

    String gatewayReceiverStartPort = "10000";
    String gatewayReceiverEndPort = "20000";
    final String gsId = "GatewaySender1";
    final String batchSize = "1000";
    final String dispatcherThreads = "5";
    final String enableConflation = "false";
    final String manualStart = "false";
    final String receiverManualStart = "true";
    final String alertThreshold = "1000";
    final String batchTimeInterval = "20";
    final String maxQueueMemory = "100";
    final String orderPolicy = OrderPolicy.KEY.toString();
    final String parallel = "true";
    final String rmDsId = "250";
    final String socketBufferSize =
        String.valueOf(GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000);
    final String socketReadTimeout =
        String.valueOf(GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 200);
    final String DESTROY_REGION = "regionToBeDestroyed";

    createRegion(REPLICATE_REGION, RegionShortcut.REPLICATE, null);
    createRegion(PARTITION_REGION, RegionShortcut.PARTITION, null);
    createRegion(DESTROY_REGION, RegionShortcut.REPLICATE, null);
    createIndex(INDEX1, "AAPL", REPLICATE_REGION, null);
    createIndex(INDEX2, "VMW", PARTITION_REGION, null);

    createAndDeployJar(workingDir + JAR1, null);
    createAndDeployJar(workingDir + JAR2, null);
    createAndDeployJar(workingDir + JAR3, null);

    createAsyncEventQueue(AsyncEventQueue1, "false", null, "1000", "1000", null);
    destroyRegion(DESTROY_REGION);
    destroyIndex(INDEX2, PARTITION_REGION, null);

    undeployJar(JAR3, null);

    alterRuntime("true", "", "", "");
    createGatewayReceiver(receiverManualStart, "", gatewayReceiverStartPort, gatewayReceiverEndPort,
        "20", "");
    createGatewaySender(gsId, batchSize, alertThreshold, batchTimeInterval, dispatcherThreads,
        enableConflation, manualStart, maxQueueMemory, orderPolicy, parallel, rmDsId,
        socketBufferSize, socketReadTimeout);

    // alterRegion(PARTITION_REGION, "false", AsyncEventQueue1, "", "", "", "", "", "", gsId);
    // Start a new member which receives the shared configuration
    // Verify the config creation on this member

    final String newMemberWorkingDir =
        this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + newMember;

    VM newMember = getHost(0).getVM(2);
    newMember.invoke(new SerializableCallable() {
      @Override
      public Object call() throws IOException {
        Properties localProps = new Properties();

        File workingDir = new File(newMemberWorkingDir);
        workingDir.mkdirs();

        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        localProps.setProperty(NAME, ClusterConfigurationDUnitTest.newMember);
        localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
        localProps.setProperty(DEPLOY_WORKING_DIR, workingDir.getCanonicalPath());

        getSystem(localProps);
        Cache cache = getCache();

        assertNotNull(cache);
        assertTrue(cache.getCopyOnRead());

        Region region1 = cache.getRegion(REPLICATE_REGION);
        assertNotNull(region1);
        Region region2 = cache.getRegion(PARTITION_REGION);
        assertNotNull(region2);

        Region region3 = cache.getRegion(DESTROY_REGION);
        assertNull(region3);

        // Index verification
        Index index1 = cache.getQueryService().getIndex(region1, INDEX1);
        assertNotNull(index1);
        assertNull(cache.getQueryService().getIndex(region2, INDEX2));

        final JarDeployer jarDeployer = new JarDeployer(
            ((GemFireCacheImpl) cache).getDistributedSystem().getConfig().getDeployWorkingDir());

        final List<JarClassLoader> jarClassLoaders = jarDeployer.findJarClassLoaders();

        Set<String> jarNames = new HashSet<String>();

        for (JarClassLoader jarClassLoader : jarClassLoaders) {
          jarNames.add(jarClassLoader.getJarName());
        }

        assertTrue(jarNames.contains(JAR1));
        assertTrue(jarNames.contains(JAR2));
        assertFalse(jarNames.contains(JAR3));

        // ASYNC-EVENT-QUEUE verification
        AsyncEventQueue aeq = cache.getAsyncEventQueue(AsyncEventQueue1);
        assertNotNull(aeq);
        assertFalse(aeq.isPersistent());
        assertTrue(aeq.getBatchSize() == 1000);
        assertTrue(aeq.getMaximumQueueMemory() == 1000);

        // GatewayReceiver verification
        Set<GatewayReceiver> gatewayReceivers = cache.getGatewayReceivers();
        assertFalse(gatewayReceivers.isEmpty());
        assertTrue(gatewayReceivers.size() == 1);

        // Gateway Sender verification
        GatewaySender gs = cache.getGatewaySender(gsId);
        assertNotNull(gs);
        assertTrue(alertThreshold.equals(Integer.toString(gs.getAlertThreshold())));
        assertTrue(batchSize.equals(Integer.toString(gs.getBatchSize())));
        assertTrue(dispatcherThreads.equals(Integer.toString(gs.getDispatcherThreads())));
        assertTrue(enableConflation.equals(Boolean.toString(gs.isBatchConflationEnabled())));
        assertTrue(manualStart.equals(Boolean.toString(gs.isManualStart())));
        assertTrue(alertThreshold.equals(Integer.toString(gs.getAlertThreshold())));
        assertTrue(batchTimeInterval.equals(Integer.toString(gs.getBatchTimeInterval())));
        assertTrue(maxQueueMemory.equals(Integer.toString(gs.getMaximumQueueMemory())));
        assertTrue(orderPolicy.equals(gs.getOrderPolicy().toString()));
        assertTrue(parallel.equals(Boolean.toString(gs.isParallel())));
        assertTrue(rmDsId.equals(Integer.toString(gs.getRemoteDSId())));
        assertTrue(socketBufferSize.equals(Integer.toString(gs.getSocketBufferSize())));
        assertTrue(socketReadTimeout.equals(Integer.toString(gs.getSocketReadTimeout())));

        return getAllNormalMembers(cache);
      }
    });
  }

  /**
   * Tests for {@link Extension}, {@link Extensible}, {@link XmlParser}, {@link XmlGenerator},
   * {@link XmlEntity} as it applies to Extensions. Asserts that Mock Extension is created and
   * altered on region and cache.
   * 
   * @since GemFire 8.1
   */
  @Test
  public void testCreateExtensions() throws Exception {
    Object[] result = setup();
    final int locatorPort = (Integer) result[0];

    createRegion(REPLICATE_REGION, RegionShortcut.REPLICATE, null);
    createMockRegionExtension(REPLICATE_REGION, "value1");
    alterMockRegionExtension(REPLICATE_REGION, "value2");
    createMockCacheExtension("value1");
    alterMockCacheExtension("value2");

    // Start a new member which receives the shared configuration
    // Verify the config creation on this member

    final String newMemberWorkDir =
        this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + newMember;

    VM newMember = getHost(0).getVM(2);
    newMember.invoke(new SerializableCallable() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object call() throws IOException {
        Properties localProps = new Properties();

        File workingDir = new File(newMemberWorkDir);
        workingDir.mkdirs();

        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        localProps.setProperty(NAME, ClusterConfigurationDUnitTest.newMember);
        localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
        localProps.setProperty(DEPLOY_WORKING_DIR, workingDir.getCanonicalPath());

        getSystem(localProps);
        Cache cache = getCache();

        assertNotNull(cache);

        Region<?, ?> region1 = cache.getRegion(REPLICATE_REGION);
        assertNotNull(region1);

        // MockRegionExtension verification
        @SuppressWarnings("unchecked")
        // should only be one region extension
        final MockRegionExtension mockRegionExtension =
            (MockRegionExtension) ((Extensible<Region<?, ?>>) region1).getExtensionPoint()
                .getExtensions().iterator().next();
        assertNotNull(mockRegionExtension);
        assertEquals(1, mockRegionExtension.beforeCreateCounter.get());
        assertEquals(1, mockRegionExtension.onCreateCounter.get());
        assertEquals("value2", mockRegionExtension.getValue());

        // MockCacheExtension verification
        @SuppressWarnings("unchecked")
        // should only be one cache extension
        final MockCacheExtension mockCacheExtension =
            (MockCacheExtension) ((Extensible<Cache>) cache).getExtensionPoint().getExtensions()
                .iterator().next();
        assertNotNull(mockCacheExtension);
        assertEquals(1, mockCacheExtension.beforeCreateCounter.get());
        assertEquals(1, mockCacheExtension.onCreateCounter.get());
        assertEquals("value2", mockCacheExtension.getValue());

        return getAllNormalMembers(cache);
      }
    });
  }

  /**
   * Tests for {@link Extension}, {@link Extensible}, {@link XmlParser}, {@link XmlGenerator},
   * {@link XmlEntity} as it applies to Extensions. Asserts that Mock Extension is created and
   * destroyed on region and cache.
   * 
   * @since GemFire 8.1
   */
  @Test
  public void testDestroyExtensions() throws Exception {
    Object[] result = setup();
    final int locatorPort = (Integer) result[0];

    createRegion(REPLICATE_REGION, RegionShortcut.REPLICATE, null);
    createMockRegionExtension(REPLICATE_REGION, "value1");
    destroyMockRegionExtension(REPLICATE_REGION);
    createMockCacheExtension("value1");
    destroyMockCacheExtension();

    // Start a new member which receives the shared configuration
    // Verify the config creation on this member

    final String newMemberWorkingDir =
        this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + newMember;

    VM newMember = getHost(0).getVM(2);
    newMember.invoke(new SerializableCallable() {

      @Override
      public Object call() throws IOException {
        Properties localProps = new Properties();

        File workingDir = new File(newMemberWorkingDir);
        workingDir.mkdirs();

        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        localProps.setProperty(NAME, ClusterConfigurationDUnitTest.newMember);
        localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
        localProps.setProperty(DEPLOY_WORKING_DIR, workingDir.getCanonicalPath());

        getSystem(localProps);
        Cache cache = getCache();

        assertNotNull(cache);

        Region<?, ?> region1 = cache.getRegion(REPLICATE_REGION);
        assertNotNull(region1);

        // MockRegionExtension verification
        @SuppressWarnings("unchecked")
        final Extensible<Region<?, ?>> extensibleRegion = (Extensible<Region<?, ?>>) region1;
        // Should not be any region extensions
        assertTrue(!extensibleRegion.getExtensionPoint().getExtensions().iterator().hasNext());

        // MockCacheExtension verification
        @SuppressWarnings("unchecked")
        final Extensible<Cache> extensibleCache = (Extensible<Cache>) cache;
        // Should not be any cache extensions
        assertTrue(!extensibleCache.getExtensionPoint().getExtensions().iterator().hasNext());

        return getAllNormalMembers(cache);
      }
    });
  }

  @Ignore("disabled for unknown reason") // this passes when @Ignore is removed
  @Test
  public void testCreateDiskStore() throws Exception {
    Object[] result = setup();
    final int locatorPort = (Integer) result[0];
    final String jmxHost = (String) result[1];
    final int jmxPort = (Integer) result[2];
    final int httpPort = (Integer) result[3];
    final String locatorString = "localHost[" + locatorPort + "]";

    final String diskStoreName = "clusterConfigTestDiskStore";
    final String diskDirs = "dir1";

    // final String
    // createPersistentRegion(peersRegion, RegionShortcut.PARTITION_PERSISTENT, "", diskStoreName);

    final String autoCompact = "true";
    final String allowForceCompaction = "true";
    final String compactionThreshold = "50";
    final String duCritical = "90";
    final String duWarning = "85";
    final String maxOplogSize = "1000";
    final String queueSize = "300";
    final String timeInterval = "10";
    final String writeBufferSize = "100";

    createDiskStore(diskStoreName, diskDirs, autoCompact, allowForceCompaction, compactionThreshold,
        duCritical, duWarning, maxOplogSize, queueSize, timeInterval, writeBufferSize);

    // createAsyncEventQueue(id, persistent, diskStoreName, batchSize, maxQueueMemory, group)

    // Stop the existing data member
    VM dataMember = getHost(0).getVM(1);
    dataMember.invoke(new SerializableCallable() {
      @Override
      public Object call() throws IOException {

        CacheFactory cf = new CacheFactory();
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        File[] diskDirs = null;
        Collection<DiskStoreImpl> diskStoreList = cache.listDiskStores();

        assertFalse(diskStoreList.isEmpty());
        assertTrue(diskStoreList.size() == 1);

        for (DiskStoreImpl diskStore : diskStoreList) {
          diskDirs = diskStore.getDiskDirs();
          break;
        }

        assertNotNull(diskDirs);
        assertTrue(diskDirs.length > 0);

        // close the cache
        cache.close();

        // Delete the disk-store files
        for (File diskDir : diskDirs) {
          deleteDirectory(diskDir);
        }
        return getAllNormalMembers(cache);
      }
    });

    final String newMemberWorkingDir =
        this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + newMember;

    // Now start the new data member and it should create all the disk-store artifacts
    VM newMember = getHost(0).getVM(2);
    newMember.invoke(new SerializableCallable() {
      @Override
      public Object call() throws IOException {
        Properties localProps = new Properties();

        File workingDir = new File(newMemberWorkingDir);
        workingDir.mkdirs();

        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        localProps.setProperty(NAME, ClusterConfigurationDUnitTest.newMember);
        localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
        localProps.setProperty(DEPLOY_WORKING_DIR, workingDir.getCanonicalPath());

        getSystem(localProps);
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        assertNotNull(cache);


        Collection<DiskStoreImpl> diskStoreList = cache.listDiskStores();

        assertFalse(diskStoreList.isEmpty());
        assertTrue(diskStoreList.size() == 1);

        for (DiskStoreImpl diskStore : diskStoreList) {
          assertTrue(diskStore.getName().equals(diskStoreName));
          assertTrue(Boolean.toString(diskStore.getAutoCompact()).equals(autoCompact));
          assertTrue(
              Boolean.toString(diskStore.getAllowForceCompaction()).equals(allowForceCompaction));
          assertTrue(
              Integer.toString(diskStore.getCompactionThreshold()).equals(compactionThreshold));
          assertTrue(Long.toString(diskStore.getMaxOplogSize()).equals(maxOplogSize));
          assertTrue(Integer.toString(diskStore.getQueueSize()).equals(queueSize));
          assertTrue(Integer.toString(diskStore.getWriteBufferSize()).equals(writeBufferSize));
          assertTrue(Long.toString(diskStore.getTimeInterval()).equals(timeInterval));
          break;
        }
        cache.close();
        return null;
      }
    });
  }

  @Ignore("disabled for unknown reason") // this fails in configurePDX when @Ignore is removed
  @Test
  public void testConfigurePDX() throws Exception {
    Object[] result = setup();
    final int locatorPort = (Integer) result[0];
    final String jmxHost = (String) result[1];
    final int jmxPort = (Integer) result[2];
    final int httpPort = (Integer) result[3];
    final String locatorString = "localHost[" + locatorPort + "]";

    configurePDX("com.foo.*", "true", "true", null, "true");

    VM dataMember = getHost(0).getVM(1);
    dataMember.invoke(new SerializableCallable() {
      @Override
      public Object call() throws IOException {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        assertTrue(cache.getPdxReadSerialized());
        assertTrue(cache.getPdxIgnoreUnreadFields());
        assertTrue(cache.getPdxPersistent());
        return null;
      }
    });
  }

  @Test
  public void testClusterConfigDir() throws Exception {
    final int[] ports = getRandomAvailableTCPPorts(3);
    final int locator1Port = ports[0];
    final String locator1Name = "locator1-" + locator1Port;

    final String locatorLogPath = this.temporaryFolder.getRoot().getCanonicalPath() + File.separator
        + "locator-" + locator1Port + ".log";
    final String clusterConfigPath =
        this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + "userSpecifiedDir";

    VM locatorAndMgr = getHost(0).getVM(3);
    Object[] result = (Object[]) locatorAndMgr.invoke(new SerializableCallable() {
      @Override
      public Object call() throws IOException {
        int httpPort;
        int jmxPort;
        String jmxHost;

        try {
          jmxHost = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ignore) {
          jmxHost = "localhost";
        }

        final int[] ports = getRandomAvailableTCPPorts(2);

        jmxPort = ports[0];
        httpPort = ports[1];

        final File locatorLogFile = new File(locatorLogPath);

        final Properties locatorProps = new Properties();
        locatorProps.setProperty(NAME, locator1Name);
        locatorProps.setProperty(MCAST_PORT, "0");
        locatorProps.setProperty(LOG_LEVEL, "config");
        locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
        locatorProps.setProperty(JMX_MANAGER, "true");
        locatorProps.setProperty(JMX_MANAGER_START, "true");
        locatorProps.setProperty(JMX_MANAGER_BIND_ADDRESS, String.valueOf(jmxHost));
        locatorProps.setProperty(JMX_MANAGER_PORT, String.valueOf(jmxPort));

        File clusterConfigDir = new File(clusterConfigPath);
        assertTrue(clusterConfigDir.mkdir());

        locatorProps.setProperty(CLUSTER_CONFIGURATION_DIR, clusterConfigDir.getCanonicalPath());
        locatorProps.setProperty(HTTP_SERVICE_PORT, String.valueOf(httpPort));

        final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator1Port,
            locatorLogFile, null, locatorProps);

        WaitCriterion wc = new WaitCriterion() {
          @Override
          public boolean done() {
            return locator.isSharedConfigurationRunning();
          }

          @Override
          public String description() {
            return "Waiting for shared configuration to be started";
          }
        };
        waitForCriterion(wc, TIMEOUT, INTERVAL, true);

        assertTrue(clusterConfigDir.list().length > 0);

        final Object[] result = new Object[4];
        result[0] = locator1Port;
        result[1] = jmxHost;
        result[2] = jmxPort;
        result[3] = httpPort;
        return result;
      }
    });
  }

  private Object[] setup() throws IOException {
    final int[] ports = getRandomAvailableTCPPorts(3);
    final int locator1Port = ports[0];
    final String locator1Name = "locator1-" + locator1Port;
    final String locatorLogPath = this.temporaryFolder.getRoot().getCanonicalPath() + File.separator
        + "locator-" + locator1Port + ".log";

    VM locatorAndMgr = getHost(0).getVM(3);
    Object[] result = (Object[]) locatorAndMgr.invoke(new SerializableCallable() {
      @Override
      public Object call() throws IOException {
        int httpPort;
        int jmxPort;
        String jmxHost;

        try {
          jmxHost = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ignore) {
          jmxHost = "localhost";
        }

        final int[] ports = getRandomAvailableTCPPorts(2);

        jmxPort = ports[0];
        httpPort = ports[1];

        final File locatorLogFile = new File(locatorLogPath);

        final Properties locatorProps = new Properties();
        locatorProps.setProperty(NAME, locator1Name);
        locatorProps.setProperty(MCAST_PORT, "0");
        locatorProps.setProperty(LOG_LEVEL, "config");
        locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
        locatorProps.setProperty(JMX_MANAGER, "true");
        locatorProps.setProperty(JMX_MANAGER_START, "true");
        locatorProps.setProperty(JMX_MANAGER_BIND_ADDRESS, String.valueOf(jmxHost));
        locatorProps.setProperty(JMX_MANAGER_PORT, String.valueOf(jmxPort));
        locatorProps.setProperty(HTTP_SERVICE_PORT, String.valueOf(httpPort));

        final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator1Port,
            locatorLogFile, null, locatorProps);

        WaitCriterion wc = new WaitCriterion() {
          @Override
          public boolean done() {
            return locator.isSharedConfigurationRunning();
          }

          @Override
          public String description() {
            return "Waiting for shared configuration to be started";
          }
        };
        waitForCriterion(wc, TIMEOUT, INTERVAL, true);

        final Object[] result = new Object[4];
        result[0] = locator1Port;
        result[1] = jmxHost;
        result[2] = jmxPort;
        result[3] = httpPort;
        return result;
      }
    });

    HeadlessGfsh gfsh = getDefaultShell();
    String jmxHost = (String) result[1];
    int jmxPort = (Integer) result[2];
    int httpPort = (Integer) result[3];

    connect(jmxHost, jmxPort, httpPort, gfsh);

    final String dataMemberWorkingDir =
        this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + dataMember;

    // Create a cache in VM 1
    VM dataMember = getHost(0).getVM(1);
    dataMember.invoke(new SerializableCallable() {
      @Override
      public Object call() throws IOException {
        Properties localProps = new Properties();
        File workingDir = new File(dataMemberWorkingDir);
        workingDir.mkdirs();

        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locator1Port + "]");
        localProps.setProperty(NAME, ClusterConfigurationDUnitTest.dataMember);
        localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
        localProps.setProperty(DEPLOY_WORKING_DIR, workingDir.getCanonicalPath());

        getSystem(localProps);
        Cache cache = getCache();
        assertNotNull(cache);
        return getAllNormalMembers(cache);
      }
    });

    return result;
  }

  private void createRegion(String regionName, RegionShortcut regionShortCut, String group) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortCut.name());
    csb.addOptionWithValueCheck(CliStrings.CREATE_REGION__GROUP, group);
    executeAndVerifyCommand(csb.toString());
  }

  private void createMockRegionExtension(final String regionName, final String value) {
    CommandStringBuilder csb =
        new CommandStringBuilder(MockExtensionCommands.CREATE_MOCK_REGION_EXTENSION);
    csb.addOption(MockExtensionCommands.OPTION_REGION_NAME, regionName);
    csb.addOption(MockExtensionCommands.OPTION_VALUE, value);
    executeAndVerifyCommand(csb.toString());
  }

  private void alterMockRegionExtension(final String regionName, final String value) {
    CommandStringBuilder csb =
        new CommandStringBuilder(MockExtensionCommands.ALTER_MOCK_REGION_EXTENSION);
    csb.addOption(MockExtensionCommands.OPTION_REGION_NAME, regionName);
    csb.addOption(MockExtensionCommands.OPTION_VALUE, value);
    executeAndVerifyCommand(csb.toString());
  }

  private void destroyMockRegionExtension(final String regionName) {
    CommandStringBuilder csb =
        new CommandStringBuilder(MockExtensionCommands.DESTROY_MOCK_REGION_EXTENSION);
    csb.addOption(MockExtensionCommands.OPTION_REGION_NAME, regionName);
    executeAndVerifyCommand(csb.toString());
  }

  private void createMockCacheExtension(final String value) {
    CommandStringBuilder csb =
        new CommandStringBuilder(MockExtensionCommands.CREATE_MOCK_CACHE_EXTENSION);
    csb.addOption(MockExtensionCommands.OPTION_VALUE, value);
    executeAndVerifyCommand(csb.toString());
  }

  private void alterMockCacheExtension(final String value) {
    CommandStringBuilder csb =
        new CommandStringBuilder(MockExtensionCommands.ALTER_MOCK_CACHE_EXTENSION);
    csb.addOption(MockExtensionCommands.OPTION_VALUE, value);
    executeAndVerifyCommand(csb.toString());
  }

  private void destroyMockCacheExtension() {
    CommandStringBuilder csb =
        new CommandStringBuilder(MockExtensionCommands.DESTROY_MOCK_CACHE_EXTENSION);
    executeAndVerifyCommand(csb.toString());
  }

  private void createPersistentRegion(String regionName, RegionShortcut regionShortCut,
      String group, String diskStoreName) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOptionWithValueCheck(CliStrings.CREATE_REGION__REGION, regionName);
    csb.addOptionWithValueCheck(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortCut.name());
    csb.addOptionWithValueCheck(CliStrings.CREATE_REGION__DISKSTORE, diskStoreName);
    csb.addOptionWithValueCheck(CliStrings.CREATE_REGION__GROUP, group);
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void destroyRegion(String regionName) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_REGION);
    csb.addOption(CliStrings.DESTROY_REGION__REGION, regionName);
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void alterRegion(String regionName, String cloningEnabled, String aeqId,
      String cacheListener, String cacheWriter, String cacheLoader, String entryExpIdleTime,
      String entryExpIdleTimeAction, String evictionMax, String gsId) {

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOptionWithValueCheck(CliStrings.ALTER_REGION__CLONINGENABLED, "false");
    csb.addOptionWithValueCheck(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, aeqId);
    csb.addOptionWithValueCheck(CliStrings.ALTER_REGION__CACHELISTENER, cacheListener);
    csb.addOptionWithValueCheck(CliStrings.ALTER_REGION__CACHEWRITER, cacheWriter);
    csb.addOptionWithValueCheck(CliStrings.ALTER_REGION__CACHELOADER, cacheLoader);
    csb.addOptionWithValueCheck(CliStrings.ALTER_REGION__CLONINGENABLED, cloningEnabled);
    csb.addOptionWithValueCheck(CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME, entryExpIdleTime);
    csb.addOptionWithValueCheck(CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION,
        entryExpIdleTimeAction);
    csb.addOptionWithValueCheck(CliStrings.ALTER_REGION__EVICTIONMAX, evictionMax);
    csb.addOptionWithValueCheck(CliStrings.ALTER_REGION__GATEWAYSENDERID, gsId);

    executeAndVerifyCommand(csb.getCommandString());
  }

  private void executeAndVerifyCommand(String commandString) {
    CommandResult cmdResult = executeCommand(commandString);
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("Command : " + commandString);
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
        .info("Command Result : " + commandResultToString(cmdResult));
    assertEquals(Status.OK, cmdResult.getStatus());
    assertFalse(cmdResult.failedToPersist());
  }

  private void createIndex(String indexName, String expression, String regionName, String group) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, expression);
    csb.addOption(CliStrings.CREATE_INDEX__REGION, regionName);
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void destroyIndex(String indexName, String regionName, String group) {
    if (isBlank(indexName) && isBlank(regionName) && isBlank(group)) {
      return;
    }
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOptionWithValueCheck(CliStrings.DESTROY_INDEX__NAME, indexName);
    csb.addOptionWithValueCheck(CliStrings.DESTROY_INDEX__REGION, regionName);
    csb.addOptionWithValueCheck(CliStrings.DESTROY_INDEX__GROUP, group);
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void createDiskStore(String diskStoreName, String diskDirs, String autoCompact,
      String allowForceCompaction, String compactionThreshold, String duCritical, String duWarning,
      String maxOplogSize, String queueSize, String timeInterval, String writeBufferSize) {

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);
    csb.addOption(CliStrings.CREATE_DISK_STORE__NAME, diskStoreName);
    csb.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, diskDirs);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__AUTO_COMPACT, autoCompact);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__ALLOW_FORCE_COMPACTION,
        allowForceCompaction);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__COMPACTION_THRESHOLD,
        compactionThreshold);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__DISK_USAGE_CRITICAL_PCT, duCritical);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__DISK_USAGE_WARNING_PCT, duWarning);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__MAX_OPLOG_SIZE, maxOplogSize);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__QUEUE_SIZE, queueSize);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__TIME_INTERVAL, timeInterval);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__WRITE_BUFFER_SIZE, writeBufferSize);
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void destroyDiskStore(String diskStoreName, String group) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_DISK_STORE);
    csb.addOption(CliStrings.DESTROY_DISK_STORE__NAME, diskStoreName);
    csb.addOptionWithValueCheck(CliStrings.DESTROY_DISK_STORE__GROUP, group);
    executeAndVerifyCommand(csb.toString());
  }

  private void createGatewayReceiver(String manualStart, String bindAddress, String startPort,
      String endPort, String maxTimeBetweenPings, String group) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYRECEIVER);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART, manualStart);
    csb.addOption(CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT, startPort);
    csb.addOption(CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT, endPort);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS, bindAddress);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYRECEIVER__GROUP, group);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS,
        maxTimeBetweenPings);
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void createGatewaySender(String id, String batchSize, String alertThreshold,
      String batchTimeInterval, String dispatcherThreads, String enableConflation,
      String manualStart, String maxQueueMemory, String orderPolicy, String parallel, String rmDsId,
      String socketBufferSize, String socketReadTimeout) {

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__ID, id);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE, batchSize);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD, alertThreshold);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL,
        batchTimeInterval);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS,
        dispatcherThreads);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION,
        enableConflation);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, manualStart);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY, maxQueueMemory);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY, orderPolicy);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, parallel);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, rmDsId);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE,
        socketBufferSize);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT,
        socketReadTimeout);

    executeAndVerifyCommand(csb.getCommandString());
  }

  private void createAsyncEventQueue(String id, String persistent, String diskStoreName,
      String batchSize, String maxQueueMemory, String group) throws IOException {
    String queueCommandsJarName = this.temporaryFolder.getRoot().getCanonicalPath() + File.separator
        + "testEndToEndSC-QueueCommands.jar";
    final File jarFile = new File(queueCommandsJarName);

    try {
      ClassBuilder classBuilder = new ClassBuilder();
      byte[] jarBytes =
          classBuilder.createJarFromClassContent("com/qcdunit/QueueCommandsDUnitTestListener",
              "package com.qcdunit;" + "import java.util.List; import java.util.Properties;"
                  + "import org.apache.geode.internal.cache.xmlcache.Declarable2; import org.apache.geode.cache.asyncqueue.AsyncEvent;"
                  + "import org.apache.geode.cache.asyncqueue.AsyncEventListener;"
                  + "public class QueueCommandsDUnitTestListener implements Declarable2, AsyncEventListener {"
                  + "Properties props;"
                  + "public boolean processEvents(List<AsyncEvent> events) { return true; }"
                  + "public void close() {}"
                  + "public void init(final Properties props) {this.props = props;}"
                  + "public Properties getConfig() {return this.props;}}");

      writeByteArrayToFile(jarFile, jarBytes);
      CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DEPLOY);
      csb.addOption(CliStrings.DEPLOY__JAR, queueCommandsJarName);
      executeAndVerifyCommand(csb.getCommandString());

      csb = new CommandStringBuilder(CliStrings.CREATE_ASYNC_EVENT_QUEUE);
      csb.addOptionWithValueCheck(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, id);
      csb.addOptionWithValueCheck(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER,
          "com.qcdunit.QueueCommandsDUnitTestListener");
      csb.addOptionWithValueCheck(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE, diskStoreName);
      csb.addOptionWithValueCheck(CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE, batchSize);
      csb.addOptionWithValueCheck(CliStrings.CREATE_ASYNC_EVENT_QUEUE__GROUP, group);
      csb.addOptionWithValueCheck(CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT, persistent);
      csb.addOptionWithValueCheck(CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY,
          maxQueueMemory);
      executeAndVerifyCommand(csb.getCommandString());

    } finally {
      deleteQuietly(jarFile);
    }
  }

  private void configurePDX(String autoSerializerClasses, String ignoreUnreadFields,
      String persistent, String portableAutoSerializerClasses, String readSerialized) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CONFIGURE_PDX);
    csb.addOptionWithValueCheck(CliStrings.CONFIGURE_PDX__AUTO__SERIALIZER__CLASSES,
        autoSerializerClasses);
    csb.addOptionWithValueCheck(CliStrings.CONFIGURE_PDX__IGNORE__UNREAD_FIELDS,
        ignoreUnreadFields);
    csb.addOptionWithValueCheck(CliStrings.CONFIGURE_PDX__PERSISTENT, persistent);
    csb.addOptionWithValueCheck(CliStrings.CONFIGURE_PDX__PORTABLE__AUTO__SERIALIZER__CLASSES,
        portableAutoSerializerClasses);
    csb.addOptionWithValueCheck(CliStrings.CONFIGURE_PDX__READ__SERIALIZED, readSerialized);
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void createAndDeployJar(String jarName, String group) throws IOException {
    File newDeployableJarFile = new File(jarName);
    this.classBuilder.writeJarFromName("ShareConfigClass", newDeployableJarFile);
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DEPLOY);
    csb.addOption(CliStrings.DEPLOY__JAR, jarName);
    if (!isBlank(group)) {
      csb.addOption(CliStrings.DEPLOY__GROUP, group);
    }
    executeAndVerifyCommand(csb.getCommandString());
    jarFileNames.add(jarName);
  }

  private void undeployJar(String jarName, String group) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.UNDEPLOY);
    if (!isBlank(jarName)) {
      csb.addOption(CliStrings.UNDEPLOY__JAR, jarName);
    }
    if (!isBlank(group)) {
      csb.addOption(CliStrings.UNDEPLOY__GROUP, group);
    }
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void alterRuntime(String copyOnRead, String lockLease, String lockTimeout,
      String messageSyncInterval) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOptionWithValueCheck(CliStrings.ALTER_RUNTIME_CONFIG__COPY__ON__READ, copyOnRead);
    csb.addOptionWithValueCheck(CliStrings.ALTER_RUNTIME_CONFIG__LOCK__LEASE, lockLease);
    csb.addOptionWithValueCheck(CliStrings.ALTER_RUNTIME_CONFIG__LOCK__TIMEOUT, lockTimeout);
    csb.addOptionWithValueCheck(CliStrings.ALTER_RUNTIME_CONFIG__MESSAGE__SYNC__INTERVAL,
        messageSyncInterval);
    executeAndVerifyCommand(csb.toString());
  }

  private void deleteSavedJarFiles() throws IOException {
    deleteMatching(new File("."), "^" + JarDeployer.JAR_PREFIX + "Deploy1.*#\\d++$");
    delete(new File("Deploy1.jar"));
  }

  private void shutdownAll() throws IOException {
    VM locatorAndMgr = getHost(0).getVM(3);
    locatorAndMgr.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();
        ShutdownAllRequest.send(cache.getDistributedSystem().getDistributionManager(), -1);
        return null;
      }
    });

    locatorAndMgr.invoke(SharedConfigurationTestUtils.cleanupLocator);
    // Clean up the directories
    if (serverNames != null && !serverNames.isEmpty()) {
      for (String serverName : serverNames) {
        final File serverDir = new File(serverName);
        cleanDirectory(serverDir);
        deleteDirectory(serverDir);
      }
    }
  }

  private static class CommandBuilder {

    private CommandStringBuilder csb;

    public CommandBuilder(String commandName, Map<String, String> options) {
      csb = new CommandStringBuilder(commandName);

      Set<Entry<String, String>> entries = options.entrySet();

      Iterator<Entry<String, String>> iter = entries.iterator();

      while (iter.hasNext()) {
        Entry<String, String> entry = iter.next();
        String option = entry.getKey();

        if (isBlank(option)) {
          csb.addOption(option, entry.getValue());
        }
      }
    }

    public String getCommandString() {
      return csb.toString();
    }
  }
}
