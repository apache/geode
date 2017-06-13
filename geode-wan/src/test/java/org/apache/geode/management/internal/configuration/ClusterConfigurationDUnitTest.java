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

import static org.apache.commons.io.FileUtils.cleanDirectory;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.geode.distributed.ConfigurationProperties.DEPLOY_WORKING_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.management.internal.cli.CliUtil.getAllNormalMembers;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Wait.waitForCriterion;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.admin.remote.ShutdownAllRequest;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
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
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

@Category(DistributedTest.class)
public class ClusterConfigurationDUnitTest extends CliCommandTestBase {

  private static final int TIMEOUT = 10000;
  private static final int INTERVAL = 500;

  private static final String REPLICATE_REGION = "ReplicateRegion1";

  private static final String dataMember = "DataMember";
  private static final String newMember = "NewMember";

  private static Set<String> serverNames = new HashSet<>();
  private static Set<String> jarFileNames = new HashSet<>();

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

  /**
   * Tests for {@link Extension}, {@link Extensible}, {@link XmlParser}, {@link XmlGenerator},
   * {@link XmlEntity} as it applies to Extensions. Asserts that Mock Extension is created and
   * altered on region and cache.
   * 
   * @since GemFire 8.1
   */
  @Category(FlakyTest.class) // GEODE-1334
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
    newMember.invoke(() -> {
      Properties localProps = new Properties();

      File workingDir = new File(newMemberWorkDir);
      workingDir.mkdirs();

      localProps.setProperty(MCAST_PORT, "0");
      localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
      localProps.setProperty(NAME, ClusterConfigurationDUnitTest.newMember);
      localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
      localProps.setProperty(DEPLOY_WORKING_DIR, workingDir.getCanonicalPath());

      getSystem(localProps);
      InternalCache cache = getCache();

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
          (MockCacheExtension) cache.getExtensionPoint().getExtensions().iterator().next();
      assertNotNull(mockCacheExtension);
      assertEquals(1, mockCacheExtension.beforeCreateCounter.get());
      assertEquals(1, mockCacheExtension.onCreateCounter.get());
      assertEquals("value2", mockCacheExtension.getValue());

      return getAllNormalMembers(cache);
    });
  }

  /**
   * Tests for {@link Extension}, {@link Extensible}, {@link XmlParser}, {@link XmlGenerator},
   * {@link XmlEntity} as it applies to Extensions. Asserts that Mock Extension is created and
   * destroyed on region and cache.
   * 
   * @since GemFire 8.1
   */
  @Category(FlakyTest.class) // GEODE-1333
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
    newMember.invoke(() -> {
      Properties localProps = new Properties();

      File workingDir = new File(newMemberWorkingDir);
      workingDir.mkdirs();

      localProps.setProperty(MCAST_PORT, "0");
      localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
      localProps.setProperty(NAME, ClusterConfigurationDUnitTest.newMember);
      localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
      localProps.setProperty(DEPLOY_WORKING_DIR, workingDir.getCanonicalPath());

      getSystem(localProps);
      InternalCache cache = getCache();

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
      final Extensible<Cache> extensibleCache = cache;
      // Should not be any cache extensions
      assertTrue(!extensibleCache.getExtensionPoint().getExtensions().iterator().hasNext());

      return getAllNormalMembers(cache);
    });
  }

  @Test
  public void testCreateGatewaySenderReceiver() throws Exception {
    addIgnoredException("could not get remote locator");

    final String gsId = "GatewaySender1";
    final String batchSize = "1000";
    final String dispatcherThreads = "5";
    final String enableConflation = "false";
    final String manualStart = "false";
    final String alertThreshold = "1000";
    final String batchTimeInterval = "20";
    final String maxQueueMemory = "100";
    final String orderPolicy = GatewaySender.OrderPolicy.KEY.toString();
    final String parallel = "true";
    final String rmDsId = "250";
    final String socketBufferSize =
        String.valueOf(GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000);
    final String socketReadTimeout =
        String.valueOf(GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 200);

    Object[] result = setup();
    final int locatorPort = (Integer) result[0];

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYRECEIVER);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT, "10000");
    csb.addOption(CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT, "20000");
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS, "20");
    executeAndVerifyCommand(csb.getCommandString());

    csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__ID, gsId);
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

    // Start a new member which receives the shared configuration
    // Verify the config creation on this member
    VM newMember = getHost(0).getVM(2);
    final String newMemberWorkingDir =
        this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + newMember;

    newMember.invoke(() -> {
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
    });
  }

  private Object[] setup() throws IOException {
    final int locator1Port = getRandomAvailableTCPPort();
    final String locator1Name = "locator1-" + locator1Port;
    final String locatorLogPath = this.temporaryFolder.getRoot().getCanonicalPath() + File.separator
        + "locator-" + locator1Port + ".log";

    VM locatorAndMgr = getHost(0).getVM(3);
    Object[] result = locatorAndMgr.invoke(() -> {
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

      final Object[] results = new Object[4];
      results[0] = locator1Port;
      results[1] = jmxHost;
      results[2] = jmxPort;
      results[3] = httpPort;
      return results;
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
    dataMember.invoke(() -> {
      Properties localProps = new Properties();
      File workingDir = new File(dataMemberWorkingDir);
      workingDir.mkdirs();

      localProps.setProperty(MCAST_PORT, "0");
      localProps.setProperty(LOCATORS, "localhost[" + locator1Port + "]");
      localProps.setProperty(NAME, ClusterConfigurationDUnitTest.dataMember);
      localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
      localProps.setProperty(DEPLOY_WORKING_DIR, workingDir.getCanonicalPath());

      getSystem(localProps);
      InternalCache cache = getCache();
      assertNotNull(cache);
      return getAllNormalMembers(cache);
    });

    return result;
  }

  private void createRegion(String regionName, RegionShortcut regionShortCut, String group) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortCut.name());
    csb.addOptionWithValueCheck(CliStrings.GROUP, group);
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

  private CommandResult executeAndVerifyCommand(String commandString) {
    CommandResult cmdResult = executeCommand(commandString);
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("Command : " + commandString);
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
        .info("Command Result : " + commandResultToString(cmdResult));
    assertEquals(Status.OK, cmdResult.getStatus());
    assertFalse(cmdResult.failedToPersist());
    return cmdResult;
  }

  private void shutdownAll() throws IOException {
    VM locatorAndMgr = getHost(0).getVM(3);
    locatorAndMgr.invoke(() -> {
      GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();
      ShutdownAllRequest.send(cache.getInternalDistributedSystem().getDistributionManager(), -1);
      return null;
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
}
