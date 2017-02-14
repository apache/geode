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
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.configuration.handlers.ConfigurationRequestHandler;
import org.apache.geode.management.internal.configuration.messages.ConfigurationRequest;
import org.apache.geode.management.internal.configuration.messages.ConfigurationResponse;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Wait.waitForCriterion;

/**
 * Tests the starting up of shared configuration, installation of
 * {@link ConfigurationRequestHandler}
 */
@Category(DistributedTest.class)
public class ClusterConfigurationServiceDUnitTest extends JUnit4CacheTestCase {

  private static final String REGION1 = "region1";
  private static final int TIMEOUT = 10000;
  private static final int INTERVAL = 500;
  private static final String DISKSTORENAME = "diskStore1";

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    for (int i = 0; i < 4; i++) {
      getHost(0).getVM(i).invoke(SharedConfigurationTestUtils.cleanupLocator);
    }
  }

  @Test
  public void testGetHostedLocatorsWithSharedConfiguration() throws Exception {
    final VM locator1Vm = getHost(0).getVM(1);
    final VM locator2Vm = getHost(0).getVM(2);

    final String testName = getName();

    final int[] ports = getRandomAvailableTCPPorts(3);

    final int locator1Port = ports[0];
    final String locator1Name = "locator1" + locator1Port;

    locator1Vm.invoke(() -> {
      final File locatorLogFile = new File(testName + "-locator-" + locator1Port + ".log");

      final Properties locatorProps = new Properties();
      locatorProps.setProperty(NAME, locator1Name);
      locatorProps.setProperty(MCAST_PORT, "0");
      locatorProps.setProperty(LOG_LEVEL, "fine");
      locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");

      try {
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

      } catch (IOException e) {
        fail("Unable to create a locator with a shared configuration", e);
      }

      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      InternalDistributedMember me = cache.getMyId();
      DM dm = cache.getDistributionManager();

      Map<InternalDistributedMember, Collection<String>> hostedLocators = dm.getAllHostedLocators();
      assertFalse(hostedLocators.isEmpty());

      Map<InternalDistributedMember, Collection<String>> hostedLocatorsWithSharedConfiguration =
          dm.getAllHostedLocatorsWithSharedConfiguration();
      assertFalse(hostedLocatorsWithSharedConfiguration.isEmpty());

      assertNotNull(hostedLocators.get(me));
      assertNotNull(hostedLocatorsWithSharedConfiguration.get(me));
      return null;
    });

    final int locator2Port = ports[1];
    final String locator2Name = "locator2" + locator2Port;

    locator2Vm.invoke(() -> {
      final File locatorLogFile = new File(testName + "-locator-" + locator2Port + ".log");

      final Properties locatorProps = new Properties();
      locatorProps.setProperty(NAME, locator2Name);
      locatorProps.setProperty(MCAST_PORT, "0");
      locatorProps.setProperty(LOG_LEVEL, "fine");
      locatorProps.setProperty(LOCATORS, "localhost[" + locator1Port + "]");
      locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

      final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator2Port,
          locatorLogFile, null, locatorProps);

      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      InternalDistributedMember me = cache.getMyId();
      DM dm = cache.getDistributionManager();

      Map<InternalDistributedMember, Collection<String>> hostedLocators = dm.getAllHostedLocators();
      assertFalse(hostedLocators.isEmpty());

      Map<InternalDistributedMember, Collection<String>> hostedLocatorsWithSharedConfiguration =
          dm.getAllHostedLocatorsWithSharedConfiguration();
      assertFalse(hostedLocatorsWithSharedConfiguration.isEmpty());
      assertNotNull(hostedLocators.get(me));
      assertNull(hostedLocatorsWithSharedConfiguration.get(me));
      assertTrue(hostedLocators.size() == 2);
      assertTrue(hostedLocatorsWithSharedConfiguration.size() == 1);

      Set<InternalDistributedMember> locatorsWithSharedConfig =
          hostedLocatorsWithSharedConfiguration.keySet();
      Set<String> locatorsWithSharedConfigNames = new HashSet<String>();

      for (InternalDistributedMember locatorWithSharedConfig : locatorsWithSharedConfig) {
        locatorsWithSharedConfigNames.add(locatorWithSharedConfig.getName());
      }
      assertTrue(locatorsWithSharedConfigNames.contains(locator1Name));

      return null;
    });

    locator1Vm.invoke(() -> {
      InternalLocator locator = (InternalLocator) Locator.getLocator();
      ClusterConfigurationService sharedConfig = locator.getSharedConfiguration();
      sharedConfig.destroySharedConfiguration();
      locator.stop();
      return null;
    });

    locator2Vm.invoke(() -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      InternalDistributedMember me = cache.getMyId();
      DM dm = cache.getDistributionManager();
      Map<InternalDistributedMember, Collection<String>> hostedLocators = dm.getAllHostedLocators();
      assertFalse(hostedLocators.isEmpty());
      Map<InternalDistributedMember, Collection<String>> hostedLocatorsWithSharedConfiguration =
          dm.getAllHostedLocatorsWithSharedConfiguration();
      assertTrue(hostedLocatorsWithSharedConfiguration.isEmpty());
      assertNotNull(hostedLocators.get(me));
      assertNull(hostedLocatorsWithSharedConfiguration.get(me));
      assertTrue(hostedLocators.size() == 1);
      assertTrue(hostedLocatorsWithSharedConfiguration.size() == 0);
      return null;
    });
  }

  @Test
  public void testSharedConfigurationService() throws Exception {
    // Start the Locator and wait for shared configuration to be available
    final String testGroup = "G1";
    final String clusterLogLevel = "error";
    final String groupLogLevel = "fine";

    final String testName = getName();

    final VM locator1Vm = getHost(0).getVM(1);
    final VM dataMemberVm = getHost(0).getVM(2);
    final VM locator2Vm = getHost(0).getVM(3);

    final int[] ports = getRandomAvailableTCPPorts(3);
    final int locator1Port = ports[0];

    locator1Vm.invoke(() -> {
      final File locatorLogFile = new File(testName + "-locator-" + locator1Port + ".log");

      final Properties locatorProps = new Properties();
      locatorProps.setProperty(NAME, "Locator1");
      locatorProps.setProperty(MCAST_PORT, "0");
      locatorProps.setProperty(LOG_LEVEL, "info");
      locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");

      try {
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

      } catch (IOException e) {
        fail("Unable to create a locator with a shared configuration", e);
      }
    });

    XmlEntity xmlEntity = dataMemberVm.invoke(() -> {
      Properties localProps = new Properties();
      localProps.setProperty(MCAST_PORT, "0");
      localProps.setProperty(LOCATORS, "localhost[" + locator1Port + "]");
      localProps.setProperty(GROUPS, testGroup);

      getSystem(localProps);
      Cache cache = getCache();
      assertNotNull(cache);

      DiskStoreFactory dsFactory = cache.createDiskStoreFactory();
      File dsDir = new File("dsDir");
      if (!dsDir.exists()) {
        dsDir.mkdir();
      }
      dsFactory.setDiskDirs(new File[] {dsDir});
      dsFactory.create(DISKSTORENAME);

      RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.REPLICATE);
      regionFactory.create(REGION1);
      return new XmlEntity(CacheXml.REGION, "name", REGION1);
    });

    locator1Vm.invoke(() -> {
      ClusterConfigurationService sc = InternalLocator.getLocator().getSharedConfiguration();
      sc.addXmlEntity(xmlEntity, new String[] {testGroup});

      // Modify property and cache attributes
      Properties clusterProperties = new Properties();
      clusterProperties.setProperty(LOG_LEVEL, clusterLogLevel);
      XmlEntity cacheEntity = XmlEntity.builder().withType(CacheXml.CACHE).build();
      Map<String, String> cacheAttributes = new HashMap<String, String>();
      cacheAttributes.put(CacheXml.COPY_ON_READ, "true");

      sc.modifyXmlAndProperties(clusterProperties, cacheEntity, null);

      clusterProperties.setProperty(LOG_LEVEL, groupLogLevel);
      sc.modifyXmlAndProperties(clusterProperties, cacheEntity, new String[] {testGroup});

      // Add a jar
      byte[][] jarBytes = new byte[1][];
      jarBytes[0] = "Hello".getBytes();
      assertTrue(sc.addJarsToThisLocator(new String[] {"foo.jar"}, jarBytes, null));

      // Add a jar for the group
      jarBytes = new byte[1][];
      jarBytes[0] = "Hello".getBytes();
      assertTrue(
          sc.addJarsToThisLocator(new String[] {"bar.jar"}, jarBytes, new String[] {testGroup}));
    });

    final int locator2Port = ports[1];

    // Create another locator in VM2
    locator2Vm.invoke(() -> {
      final File locatorLogFile = new File(testName + "-locator-" + locator2Port + ".log");

      final Properties locatorProps = new Properties();
      locatorProps.setProperty(NAME, "Locator2");
      locatorProps.setProperty(MCAST_PORT, "0");
      locatorProps.setProperty(LOG_LEVEL, "info");
      locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
      locatorProps.setProperty(LOCATORS, "localhost[" + locator1Port + "]");

      try {
        final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator2Port,
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

      } catch (IOException e) {
        fail("Unable to create a locator with a shared configuration", e);
      }

      InternalLocator locator = (InternalLocator) Locator.getLocator();
      ClusterConfigurationService sharedConfig = locator.getSharedConfiguration();
      Map<String, Configuration> entireConfiguration = sharedConfig.getEntireConfiguration();
      Configuration clusterConfig =
          entireConfiguration.get(ClusterConfigurationService.CLUSTER_CONFIG);
      assertNotNull(clusterConfig);
      assertNotNull(clusterConfig.getJarNames());
      assertTrue(clusterConfig.getJarNames().contains("foo.jar"));
      assertTrue(
          clusterConfig.getGemfireProperties().getProperty(LOG_LEVEL).equals(clusterLogLevel));
      assertNotNull(clusterConfig.getCacheXmlContent());

      Configuration testGroupConfiguration = entireConfiguration.get(testGroup);
      assertNotNull(testGroupConfiguration);
      assertNotNull(testGroupConfiguration.getJarNames());
      assertTrue(testGroupConfiguration.getJarNames().contains("bar.jar"));
      assertTrue(testGroupConfiguration.getGemfireProperties().getProperty(LOG_LEVEL)
          .equals(groupLogLevel));
      assertNotNull(testGroupConfiguration.getCacheXmlContent());
      assertTrue(testGroupConfiguration.getCacheXmlContent().contains(REGION1));

      Map<String, byte[]> jarData =
          sharedConfig.getAllJarsFromThisLocator(entireConfiguration.keySet());
      String[] jarNames = jarData.keySet().stream().toArray(String[]::new);
      byte[][] jarBytes = jarData.values().toArray(new byte[jarNames.length][]);

      assertNotNull(jarNames);
      assertNotNull(jarBytes);

      sharedConfig.deleteXmlEntity(new XmlEntity(CacheXml.REGION, "name", REGION1),
          new String[] {testGroup});
      sharedConfig.removeJars(new String[] {"foo.jar"}, null);
      sharedConfig.removeJars(null, null);
    });

    dataMemberVm.invoke(() -> {
      Set<String> groups = new HashSet<String>();
      groups.add(testGroup);
      ConfigurationRequest configRequest = new ConfigurationRequest(groups);
      ConfigurationResponse configResponse = (ConfigurationResponse) new TcpClient()
          .requestToServer(InetAddress.getByName("localhost"), locator2Port, configRequest, 1000);
      assertNotNull(configResponse);

      Map<String, Configuration> requestedConfiguration =
          configResponse.getRequestedConfiguration();
      Configuration clusterConfiguration =
          requestedConfiguration.get(ClusterConfigurationService.CLUSTER_CONFIG);
      assertNotNull(clusterConfiguration);
      assertTrue(configResponse.getJarNames().length == 0);
      assertTrue(configResponse.getJars().length == 0);
      assertTrue(clusterConfiguration.getJarNames().isEmpty());
      assertTrue(clusterConfiguration.getGemfireProperties().getProperty(LOG_LEVEL)
          .equals(clusterLogLevel));

      Configuration testGroupConfiguration = requestedConfiguration.get(testGroup);
      assertNotNull(testGroupConfiguration);
      assertFalse(testGroupConfiguration.getCacheXmlContent().contains(REGION1));
      assertTrue(testGroupConfiguration.getJarNames().isEmpty());
      assertTrue(testGroupConfiguration.getGemfireProperties().getProperty(LOG_LEVEL)
          .equals(groupLogLevel));

      GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
      Map<InternalDistributedMember, Collection<String>> locatorsWithSharedConfiguration =
          cache.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration();
      assertFalse(locatorsWithSharedConfiguration.isEmpty());
      assertTrue(locatorsWithSharedConfiguration.size() == 2);
      Set<InternalDistributedMember> locatorMembers = locatorsWithSharedConfiguration.keySet();
      for (InternalDistributedMember locatorMember : locatorMembers) {
        System.out.println(locatorMember);
      }
      return null;
    });
  }
}
