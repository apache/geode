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

import static java.util.stream.Collectors.joining;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOAD_CLUSTER_CONFIGURATION_FROM_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.test.dunit.Disconnect;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.version.VersionManager;

public class ConfigurationPersistenceServiceUsingDirDUnitTest implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(4);

  @After
  public final void tearDown() throws Exception {
    for (int i = 0; i < 2; i++) {
      VM vm = getHost(0).getVM(i);
      vm.invoke("Removing shared configuration", () -> {
        InternalLocator locator = InternalLocator.getLocator();
        if (locator == null) {
          return;
        }

        InternalConfigurationPersistenceService sharedConfig =
            locator.getConfigurationPersistenceService();
        if (sharedConfig != null) {
          sharedConfig.destroySharedConfiguration();
        }
      });
    }
  }

  @Test
  public void basicClusterConfigDirWithOneLocator() throws Exception {
    final int[] ports = getRandomAvailableTCPPorts(1);

    for (int i = 0; i < ports.length; i++) {
      VM vm = getHost(0).getVM(i);
      copyClusterXml(vm, "cluster-region.xml");
      startLocator(i, ports);
      waitForSharedConfiguration(vm);
    }

    for (int i = 2; i < 4; i++) {
      VM vm = getHost(0).getVM(i);
      restartCache(vm, i, ports);

      vm.invoke("Checking for region presence", () -> {
        await().until(() -> CacheFactory.getAnyInstance().getRegion("newReplicatedRegion") != null);
      });
    }
  }

  @Test
  public void basicClusterConfigDirWithTwoLocators() throws Exception {
    final int[] ports = getRandomAvailableTCPPorts(2);

    for (int i = 0; i < ports.length; i++) {
      VM vm = getHost(0).getVM(i);
      copyClusterXml(vm, "cluster-region.xml");
      startLocator(i, ports);
      waitForSharedConfiguration(vm);
    }

    for (int i = 2; i < 4; i++) {
      VM vm = getHost(0).getVM(i);
      restartCache(vm, i, ports);

      vm.invoke("Checking for region presence", () -> {
        await().until(() -> CacheFactory.getAnyInstance().getRegion("newReplicatedRegion") != null);
      });
    }
  }

  @Test
  public void updateClusterConfigDirWithTwoLocatorsNoRollingServerRestart() throws Exception {
    final int[] ports = getRandomAvailableTCPPorts(2);

    for (int i = 0; i < ports.length; i++) {
      VM vm = getHost(0).getVM(i);
      copyClusterXml(vm, "cluster-empty.xml");
      startLocator(i, ports);
      waitForSharedConfiguration(vm);
    }

    for (int i = 2; i < 4; i++) {
      VM vm = getHost(0).getVM(i);
      restartCache(vm, i, ports);

      vm.invoke("Checking for region absence", () -> {
        Region r = CacheFactory.getAnyInstance().getRegion("newReplicatedRegion");
        assertNull("Region does exist", r);
      });
    }

    // Shut down the locators in reverse order to how we will start them up in the next step.
    // Unless we start them asynchronously, the older one will want to wait for a new diskstore
    // to become available and will time out.
    for (int i = ports.length; i > 0; i--) {
      VM vm = getHost(0).getVM(i - 1);
      stopLocator(vm);
    }

    for (int i = 0; i < ports.length; i++) {
      VM vm = getHost(0).getVM(i);
      copyClusterXml(vm, "cluster-region.xml");
      startLocator(i, ports);
      waitForSharedConfiguration(vm);
    }

    for (int i = 2; i < 4; i++) {
      VM vm = getHost(0).getVM(i);
      vm.invoke(Disconnect::disconnectFromDS);
    }

    for (int i = 2; i < 4; i++) {
      VM vm = getHost(0).getVM(i);
      restartCache(vm, i, ports);

      vm.invoke("Checking for region presence", () -> {
        await().until(() -> CacheFactory.getAnyInstance().getRegion("newReplicatedRegion") != null);
      });
    }
  }

  @Test
  public void updateClusterConfigDirWithTwoLocatorsAndRollingServerRestart() throws Exception {
    final int[] ports = getRandomAvailableTCPPorts(2);
    final int locatorCount = ports.length;

    for (int i = 0; i < locatorCount; i++) {
      VM vm = getHost(0).getVM(i);
      copyClusterXml(vm, "cluster-empty.xml");
      startLocator(i, ports);
      waitForSharedConfiguration(vm);
    }

    for (int i = 2; i < 4; i++) {
      VM vm = getHost(0).getVM(i);
      restartCache(vm, i, ports);

      vm.invoke("Checking for region absence", () -> {
        Region r = CacheFactory.getAnyInstance().getRegion("newReplicatedRegion");
        assertNull("Region does exist", r);
      });
    }

    // Shut down the locators in reverse order to how we will start them up in the next step.
    // Unless we start them asynchronously, the older one will want to wait for a new diskstore
    // to become available and will time out.
    for (int i = locatorCount; i > 0; i--) {
      VM vm = getHost(0).getVM(i - 1);
      stopLocator(vm);
    }

    for (int i = 0; i < locatorCount; i++) {
      VM vm = getHost(0).getVM(i);
      copyClusterXml(vm, "cluster-region.xml");
      startLocator(i, ports);
      waitForSharedConfiguration(vm);
    }

    for (int i = 2; i < 4; i++) {
      VM vm = getHost(0).getVM(i);
      restartCache(vm, i, ports);

      vm.invoke("Checking for region presence", () -> {
        await().until(() -> CacheFactory.getAnyInstance().getRegion("newReplicatedRegion") != null);
      });
    }
  }

  @Test
  public void updateClusterConfigDirWithTwoLocatorsRollingRestartAndRollingServerRestart()
      throws Exception {
    final int[] ports = getRandomAvailableTCPPorts(2);
    final int locatorCount = ports.length;

    for (int i = 0; i < locatorCount; i++) {
      VM vm = getHost(0).getVM(i);
      copyClusterXml(vm, "cluster-empty.xml");
      startLocator(i, ports);
      waitForSharedConfiguration(vm);
    }

    for (int i = 2; i < 4; i++) {
      VM vm = getHost(0).getVM(i);
      restartCache(vm, i, ports);

      vm.invoke("Checking for region absence", () -> {
        Region r = CacheFactory.getAnyInstance().getRegion("newReplicatedRegion");
        assertNull("Region does exist", r);
      });
    }

    // Roll the locators
    for (int i = locatorCount - 1; i >= 0; i--) {
      VM vm = getHost(0).getVM(i);
      stopLocator(vm);
      copyClusterXml(vm, "cluster-region.xml");
      startLocator(i, ports);
      waitForSharedConfiguration(vm);
    }

    // Roll the servers
    for (int i = 2; i < 4; i++) {
      VM vm = getHost(0).getVM(i);
      restartCache(vm, i, ports);

      vm.invoke("Checking for region presence", () -> {
        await().until(() -> CacheFactory.getAnyInstance().getRegion("newReplicatedRegion") != null);
      });
    }
  }

  private void copyClusterXml(final VM vm, final String clusterXml) {
    vm.invoke("Copying new cluster.xml from " + clusterXml, () -> {
      String clusterXmlPath =
          createTempFileFromResource(ConfigurationPersistenceServiceUsingDirDUnitTest.class,
              clusterXml).getAbsolutePath();
      InputStream cacheXml = new FileInputStream(clusterXmlPath);
      assertNotNull("Could not create InputStream from " + clusterXmlPath, cacheXml);
      Files.createDirectories(Paths.get("cluster_config", "cluster"));
      Files.copy(cacheXml, Paths.get("cluster_config", "cluster", "cluster.xml"),
          StandardCopyOption.REPLACE_EXISTING);
    });
  }

  private void startLocator(final int i, final int[] locatorPorts) {
    final String locatorName = "locator" + i;
    final File logFile = new File("locator-" + i + ".log");
    final Properties props = new Properties();
    props.setProperty(NAME, locatorName);
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    props.setProperty(LOAD_CLUSTER_CONFIGURATION_FROM_DIR, "true");
    props.setProperty(LOG_FILE, logFile.getAbsolutePath());

    if (locatorPorts.length > 1) {
      int otherLocatorPort = locatorPorts[(i + 1) % locatorPorts.length];
      props.setProperty(LOCATORS, "localhost[" + otherLocatorPort + "]");
    }

    clusterStartupRule.startLocatorVM(i, locatorPorts[i],
        VersionManager.CURRENT_VERSION,
        locatorStarterRule -> locatorStarterRule.withProperties(props));
  }

  private void waitForSharedConfiguration(final VM vm) {
    vm.invoke("Waiting for shared configuration", () -> {
      final InternalLocator locator = InternalLocator.getLocator();
      await().until(locator::isSharedConfigurationRunning);
    });
  }

  private void stopLocator(final VM vm) {
    vm.invoke("Stopping locator on " + vm, () -> {
      InternalLocator locator = InternalLocator.getLocator();
      assertNotNull("No locator found", locator);
      locator.stop();
      Disconnect.disconnectAllFromDS();
    });
  }

  private void restartCache(final VM vm, final int i, final int[] locatorPorts) {
    vm.invoke("Creating cache on VM " + i, Disconnect::disconnectFromDS);
    final Properties props = new Properties();
    props.setProperty(NAME, "member" + i);
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, getLocatorStr(locatorPorts));
    props.setProperty(LOG_FILE, "server-" + i + ".log");
    props.setProperty(USE_CLUSTER_CONFIGURATION, "true");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");

    clusterStartupRule.startServerVM(i, props);
  }

  private String getLocatorStr(final int[] locatorPorts) {
    return Arrays.stream(locatorPorts).mapToObj(p -> "localhost[" + p + "]").collect(joining(","));
  }
}
