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
package org.apache.geode.distributed;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.internal.ClusterDistributionManager.LOCATOR_DM_TYPE;
import static org.apache.geode.distributed.internal.ClusterDistributionManager.NORMAL_DM_TYPE;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.rules.DistributedRule.getLocators;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.rules.DistributedReference;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

public class DmTypesDistributedTest {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public DistributedReference<LocatorLauncher> locatorLauncher = new DistributedReference<>();
  @Rule
  public DistributedReference<ServerLauncher> serverLauncher = new DistributedReference<>();
  @Rule
  public DistributedReference<Cache> cache = new DistributedReference<>();
  @Rule
  public DistributedReference<Locator> locator = new DistributedReference<>();
  @Rule
  public DistributedRestoreSystemProperties restoreProps = new DistributedRestoreSystemProperties();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Test
  public void serverLauncherCreatesNormalDmType() throws IOException {
    String name = "server-" + getVMId();
    ServerLauncher serverLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(temporaryFolder.newFolder(name).getAbsolutePath())
        .setMemberName(name)
        .setDisableDefaultServer(true)
        .set(LOCATORS, getLocators())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER_PORT, "0")
        .build();
    serverLauncher.start();
    this.serverLauncher.set(serverLauncher);

    DistributionManager distributionManager = getDistributionManager(serverLauncher.getCache());

    assertThat(distributionManager.getDMType()).isEqualTo(NORMAL_DM_TYPE);
  }

  @Test
  public void locatorLauncherCreatesNormalDmType() throws IOException {
    String name = "locator-" + getVMId();
    int locatorPort = getRandomAvailableTCPPort();
    LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(temporaryFolder.newFolder(name).getAbsolutePath())
        .setMemberName(name)
        .setPort(locatorPort)
        .set(LOCATORS, getLocators())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER_PORT, "0")
        .build();
    locatorLauncher.start();
    this.locatorLauncher.set(locatorLauncher);

    DistributionManager distributionManager = getDistributionManager(locatorLauncher.getLocator());

    assertThat(distributionManager.getDMType()).isEqualTo(LOCATOR_DM_TYPE);
  }

  @Test
  public void cacheFactoryCreatesNormalDmType() {
    Cache cache = new CacheFactory()
        .set(LOCATORS, getLocators())
        .create();
    this.cache.set(cache);

    DistributionManager distributionManager = getDistributionManager(cache);

    assertThat(distributionManager.getDMType()).isEqualTo(NORMAL_DM_TYPE);
  }

  @Test
  public void locatorWithoutForceLocatorDmTypeCreatesNormalDmType() throws IOException {
    File logFile = Paths.get(temporaryFolder.getRoot().getAbsolutePath(), "locator.log").toFile();
    int locatorPort = getRandomAvailableTCPPort();
    Locator locator = Locator.startLocatorAndDS(locatorPort, logFile, new Properties());
    this.locator.set(locator);

    DistributionManager distributionManager = getDistributionManager(locator);

    assertThat(distributionManager.getDMType()).isEqualTo(LOCATOR_DM_TYPE);
  }

  @Test
  public void locatorWithForceLocatorDmTypeCreatesLocatorDmType() throws IOException {
    System.setProperty(InternalLocator.FORCE_LOCATOR_DM_TYPE, "true");
    File logFile = Paths.get(temporaryFolder.getRoot().getAbsolutePath(), "locator.log").toFile();
    int locatorPort = getRandomAvailableTCPPort();
    Locator locator = Locator.startLocatorAndDS(locatorPort, logFile, new Properties());
    this.locator.set(locator);

    DistributionManager distributionManager = getDistributionManager(locator);

    assertThat(distributionManager.getDMType()).isEqualTo(LOCATOR_DM_TYPE);
  }

  private static DistributionManager getDistributionManager(Cache cache) {
    InternalCache internalCache = (InternalCache) cache;
    return internalCache.getDistributionManager();
  }

  private static DistributionManager getDistributionManager(Locator locator) {
    InternalLocator internalLocator = (InternalLocator) locator;
    InternalDistributedSystem system =
        (InternalDistributedSystem) internalLocator.getDistributedSystem();
    return system.getDistributionManager();
  }
}
