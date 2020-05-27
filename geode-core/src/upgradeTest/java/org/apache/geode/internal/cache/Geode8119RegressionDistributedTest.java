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

package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPortsForDUnitSite;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@RunWith(Parameterized.class)
public class Geode8119RegressionDistributedTest implements Serializable {
  private static final String REGION_NAME = "TestRegion";
  private static final String DISK_STORE_ID = "testDisk";

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Parameterized.Parameter
  public RegionShortcut regionShortcut;

  @Parameterized.Parameters(name = "RegionType={0}")
  public static Iterable<RegionShortcut> data() {
    return Arrays.asList(RegionShortcut.PARTITION_PERSISTENT, RegionShortcut.REPLICATE_PERSISTENT);
  }

  private Properties createLocatorConfiguration(int localLocatorPort) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "localhost[" + localLocatorPort + ']');
    config.setProperty(START_LOCATOR,
        "localhost[" + localLocatorPort + "],server=true,peer=true,hostname-for-clients=localhost");

    return config;
  }

  private Properties createServerConfiguration(int localLocatorPort) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "localhost[" + localLocatorPort + ']');

    return config;
  }

  private void createDiskStore(File[] diskStoreDirectories) {
    DiskStoreFactory diskStoreFactory = cacheRule.getCache().createDiskStoreFactory();
    diskStoreFactory.setMaxOplogSize(1);
    diskStoreFactory.setAutoCompact(true);
    diskStoreFactory.setAllowForceCompaction(true);
    diskStoreFactory.setDiskDirs(diskStoreDirectories);

    diskStoreFactory.create(DISK_STORE_ID);
  }

  private void createRegion() {
    cacheRule.getCache()
        .<String, String>createRegionFactory(regionShortcut)
        .setDiskStoreName(DISK_STORE_ID)
        .create(REGION_NAME);
  }

  private void createServerWithRegionAndPersistentRegion(File[] diskStoreDirectories) {
    createDiskStore(diskStoreDirectories);
    createRegion();
    cacheRule.getCache().getRegion(REGION_NAME);
  }

  private void gracefullyDisconnect() {
    InternalDistributedSystem.getConnectedInstance().stopReconnectingNoDisconnect();
    InternalDistributedSystem.getConnectedInstance().disconnect();
    await()
        .untilAsserted(() -> assertThat(InternalDistributedSystem.getConnectedInstance()).isNull());
  }

  @Test
  public void regressionTest() throws IOException {
    VM locator, server;
    final int ENTRIES = 100000;
    final Host host = Host.getHost(0);
    int site1Port = getRandomAvailableTCPPortsForDUnitSite(1)[0];
    File diskStoreDirectory1 = temporaryFolder.newFolder("diskDir1");
    File diskStoreDirectory2 = temporaryFolder.newFolder("diskDir2");
    File diskStoreDirectory3 = temporaryFolder.newFolder("diskDir3");
    File[] diskStoreDirectories =
        new File[] {diskStoreDirectory1, diskStoreDirectory2, diskStoreDirectory3};
    String diskDirs = Arrays.stream(diskStoreDirectories).map(File::getAbsolutePath)
        .collect(Collectors.joining(","));

    // Start cluster using Geode 1.12.0, insert some entries, stop cluster and validate disk-store.
    locator = host.getVM(Version.GEODE_1_12_0.getName(), 0);
    server = host.getVM(Version.GEODE_1_12_0.getName(), 1);
    locator.invoke(() -> cacheRule.createCache(createLocatorConfiguration(site1Port)));
    server.invoke(() -> {
      cacheRule.createCache(createServerConfiguration(site1Port));
      createServerWithRegionAndPersistentRegion(diskStoreDirectories);
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      IntStream.range(0, ENTRIES).forEach(i -> region.put("Key_" + i, "Value_" + i));
    });
    locator.invoke(this::gracefullyDisconnect);
    server.invoke(this::gracefullyDisconnect);
    gfsh.executeAndAssertThat(
        "validate offline-disk-store --name=" + DISK_STORE_ID + " --disk-dirs=" + diskDirs)
        .statusIsSuccess();

    // Start cluster using current Geode version, verify data, stop cluster and validate disk-store.
    locator = getVM(2);
    server = getVM(3);
    locator.invoke(() -> cacheRule.createCache(createLocatorConfiguration(site1Port)));
    server.invoke(() -> cacheRule.createCache(createServerConfiguration(site1Port)));
    server.invoke(() -> {
      createServerWithRegionAndPersistentRegion(diskStoreDirectories);
      await().untilAsserted(
          () -> assertThat(cacheRule.getCache().getRegion(REGION_NAME).size()).isEqualTo(ENTRIES));
    });
    locator.invoke(this::gracefullyDisconnect);
    server.invoke(this::gracefullyDisconnect);
    gfsh.executeAndAssertThat(
        "validate offline-disk-store --name=" + DISK_STORE_ID + " --disk-dirs=" + diskDirs)
        .statusIsSuccess();
  }
}
