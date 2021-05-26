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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.DiskInitFile;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@RunWith(JUnitParamsRunner.class)
public class OfflineDiskStoreCommandsDUnitTest implements Serializable {
  private static final String REGION_NAME = "testRegion";
  private static final String DISK_STORE_ID = "testDisk";
  private static final String WRONG_DISK_STORE_ID = "wrongTestDisk";

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public transient TemporaryFolder temporaryFolder = new TemporaryFolder();


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
        .<String, String>createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
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
  @Parameters({"compact offline-disk-store", "describe offline-disk-store",
      "validate offline-disk-store",
      "alter disk-store --region=testRegion --enable-statistics=true"})
  public void offlineDiskStoreCommandsSupportDiskStoresWithMultipleDirectories(String baseCommand)
      throws IOException {
    VM locator = getVM(0);
    VM server = getVM(1);
    final int ENTRIES = 100000;
    int site1Port = getRandomAvailableTCPPort();

    File diskStoreDirectory1 = temporaryFolder.newFolder("diskDir1");
    File diskStoreDirectory2 = temporaryFolder.newFolder("diskDir2");
    File diskStoreDirectory3 = temporaryFolder.newFolder("diskDir3");
    File[] diskStoreDirectories =
        new File[] {diskStoreDirectory1, diskStoreDirectory2, diskStoreDirectory3};
    String diskDirs = Arrays.stream(diskStoreDirectories).map(File::getAbsolutePath)
        .collect(Collectors.joining(","));

    locator.invoke(() -> cacheRule.createCache(createLocatorConfiguration(site1Port)));
    server.invoke(() -> cacheRule.createCache(createServerConfiguration(site1Port)));
    server.invoke(() -> {
      createServerWithRegionAndPersistentRegion(diskStoreDirectories);
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      IntStream.range(0, ENTRIES).forEach(i -> region.put("Key_" + i, "Value_" + i));
    });
    locator.invoke(this::gracefullyDisconnect);
    server.invoke(this::gracefullyDisconnect);
    gfsh.executeAndAssertThat(
        baseCommand + " --name=" + DISK_STORE_ID + " --disk-dirs=" + diskDirs)
        .statusIsSuccess();
  }

  @Test
  @Parameters({"describe offline-disk-store",
      "validate offline-disk-store",
      "alter disk-store --region=testRegion --enable-statistics=true"})
  public void offlineDiskStoreCommandsDoNotLeaveLingeringThreadsRunning(String baseCommand)
      throws IOException {
    VM locator = getVM(0);
    VM server = getVM(1);
    final int ENTRIES = 100000;
    int site1Port = getRandomAvailableTCPPort();
    String threadName = "Asynchronous disk writer for region";
    int counter = 0;

    File diskStoreDirectory1 = temporaryFolder.newFolder("diskDir1");

    File[] diskStoreDirectories =
        new File[] {diskStoreDirectory1};
    String diskDirs = Arrays.stream(diskStoreDirectories).map(File::getAbsolutePath)
        .collect(Collectors.joining(","));

    locator.invoke(() -> cacheRule.createCache(createLocatorConfiguration(site1Port)));
    server.invoke(() -> cacheRule.createCache(createServerConfiguration(site1Port)));
    server.invoke(() -> {
      createServerWithRegionAndPersistentRegion(diskStoreDirectories);
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      IntStream.range(0, ENTRIES).forEach(i -> region.put("Key_" + i, "Value_" + i));
    });
    locator.invoke(this::gracefullyDisconnect);
    server.invoke(this::gracefullyDisconnect);

    gfsh.executeAndAssertThat(
        baseCommand + " --name=" + DISK_STORE_ID + " --disk-dirs=" + diskDirs)
        .statusIsSuccess();

    File tempFile = temporaryFolder.newFile("dumpFile.txt");
    BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
    ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] infos = bean.dumpAllThreads(true, true);
    for (ThreadInfo info : infos) {
      if (info.toString().contains(threadName)) {
        writer.append(info.toString());
      }
    }

    writer.close();

    try (BufferedReader br = new BufferedReader(new FileReader(tempFile))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (line.contains(threadName)) {
          counter++;
        }
      }
    }
    assertThat(counter).isEqualTo(0);
  }

  @Test
  @Parameters({"compact offline-disk-store", "describe offline-disk-store",
      "validate offline-disk-store",
      "alter disk-store --region=testRegion --enable-statistics=true"})
  public void offlineDiskStoreCommandShouldFailWhenDiskStoreFileDoesNotExist(String baseCommand)
      throws IOException {
    VM locator = getVM(0);
    VM server = getVM(1);
    final int ENTRIES = 100000;
    int site1Port = getRandomAvailableTCPPort();

    File diskStoreDirectory1 = temporaryFolder.newFolder("diskDir1");
    File diskStoreDirectory2 = temporaryFolder.newFolder("diskDir2");
    File diskStoreDirectory3 = temporaryFolder.newFolder("diskDir3");
    File[] diskStoreDirectories =
        new File[] {diskStoreDirectory1, diskStoreDirectory2, diskStoreDirectory3};
    String diskDirs = Arrays.stream(diskStoreDirectories).map(File::getAbsolutePath)
        .collect(Collectors.joining(","));

    locator.invoke(() -> cacheRule.createCache(createLocatorConfiguration(site1Port)));
    server.invoke(() -> cacheRule.createCache(createServerConfiguration(site1Port)));
    server.invoke(() -> {
      createServerWithRegionAndPersistentRegion(diskStoreDirectories);
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      IntStream.range(0, ENTRIES).forEach(i -> region.put("Key_" + i, "Value_" + i));
    });
    locator.invoke(this::gracefullyDisconnect);
    server.invoke(this::gracefullyDisconnect);
    gfsh.executeAndAssertThat(
        baseCommand + " --name=" + WRONG_DISK_STORE_ID + " --disk-dirs=" + diskDirs)
        .statusIsError()
        .containsOutput("The init file " + diskStoreDirectory1 + File.separator + "BACKUP"
            + WRONG_DISK_STORE_ID + DiskInitFile.IF_FILE_EXT + " does not exist.");
  }
}
