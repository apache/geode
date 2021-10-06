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

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.lang.SystemPropertyHelper;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

public class CompactOfflineDiskStoreDUnitTest implements Serializable {

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public DistributedRule distributedRule = new DistributedRule(2);

  private String locatorName;

  private File locatorDir;

  private int locatorPort;

  private int locatorJmxPort;

  private static final LocatorLauncher DUMMY_LOCATOR = mock(LocatorLauncher.class);

  private static final AtomicReference<LocatorLauncher> LOCATOR =
      new AtomicReference<>(DUMMY_LOCATOR);

  private VM server;

  private String serverName;

  private File serverDir;

  private int serverPort;

  private String locators;

  private static final ServerLauncher DUMMY_SERVER = mock(ServerLauncher.class);

  private static final AtomicReference<ServerLauncher> SERVER =
      new AtomicReference<>(DUMMY_SERVER);

  private final int NUM_ENTRIES = 1000;

  private static final String DISK_STORE_NAME = "testDiskStore";

  private static final String REGION_NAME = "testRegion";

  @Before
  public void setUp() throws Exception {
    VM locator = getVM(0);
    server = getVM(1);

    locatorName = "locator";
    serverName = "server";

    locatorDir = temporaryFolder.newFolder(locatorName);

    serverDir = temporaryFolder.newFolder(serverName);

    int[] port = getRandomAvailableTCPPorts(3);
    locatorPort = port[0];
    locatorJmxPort = port[1];
    serverPort = port[2];

    locators = "localhost[" + locatorPort + "]";

    Invoke.invokeInEveryVM(() -> System.setProperty("jdk.serialFilter", "*"));

    locator.invoke(() -> startLocator(locatorName, locatorDir, locatorPort, locatorJmxPort));

    gfsh.connectAndVerify(locatorJmxPort, GfshCommandRule.PortType.jmxManager);

    server.invoke(() -> startServer(serverName, serverDir, serverPort, locators, true));
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> {
      SERVER.getAndSet(DUMMY_SERVER).stop();
      LOCATOR.getAndSet(DUMMY_LOCATOR).stop();
    });
    disconnectAllFromDS();
  }

  @Test
  public void testDuplicateDiskStoreCompaction() {

    createDiskStore();

    createRegion();

    populateRegions();

    assertRegionSizeAndDiskStore();

    server.invoke(CompactOfflineDiskStoreDUnitTest::stopServer);

    server.invoke(this::compactOfflineDiskStore);

    server.invoke(() -> startServer(serverName, serverDir, serverPort, locators, false));

    server.invoke(CompactOfflineDiskStoreDUnitTest::verifyDiskStoreOplogs);

    assertRegionSizeAndDiskStore();

  }

  private void compactOfflineDiskStore() throws Exception {
    DiskStoreImpl.offlineCompact(DISK_STORE_NAME, new File[] {serverDir}, false/* upgrade */, -1);
  }

  private static void startLocator(String name, File workingDirectory, int locatorPort,
      int jmxPort) {
    LOCATOR.set(new LocatorLauncher.Builder()
        .setMemberName(name)
        .setPort(locatorPort)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(workingDirectory, name + ".log").getAbsolutePath())
        .set(MAX_WAIT_TIME_RECONNECT, "1000")
        .set(MEMBER_TIMEOUT, "2000")
        .build());

    LOCATOR.get().start();

    await().untilAsserted(() -> {
      InternalLocator locator = (InternalLocator) LOCATOR.get().getLocator();
      assertThat(locator.isSharedConfigurationRunning())
          .as("Locator shared configuration is running on locator" + getVMId())
          .isTrue();
    });
  }

  private static void startServer(String name, File workingDirectory, int serverPort,
      String locators, boolean parallelDiskStoreRecovery) {

    System.setProperty(GEODE_PREFIX + SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY,
        String.valueOf(parallelDiskStoreRecovery));

    SERVER.set(new ServerLauncher.Builder()
        .setDeletePidFileOnStop(Boolean.TRUE)
        .setMemberName(name)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .setServerPort(serverPort)
        .set(HTTP_SERVICE_PORT, "0")
        .set(LOCATORS, locators)
        .set(LOG_FILE, new File(workingDirectory, name + ".log").getAbsolutePath())
        .set(MAX_WAIT_TIME_RECONNECT, "1000")
        .set(MEMBER_TIMEOUT, "2000")
        .build());

    SERVER.get().start();
  }

  private static void stopServer() {
    SERVER.get().stop();
  }

  private static void verifyDiskStoreOplogs() {
    ((InternalCache) SERVER.get().getCache()).listDiskStores().forEach(diskStore -> {
      Oplog[] oplogs = ((DiskStoreImpl) diskStore).getPersistentOplogs().getAllOplogs();
      // There should be two Oplogs in the array.
      // One is the offline compacted Oplog.
      // The other is the new Oplog created during server restart.
      assertThat(oplogs.length).isEqualTo(2);
    });
  }

  private void assertRegionSizeAndDiskStore() {
    assertRegionSize();
    assertDiskStore(serverName);
  }

  private void assertDiskStore(String serverName) {
    String command;
    command = new CommandStringBuilder("describe disk-store")
        .addOption("name", CompactOfflineDiskStoreDUnitTest.DISK_STORE_NAME)
        .addOption("member", serverName)
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess().containsOutput(
        CompactOfflineDiskStoreDUnitTest.REGION_NAME);
  }

  private void assertRegionSize() {
    String command;
    command = new CommandStringBuilder("describe region")
        .addOption("name", CompactOfflineDiskStoreDUnitTest.REGION_NAME)
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput(String.valueOf(NUM_ENTRIES));
  }

  private void populateRegions() {
    ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
    ClientCache clientCache =
        clientCacheFactory.addPoolLocator("localhost", locatorPort).create();

    Region<Object, Object> clientRegion1 = clientCache
        .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(REGION_NAME);

    IntStream.range(0, NUM_ENTRIES).forEach(i -> {
      clientRegion1.put("key-" + i, "value-" + i);
      clientRegion1.put("key-" + i, "value-" + i + 1); // update again for future compaction
    });
  }

  private void createRegion() {
    String command;
    command = new CommandStringBuilder("create region")
        .addOption("name", CompactOfflineDiskStoreDUnitTest.REGION_NAME)
        .addOption("type", "PARTITION_PERSISTENT")
        .addOption("disk-store", CompactOfflineDiskStoreDUnitTest.DISK_STORE_NAME)
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  private void createDiskStore() {
    String command;
    command = new CommandStringBuilder("create disk-store")
        .addOption("name", CompactOfflineDiskStoreDUnitTest.DISK_STORE_NAME)
        .addOption("dir", serverDir.getAbsolutePath())
        .addOption("auto-compact", "true")
        .addOption("allow-force-compaction", "true")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }


}
