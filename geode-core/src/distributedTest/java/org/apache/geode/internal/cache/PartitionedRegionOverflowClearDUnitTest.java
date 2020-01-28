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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

public class PartitionedRegionOverflowClearDUnitTest implements Serializable {

  @Rule
  public DistributedRule distributedRule = new DistributedRule(5);

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  private VM locator;
  private VM server1;
  private VM server2;
  private VM accessor;
  private VM client;

  private static final String LOCATOR_NAME = "locator";
  private static final String SERVER1_NAME = "server1";
  private static final String SERVER2_NAME = "server2";
  private static final String SERVER3_NAME = "server3";

  private File locatorDir;
  private File server1Dir;
  private File server2Dir;
  private File server3Dir;

  private String locatorString;

  private int locatorPort;
  private int locatorJmxPort;
  private int locatorHttpPort;
  private int serverPort1;
  private int serverPort2;
  private int serverPort3;

  private static final AtomicReference<LocatorLauncher> LOCATOR_LAUNCHER = new AtomicReference<>();

  private static final AtomicReference<ServerLauncher> SERVER_LAUNCHER = new AtomicReference<>();

  private static final AtomicReference<ClientCache> CLIENT_CACHE = new AtomicReference<>();

  private static final String OVERFLOW_REGION_NAME = "testOverflowRegion";

  public static final int NUM_ENTRIES = 1000;

  @Before
  public void setup() throws Exception {
    locator = getVM(0);
    server1 = getVM(1);
    server2 = getVM(2);
    accessor = getVM(3);
    client = getVM(4);

    locatorDir = temporaryFolder.newFolder(LOCATOR_NAME);
    server1Dir = temporaryFolder.newFolder(SERVER1_NAME);
    server2Dir = temporaryFolder.newFolder(SERVER2_NAME);
    server3Dir = temporaryFolder.newFolder(SERVER3_NAME);

    int[] ports = getRandomAvailableTCPPorts(6);
    locatorPort = ports[0];
    locatorJmxPort = ports[1];
    locatorHttpPort = ports[2];
    serverPort1 = ports[3];
    serverPort2 = ports[4];
    serverPort3 = ports[5];

    locator.invoke(
        () -> startLocator(locatorDir, locatorPort, locatorJmxPort, locatorHttpPort));
    gfsh.connectAndVerify(locatorJmxPort, GfshCommandRule.PortType.jmxManager);

    locatorString = "localhost[" + locatorPort + "]";
    server1.invoke(() -> startServer(SERVER1_NAME, server1Dir, locatorString, serverPort1));
    server2.invoke(() -> startServer(SERVER2_NAME, server2Dir, locatorString, serverPort2));
  }

  @After
  public void tearDown() {
    destroyRegion();
    destroyDiskStore(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);

    for (VM vm : new VM[] {client, accessor, server1, server2, locator}) {
      vm.invoke(() -> {
        if (CLIENT_CACHE.get() != null) {
          CLIENT_CACHE.get().close();
        }
        if (LOCATOR_LAUNCHER.get() != null) {
          LOCATOR_LAUNCHER.get().stop();
        }
        if (SERVER_LAUNCHER.get() != null) {
          SERVER_LAUNCHER.get().stop();
        }

        CLIENT_CACHE.set(null);
        LOCATOR_LAUNCHER.set(null);
        SERVER_LAUNCHER.set(null);
      });
    }
  }

  @Test
  public void testGfshClearRegionWithOverflow() throws InterruptedException {
    createPartitionRedundantPersistentOverflowRegion();

    populateRegion();
    assertRegionSize(NUM_ENTRIES);

    gfsh.executeAndAssertThat("clear region --name=" + OVERFLOW_REGION_NAME).statusIsSuccess();
    assertRegionSize(0);

    restartServers();

    assertRegionSize(0);
  }

  @Test
  public void testClientRegionClearWithOverflow() throws InterruptedException {
    createPartitionRedundantPersistentOverflowRegion();

    populateRegion();
    assertRegionSize(NUM_ENTRIES);

    client.invoke(() -> {
      if (CLIENT_CACHE.get() == null) {
        ClientCache clientCache =
            new ClientCacheFactory().addPoolLocator("localhost", locatorPort).create();
        CLIENT_CACHE.set(clientCache);
      }

      CLIENT_CACHE.get().getRegion(OVERFLOW_REGION_NAME).clear();
    });
    assertRegionSize(0);

    restartServers();

    assertRegionSize(0);
  }

  @Test
  public void testAccessorRegionClearWithOverflow() throws InterruptedException {

    for (VM vm : toArray(server1, server2)) {
      vm.invoke(this::createRegionWithDefaultDiskStore);
    }

    accessor.invoke(() -> {
      startServer(SERVER3_NAME, server3Dir, locatorString, serverPort3);
      SERVER_LAUNCHER.get().getCache()
          .createRegionFactory(RegionShortcut.PARTITION_REDUNDANT_OVERFLOW)
          .setPartitionAttributes(
              new PartitionAttributesFactory().setLocalMaxMemory(0).create())
          .create(OVERFLOW_REGION_NAME);
    });

    populateRegion();
    assertRegionSize(NUM_ENTRIES);

    accessor.invoke(() -> {
      assertThat(SERVER_LAUNCHER.get().getCache().getRegion(OVERFLOW_REGION_NAME).size())
          .isEqualTo(NUM_ENTRIES);
      SERVER_LAUNCHER.get().getCache().getRegion(OVERFLOW_REGION_NAME).clear();
      assertThat(SERVER_LAUNCHER.get().getCache().getRegion(OVERFLOW_REGION_NAME).size())
          .isEqualTo(0);
    });
    assertRegionSize(0);

    for (VM vm : toArray(server1, server2)) {
      vm.invoke(PartitionedRegionOverflowClearDUnitTest::stopServer);
    }

    gfsh.executeAndAssertThat("list members").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("locator");
    AsyncInvocation asyncInvocation1 = server1.invokeAsync(() -> {
      startServer(SERVER1_NAME, server1Dir, locatorString, serverPort1);
      createRegionWithDefaultDiskStore();
    });
    AsyncInvocation asyncInvocation2 = server2.invokeAsync(() -> {
      startServer(SERVER2_NAME, server2Dir, locatorString, serverPort2);
      createRegionWithDefaultDiskStore();
    });
    asyncInvocation1.get();
    asyncInvocation2.get();
    assertRegionSize(0);
  }

  private void restartServers() throws InterruptedException {
    for (VM vm : toArray(server1, server2)) {
      vm.invoke(PartitionedRegionOverflowClearDUnitTest::stopServer);
    }

    gfsh.executeAndAssertThat("list members").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("locator");
    AsyncInvocation asyncInvocation1 =
        server1
            .invokeAsync(() -> startServer(SERVER1_NAME, server1Dir, locatorString, serverPort1));
    AsyncInvocation asyncInvocation2 =
        server2
            .invokeAsync(() -> startServer(SERVER2_NAME, server2Dir, locatorString, serverPort2));
    asyncInvocation1.get();
    asyncInvocation2.get();
  }

  private void createPartitionRedundantPersistentOverflowRegion() {
    String command = new CommandStringBuilder("create region")
        .addOption("name", OVERFLOW_REGION_NAME)
        .addOption("type", "PARTITION_REDUNDANT_PERSISTENT_OVERFLOW")
        .addOption("redundant-copies", "1")
        .addOption("eviction-entry-count", "1")
        .addOption("eviction-action", "overflow-to-disk")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  private void destroyRegion() {
    server1.invoke(() -> {
      assertThat(SERVER_LAUNCHER.get().getCache().getRegion(OVERFLOW_REGION_NAME)).isNotNull();
      SERVER_LAUNCHER.get().getCache().getRegion(OVERFLOW_REGION_NAME).destroyRegion();

    });
  }

  private void destroyDiskStore(String diskStoreName) {
    String command = new CommandStringBuilder("destroy disk-store")
        .addOption("name", diskStoreName)
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  private void createRegionWithDefaultDiskStore() {
    SERVER_LAUNCHER.get().getCache().createDiskStoreFactory()
        .create(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);
    SERVER_LAUNCHER.get().getCache()
        .createRegionFactory(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT_OVERFLOW)
        .setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(1).create())
        .setDiskStoreName(DiskStoreFactory.DEFAULT_DISK_STORE_NAME)
        .setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK))
        .create(OVERFLOW_REGION_NAME);
  }

  private void populateRegion() {
    client.invoke(() -> {
      if (CLIENT_CACHE.get() == null) {
        ClientCache clientCache =
            new ClientCacheFactory().addPoolLocator("localhost", locatorPort).create();
        CLIENT_CACHE.set(clientCache);
      }

      Region<Object, Object> clientRegion = CLIENT_CACHE.get()
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .create(OVERFLOW_REGION_NAME);

      IntStream.range(0, NUM_ENTRIES).forEach(i -> clientRegion.put("key-" + i, "value-" + i));
    });
  }

  private void assertRegionSize(int size) {
    server1.invoke(() -> {
      assertThat(SERVER_LAUNCHER.get().getCache().getRegion(OVERFLOW_REGION_NAME)).isNotNull();
      assertThat(SERVER_LAUNCHER.get().getCache().getRegion(OVERFLOW_REGION_NAME).size())
          .isEqualTo(size);
    });
    server2.invoke(() -> {
      assertThat(SERVER_LAUNCHER.get().getCache().getRegion(OVERFLOW_REGION_NAME)).isNotNull();
      assertThat(SERVER_LAUNCHER.get().getCache().getRegion(OVERFLOW_REGION_NAME).size())
          .isEqualTo(size);
    });
  }

  private void startLocator(File directory, int port, int jmxPort, int httpPort) {
    LOCATOR_LAUNCHER.set(new LocatorLauncher.Builder()
        .setMemberName(LOCATOR_NAME)
        .setPort(port)
        .setWorkingDirectory(directory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, httpPort + "")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(directory, LOCATOR_NAME + ".log").getAbsolutePath())
        .set(MAX_WAIT_TIME_RECONNECT, "1000")
        .set(MEMBER_TIMEOUT, "2000")
        .set(ENABLE_CLUSTER_CONFIGURATION, "true")
        .set(USE_CLUSTER_CONFIGURATION, "true")
        .build());

    LOCATOR_LAUNCHER.get().start();

    await().untilAsserted(() -> {
      InternalLocator locator = (InternalLocator) LOCATOR_LAUNCHER.get().getLocator();
      assertThat(locator.isSharedConfigurationRunning())
          .as("Locator shared configuration is running on locator" + getVMId())
          .isTrue();
    });
  }

  private void startServer(String name, File workingDirectory, String locator, int serverPort) {
    SERVER_LAUNCHER.set(new ServerLauncher.Builder()
        .setDeletePidFileOnStop(true)
        .setMemberName(name)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .setServerPort(serverPort)
        .set(HTTP_SERVICE_PORT, "0")
        .set(LOCATORS, locator)
        .set(LOG_FILE, new File(workingDirectory, name + ".log").getAbsolutePath())
        .set(MAX_WAIT_TIME_RECONNECT, "1000")
        .set(MEMBER_TIMEOUT, "2000")
        .set(ENABLE_CLUSTER_CONFIGURATION, "true")
        .set(USE_CLUSTER_CONFIGURATION, "true")
        .build());

    SERVER_LAUNCHER.get().start();
  }

  private static void stopServer() {
    SERVER_LAUNCHER.get().stop();
  }
}
