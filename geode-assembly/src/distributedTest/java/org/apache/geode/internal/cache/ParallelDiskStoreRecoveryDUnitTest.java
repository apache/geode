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

import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__DIR;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__LOCATORS;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__NAME;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__SERVER_PORT;

import java.io.File;
import java.util.stream.IntStream;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

public class ParallelDiskStoreRecoveryDUnitTest {

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  private static MemberVM locator;

  private static String locatorConnectionString;
  public static final int NUM_ENTRIES = 1000;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // locator used to clean up members started during tests
    locator = cluster.startLocatorVM(0, 0);

    locatorConnectionString = "localhost[" + locator.getPort() + "]";

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testParallelDiskStoreRecovery() throws Exception {
    File dir1 = temporaryFolder.newFolder("server1");
    File dir2 = temporaryFolder.newFolder("server2");
    String serverName1 = "server1";
    String serverName2 = "server2";
    String diskStoreName1 = "disk1";
    String diskStoreName2 = "disk2";
    String regionName1 = "region1";
    String regionName2 = "region2";

    startServer(serverName1, dir1);

    startServer(serverName2, dir2);

    createDiskStore(diskStoreName1);

    createDiskStore(diskStoreName2);

    createRegion(regionName1, diskStoreName1);

    createRegion(regionName2, diskStoreName2);

    populateRegion(NUM_ENTRIES, regionName1, regionName2);

    assertRegionSize(regionName1);

    assertRegionSize(regionName2);

    assertDiskStore(serverName1, diskStoreName1, regionName1, regionName2);
    assertDiskStore(serverName1, diskStoreName2, regionName2, regionName1);
    assertDiskStore(serverName2, diskStoreName1, regionName1, regionName2);
    assertDiskStore(serverName2, diskStoreName2, regionName2, regionName1);

    gfsh.connectAndVerify(locator);
    gfsh.execute("shutdown");

    // restart the servers

    startServer(serverName1, dir1);

    startServer(serverName2, dir2);

    assertRegionSize(regionName1);

    assertRegionSize(regionName2);

    assertDiskStore(serverName1, diskStoreName1, regionName1, regionName2);
    assertDiskStore(serverName1, diskStoreName2, regionName2, regionName1);
    assertDiskStore(serverName2, diskStoreName1, regionName1, regionName2);
    assertDiskStore(serverName2, diskStoreName2, regionName2, regionName1);

  }

  private void assertDiskStore(String serverName, String diskStoreName, String expected,
      String unexpected) {
    String command;
    command = new CommandStringBuilder("describe disk-store")
        .addOption("name", diskStoreName)
        .addOption("member", serverName)
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess().containsOutput(expected)
        .doesNotContainOutput(unexpected);
  }

  private void assertRegionSize(String regionName) {
    String command;
    command = new CommandStringBuilder("describe region")
        .addOption("name", regionName)
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput(String.valueOf(NUM_ENTRIES));
  }

  private void populateRegion(int numEntries, String regionName1, String regionName2) {
    ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
    ClientCache clientCache =
        clientCacheFactory.addPoolLocator("localhost", locator.getPort()).create();

    Region<Object, Object> clientRegion1 = clientCache
        .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName1);
    Region<Object, Object> clientRegion2 = clientCache
        .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName2);

    IntStream.range(0, numEntries).forEach(i -> {
      clientRegion1.put("key-" + i, "value-" + i);
      clientRegion2.put("key-" + i, "value-" + i);
    });
  }

  private void createRegion(String regionName, String diskStoreName) {
    String command;
    command = new CommandStringBuilder("create region")
        .addOption("name", regionName)
        .addOption("type", "PARTITION_REDUNDANT_PERSISTENT")
        .addOption("disk-store", diskStoreName)
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  private void createDiskStore(String diskStoreName) {
    String command;
    command = new CommandStringBuilder("create disk-store")
        .addOption("name", diskStoreName)
        .addOption("dir", diskStoreName)
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  private void startServer(String serverName, File dir) {
    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, serverName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT,
            String.valueOf(AvailablePortHelper.getRandomAvailableTCPPort()))
        .addOption(START_SERVER__DIR, dir)
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

}
