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


import java.util.concurrent.Future;
import java.util.stream.IntStream;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class ParallelDiskStoreRecoveryDUnitTest {

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  private MemberVM locator, server1, server2;

  private final int NUM_ENTRIES = 1000;

  @Test
  public void testParallelDiskStoreRecovery() throws Exception {

    String diskStoreName1 = "disk1";
    String diskStoreName2 = "disk2";
    String regionName1 = "region1";
    String regionName2 = "region2";

    locator = cluster.startLocatorVM(0, 0);
    gfsh.connectAndVerify(locator);
    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());

    createDiskStore(diskStoreName1);

    createDiskStore(diskStoreName2);

    createRegion(regionName1, diskStoreName1);

    createRegion(regionName2, diskStoreName2);

    populateRegion(NUM_ENTRIES, regionName1, regionName2);

    AssertRegionSizeAndDiskStore(diskStoreName1, diskStoreName2, regionName1, regionName2);

    server1.stop(false);
    server2.stop(false);

    // wait for the cluster to shutdown, then restart the servers
    Thread.sleep(10000);

    Future result1 = executorServiceRule.submit(() -> {
      server1 = cluster.startServerVM(1, locator.getPort());
    });

    Future result2 = executorServiceRule.submit(() -> {
      server2 = cluster.startServerVM(2, locator.getPort());
    });

    // wait for the servers to restart
    result1.get();

    result2.get();

    AssertRegionSizeAndDiskStore(diskStoreName1, diskStoreName2, regionName1, regionName2);

    gfsh.connectAndVerify(locator);
    gfsh.execute("shutdown --include-locators");

  }

  private void AssertRegionSizeAndDiskStore(String diskStoreName1, String diskStoreName2,
      String regionName1, String regionName2) {
    assertRegionSize(regionName1);

    assertRegionSize(regionName2);

    assertDiskStore(server1.getName(), diskStoreName1, regionName1, regionName2);
    assertDiskStore(server1.getName(), diskStoreName2, regionName2, regionName1);
    assertDiskStore(server2.getName(), diskStoreName1, regionName1, regionName2);
    assertDiskStore(server2.getName(), diskStoreName2, regionName2, regionName1);
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
        .addOption("max-oplog-size", "1")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

}
