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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.PersistenceTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({DistributedTest.class, PersistenceTest.class})
public class ShowMissingDiskStoresDUnitTest {

  // private static final String DISK_STORE = "diskStore";
  private static final String DISK_STORE_DIR = "myDiskStores";
  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfshConnector = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    locator = lsRule.startLocatorVM(0);
    gfshConnector.connect(locator);
    assertThat(gfshConnector.isConnected()).isTrue();

    // start a server so that we can execute data commands that requires at least a server running
    server1 = lsRule.startServerVM(1, locator.getPort());
    server2 = lsRule.startServerVM(2, locator.getPort());
  }

  @Ignore("WIP: new test for GEODE-2681 fix")
  @Test
  public void missingDiskStores_gfshDoesntHang() throws Exception {
    final String testRegionName = "regionA";
    CommandStringBuilder csb;
    // TODO: Need to ensure that the diskstores are created in "user.dir" as set by the
    // *StarterRules, see DiskStoreFactoryImpl.setDiskDirsAndSizes
    csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE)
        .addOption(CliStrings.CREATE_DISK_STORE__NAME, "diskStore")
        .addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "diskStoreDir");
    gfshConnector.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    Awaitility.await().until(() -> {
      return new File(server1.getWorkingDir(), "diskStoreDir").exists()
          && new File(server2.getWorkingDir(), "diskStoreDir").exists();
    });

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION)
        .addOption(CliStrings.CREATE_REGION__REGION, testRegionName)
        .addOption(CliStrings.CREATE_REGION__DISKSTORE, "diskStore")
        .addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT,
            RegionShortcut.REPLICATE_PERSISTENT.toString());
    gfshConnector.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    // Add data to the region
    putUsingGfsh(gfshConnector, testRegionName, 1, "A");
    putUsingGfsh(gfshConnector, testRegionName, 2, "B");
    putUsingGfsh(gfshConnector, testRegionName, 3, "C");

    lsRule.stopMember(1);
    lsRule.stopMember(2);

    AsyncInvocation restart1 = restartServerAsync(server1);
    checkAsyncResults(restart1, gfshConnector, 5);

    AsyncInvocation restart2 = restartServerAsync(server2);
    checkAsyncResults(restart2, gfshConnector, 5);

    for (AsyncInvocation ai : new AsyncInvocation[] {restart1, restart2}) {
      if (ai.isAlive()) {
        restart1.cancel(true);
      }
    }
  }

  private AsyncInvocation restartServerAsync(MemberVM member) throws Exception {
    String memberWorkingDir = member.getWorkingDir().getAbsolutePath();
    int locatorPort = locator.getPort();
    AsyncInvocation restart = member.invokeAsync(() -> {
      ServerLauncher serverLauncher =
          new ServerLauncher.Builder().setWorkingDirectory(memberWorkingDir)
              .setMemberName("server-1").set(LOCATORS, "localhost[" + locatorPort + "]").build();
      serverLauncher.start();
    });

    return restart;
  }

  private void checkAsyncResults(AsyncInvocation ai, GfshCommandRule gfsh, int secsToWait)
      throws Exception {
    try {
      Awaitility.await().atLeast(secsToWait, TimeUnit.SECONDS).until(() -> ai.isDone());
    } catch (Exception e) {
      // e.printStackTrace();
    }

    CommandResult result;

    result = gfsh.executeCommand("list members");
    System.out.println(result);
    result = gfsh.executeCommand("show missing-disk-stores");
    System.out.println(result);
  }

  private void putUsingGfsh(GfshCommandRule gfsh, String regionName, int key, String val)
      throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.PUT)
        .addOption(CliStrings.PUT__KEY, Integer.toString(key)).addOption(CliStrings.PUT__VALUE, val)
        .addOption(CliStrings.PUT__REGIONNAME, regionName);
    gfsh.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();
  }
}
