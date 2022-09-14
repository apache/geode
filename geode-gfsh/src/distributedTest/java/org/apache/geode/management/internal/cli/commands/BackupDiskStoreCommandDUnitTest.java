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


import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.PersistenceTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({PersistenceTest.class})
public class BackupDiskStoreCommandDUnitTest {

  private MemberVM locator;

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
  }

  @Test
  public void backupDiskStoresOfOneDiskStore() {
    MemberVM server1 = lsRule.startServerVM(1, locator.getPort());
    @SuppressWarnings("unused")
    MemberVM server2 = lsRule.startServerVM(2, locator.getPort());
    @SuppressWarnings("unused")

    final String testRegionName = "regionA";
    CommandStringBuilder csb;
    csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE)
        .addOption(CliStrings.CREATE_DISK_STORE__NAME, "diskStore")
        .addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "diskStoreDir");
    gfshConnector.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    CommandStringBuilder createRegion = new CommandStringBuilder(CliStrings.CREATE_REGION)
        .addOption(CliStrings.CREATE_REGION__REGION, testRegionName)
        .addOption(CliStrings.CREATE_REGION__DISKSTORE, "diskStore")
        .addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT,
            RegionShortcut.PARTITION_PERSISTENT.toString());
    await().untilAsserted(() -> gfshConnector.executeAndAssertThat(createRegion.getCommandString())
        .statusIsSuccess());

    // Add data to the region
    addData(server1, testRegionName);

    csb = new CommandStringBuilder(CliStrings.BACKUP_DISK_STORE)
        .addOption(CliStrings.BACKUP_DISK_STORE__DISKDIRS, "backupDir");

    @SuppressWarnings("deprecation")
    ResultModel result =
        gfshConnector.executeCommand(csb.getCommandString()).getResultData();
    TabularResultModel tableSection = result.getTableSection("backed-up-diskstores");
    List<String> list = tableSection.getValuesInColumn("UUID");
    assertThat(list).hasSize(3);
  }

  @Test
  public void backupDiskStoresTwoDiskStores() {
    MemberVM server1 = lsRule.startServerVM(1, locator.getPort());
    @SuppressWarnings("unused")
    MemberVM server2 = lsRule.startServerVM(2, locator.getPort());
    @SuppressWarnings("unused")

    final String testRegionName = "regionA";
    final String testRegionName2 = "regionB";

    CommandStringBuilder csb;
    csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE)
        .addOption(CliStrings.CREATE_DISK_STORE__NAME, "diskStore")
        .addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "diskStoreDir");
    gfshConnector.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    CommandStringBuilder createRegion = new CommandStringBuilder(CliStrings.CREATE_REGION)
        .addOption(CliStrings.CREATE_REGION__REGION, testRegionName)
        .addOption(CliStrings.CREATE_REGION__DISKSTORE, "diskStore")
        .addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT,
            RegionShortcut.PARTITION_PERSISTENT.toString());
    await().untilAsserted(() -> gfshConnector.executeAndAssertThat(createRegion.getCommandString())
        .statusIsSuccess());


    csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE)
        .addOption(CliStrings.CREATE_DISK_STORE__NAME, "diskStore2")
        .addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "diskStoreDir2");
    gfshConnector.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    CommandStringBuilder createRegion2 = new CommandStringBuilder(CliStrings.CREATE_REGION)
        .addOption(CliStrings.CREATE_REGION__REGION, testRegionName2)
        .addOption(CliStrings.CREATE_REGION__DISKSTORE, "diskStore2")
        .addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT,
            RegionShortcut.PARTITION_PERSISTENT.toString());
    await().untilAsserted(() -> gfshConnector.executeAndAssertThat(createRegion2.getCommandString())
        .statusIsSuccess());

    // Add data to the region
    addData(server1, testRegionName);
    addData(server2, testRegionName2);

    csb = new CommandStringBuilder(CliStrings.BACKUP_DISK_STORE)
        .addOption(CliStrings.BACKUP_DISK_STORE__DISKDIRS, "backupDir");

    @SuppressWarnings("deprecation")
    ResultModel result =
        gfshConnector.executeCommand(csb.getCommandString()).getResultData();
    TabularResultModel tableSection = result.getTableSection("backed-up-diskstores");
    List<String> list = tableSection.getValuesInColumn("UUID");
    assertThat(list).hasSize(5);
  }

  @Test
  public void backupDiskStoresTwoDiskStoresIncludeJustOne() {
    MemberVM server1 = lsRule.startServerVM(1, locator.getPort());
    @SuppressWarnings("unused")
    MemberVM server2 = lsRule.startServerVM(2, locator.getPort());
    @SuppressWarnings("unused")

    final String testRegionName = "regionA";
    final String testRegionName2 = "regionB";

    CommandStringBuilder csb;
    csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE)
        .addOption(CliStrings.CREATE_DISK_STORE__NAME, "diskStore")
        .addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "diskStoreDir");
    gfshConnector.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    CommandStringBuilder createRegion = new CommandStringBuilder(CliStrings.CREATE_REGION)
        .addOption(CliStrings.CREATE_REGION__REGION, testRegionName)
        .addOption(CliStrings.CREATE_REGION__DISKSTORE, "diskStore")
        .addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT,
            RegionShortcut.PARTITION_PERSISTENT.toString());
    await().untilAsserted(() -> gfshConnector.executeAndAssertThat(createRegion.getCommandString())
        .statusIsSuccess());


    csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE)
        .addOption(CliStrings.CREATE_DISK_STORE__NAME, "diskStore2")
        .addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "diskStoreDir2");
    gfshConnector.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    CommandStringBuilder createRegion2 = new CommandStringBuilder(CliStrings.CREATE_REGION)
        .addOption(CliStrings.CREATE_REGION__REGION, testRegionName2)
        .addOption(CliStrings.CREATE_REGION__DISKSTORE, "diskStore2")
        .addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT,
            RegionShortcut.PARTITION_PERSISTENT.toString());
    await().untilAsserted(() -> gfshConnector.executeAndAssertThat(createRegion2.getCommandString())
        .statusIsSuccess());

    // Add data to the region
    addData(server1, testRegionName);
    addData(server2, testRegionName2);

    csb = new CommandStringBuilder(CliStrings.BACKUP_DISK_STORE)
        .addOption(CliStrings.BACKUP_DISK_STORE__DISKDIRS, "backupDir")
        .addOption(CliStrings.BACKUP_INCLUDE_DISK_STORES, "diskStore");

    @SuppressWarnings("deprecation")
    ResultModel result =
        gfshConnector.executeCommand(csb.getCommandString()).getResultData();
    TabularResultModel tableSection = result.getTableSection("backed-up-diskstores");
    List<String> list = tableSection.getValuesInColumn("UUID");
    assertThat(list).hasSize(3);
  }

  @Test
  public void backupDiskStoresInvalidIncludeDiskStores() {
    MemberVM server1 = lsRule.startServerVM(1, locator.getPort());
    @SuppressWarnings("unused")
    MemberVM server2 = lsRule.startServerVM(2, locator.getPort());
    @SuppressWarnings("unused")

    final String testRegionName = "regionA";
    final String testRegionName2 = "regionB";

    CommandStringBuilder csb;
    csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE)
        .addOption(CliStrings.CREATE_DISK_STORE__NAME, "diskStore")
        .addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "diskStoreDir");
    gfshConnector.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    CommandStringBuilder createRegion = new CommandStringBuilder(CliStrings.CREATE_REGION)
        .addOption(CliStrings.CREATE_REGION__REGION, testRegionName)
        .addOption(CliStrings.CREATE_REGION__DISKSTORE, "diskStore")
        .addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT,
            RegionShortcut.PARTITION_PERSISTENT.toString());
    await().untilAsserted(() -> gfshConnector.executeAndAssertThat(createRegion.getCommandString())
        .statusIsSuccess());


    csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE)
        .addOption(CliStrings.CREATE_DISK_STORE__NAME, "diskStore2")
        .addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "diskStoreDir2");
    gfshConnector.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    CommandStringBuilder createRegion2 = new CommandStringBuilder(CliStrings.CREATE_REGION)
        .addOption(CliStrings.CREATE_REGION__REGION, testRegionName2)
        .addOption(CliStrings.CREATE_REGION__DISKSTORE, "diskStore2")
        .addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT,
            RegionShortcut.PARTITION_PERSISTENT.toString());
    await().untilAsserted(() -> gfshConnector.executeAndAssertThat(createRegion2.getCommandString())
        .statusIsSuccess());

    // Add data to the region
    addData(server1, testRegionName);
    addData(server2, testRegionName2);

    csb = new CommandStringBuilder(CliStrings.BACKUP_DISK_STORE)
        .addOption(CliStrings.BACKUP_DISK_STORE__DISKDIRS, "backupDir")
        .addOption(CliStrings.BACKUP_INCLUDE_DISK_STORES, "diskStore3");

    gfshConnector.executeAndAssertThat(csb.getCommandString()).statusIsError()
        .containsOutput("Specify valid include-disk-stores.");

    csb = new CommandStringBuilder(CliStrings.BACKUP_DISK_STORE)
        .addOption(CliStrings.BACKUP_DISK_STORE__DISKDIRS, "backupDir")
        .addOption(CliStrings.BACKUP_INCLUDE_DISK_STORES, "diskStore,diskStore4");

    gfshConnector.executeAndAssertThat(csb.getCommandString()).statusIsError()
        .containsOutput("Specify valid include-disk-stores.");
  }

  private void addData(MemberVM server1, String testRegionName) {
    server1.invoke(() -> {
      Region<Object, Object> region = CacheFactory.getAnyInstance().getRegion(testRegionName);
      for (int i = 0; i < 113; i++) {
        region.put(i, "A");
      }
    });
  }
}
