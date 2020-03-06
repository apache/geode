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
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.PersistenceTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({PersistenceTest.class})
public class ShowMissingDiskStoreCommandDUnitTest {

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
  public void showMissingDiskStoresDoesNotDuplicateDiskStores() {
    MemberVM server1 = lsRule.startServerVM(1, locator.getPort());
    @SuppressWarnings("unused")
    MemberVM server2 = lsRule.startServerVM(2, locator.getPort());
    @SuppressWarnings("unused")
    MemberVM server3 = lsRule.startServerVM(3, locator.getPort());

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

    lsRule.stop(1);

    csb = new CommandStringBuilder(CliStrings.SHOW_MISSING_DISK_STORE);
    @SuppressWarnings("deprecation")
    ResultModel result =
        gfshConnector.executeCommand(csb.getCommandString()).getResultData();
    TabularResultModel tableSection = result.getTableSection("missing-disk-stores");
    List<String> missingDiskStoreIds = tableSection.getValuesInColumn("Disk Store ID");
    assertThat(missingDiskStoreIds).hasSize(1);

    String missingId = missingDiskStoreIds.iterator().next();

    csb = new CommandStringBuilder(CliStrings.REVOKE_MISSING_DISK_STORE)
        .addOption(CliStrings.REVOKE_MISSING_DISK_STORE__ID, missingId);
    gfshConnector.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();
  }

  @Test
  public void stoppingAndRestartingMemberDoesNotResultInMissingDiskStore() {
    MemberVM server1 = lsRule.startServerVM(1, locator.getPort());
    @SuppressWarnings("unused")
    MemberVM server2 = lsRule.startServerVM(2, locator.getPort());
    @SuppressWarnings("unused")
    MemberVM server3 = lsRule.startServerVM(3, locator.getPort());
    @SuppressWarnings("unused")
    MemberVM server4 = lsRule.startServerVM(4, locator.getPort());

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
            RegionShortcut.PARTITION_REDUNDANT_PERSISTENT.toString());
    await().untilAsserted(() -> gfshConnector.executeAndAssertThat(createRegion.getCommandString())
        .statusIsSuccess());


    // Add data to the region
    addData(server1, testRegionName);

    rebalance();

    lsRule.stop(1, false);

    rebalance();

    lsRule.stop(2, false);

    rebalance();

    lsRule.startServerVM(1, locator.getPort());

    lsRule.startServerVM(2, locator.getPort());

    csb = new CommandStringBuilder(CliStrings.SHOW_MISSING_DISK_STORE);
    @SuppressWarnings("deprecation")
    CommandResult commandResult = gfshConnector.executeCommand(csb.getCommandString());
    ResultModel result = commandResult.getResultData();
    TabularResultModel tableSection = result.getTableSection("missing-disk-stores");
    List<String> missingDiskStoreIds = tableSection.getValuesInColumn("Disk Store ID");
    assertThat(missingDiskStoreIds).isNull();
  }

  private void addData(MemberVM server1, String testRegionName) {
    server1.invoke(() -> {
      Region<Object, Object> region = CacheFactory.getAnyInstance().getRegion(testRegionName);
      for (int i = 0; i < 113; i++) {
        region.put(i, "A");
      }
    });
  }

  private void rebalance() {
    CommandStringBuilder rebalance = new CommandStringBuilder(CliStrings.REBALANCE);
    await().untilAsserted(
        () -> gfshConnector.executeAndAssertThat(rebalance.getCommandString()).statusIsSuccess());
  }
}
