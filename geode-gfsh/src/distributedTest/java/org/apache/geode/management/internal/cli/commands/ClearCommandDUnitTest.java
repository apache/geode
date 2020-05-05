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

import static org.apache.geode.management.internal.cli.commands.RemoveCommand.REGION_NOT_FOUND;
import static org.apache.geode.management.internal.i18n.CliStrings.CLEAR_REGION_CLEARED_ALL_KEYS;
import static org.assertj.core.api.Assertions.assertThat;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;

@RunWith(JUnitParamsRunner.class)
public class ClearCommandDUnitTest {
  private static final String REPLICATE_REGION_NAME = "replicateRegion";
  private static final String PARTITIONED_REGION_NAME = "partitionedRegion";
  private static final String EMPTY_STRING = "";
  private static final int NUM_ENTRIES = 200;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;

  @Before
  public void setup() throws Exception {
    locator = clusterStartupRule.startLocatorVM(0);
    server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    server2 = clusterStartupRule.startServerVM(2, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + REPLICATE_REGION_NAME + " --type=REPLICATE")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + PARTITIONED_REGION_NAME + " --type=PARTITION").statusIsSuccess();

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + REPLICATE_REGION_NAME, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + PARTITIONED_REGION_NAME, 2);

    VMProvider.invokeInEveryMember(ClearCommandDUnitTest::populateTestRegions, server1, server2);
  }

  private static void populateTestRegions() {
    Cache cache = CacheFactory.getAnyInstance();

    Region<String, String> replicateRegion = cache.getRegion(REPLICATE_REGION_NAME);
    replicateRegion.put(EMPTY_STRING, "valueForEmptyKey");
    for (int i = 0; i < NUM_ENTRIES; i++) {
      replicateRegion.put("key" + i, "value" + i);
    }

    Region<String, String> partitionedRegion = cache.getRegion(PARTITIONED_REGION_NAME);
    replicateRegion.put(EMPTY_STRING, "valueForEmptyKey");
    for (int i = 0; i < NUM_ENTRIES; i++) {
      partitionedRegion.put("key" + i, "value" + i);
    }
  }

  @Test
  public void clearFailsWhenRegionIsNotFound() {
    String invalidRegionName = "NotAValidRegion";
    String command = new CommandStringBuilder(CliStrings.CLEAR_REGION)
        .addOption(CliStrings.CLEAR_REGION_REGION_NAME, invalidRegionName).getCommandString();
    gfsh.executeAndAssertThat(command).statusIsError()
        .containsOutput(String.format(REGION_NOT_FOUND, "/" + invalidRegionName));
  }

  @Test
  @Parameters({REPLICATE_REGION_NAME, PARTITIONED_REGION_NAME})
  public void clearSucceedsWithValidRegion(String regionName) {
    String command = new CommandStringBuilder(CliStrings.CLEAR_REGION)
        .addOption(CliStrings.CLEAR_REGION_REGION_NAME, regionName).getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess();

    assertThat(gfsh.getGfshOutput()).contains(CLEAR_REGION_CLEARED_ALL_KEYS);

    server1.invoke(() -> verifyAllKeysAreRemoved(regionName));
    server2.invoke(() -> verifyAllKeysAreRemoved(regionName));
  }

  private static void verifyAllKeysAreRemoved(String regionName) {
    Region region = getRegion(regionName);
    assertThat(region.size()).isEqualTo(0);
  }

  private static Region getRegion(String regionName) {
    return CacheFactory.getAnyInstance().getRegion(regionName);
  }
}
