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

import static org.apache.geode.management.internal.i18n.CliStrings.GROUP;
import static org.apache.geode.management.internal.i18n.CliStrings.LIST_REGION;
import static org.apache.geode.management.internal.i18n.CliStrings.MEMBER;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({RegionsTest.class})
public class ListRegionDUnitTest {
  private static final String REGION1 = "region1";
  private static final String REGION2 = "region2";
  private static final String REGION3 = "region3";
  private static final String SUBREGION1A = "subregion1A";
  private static final String SUBREGION1B = "subregion1B";
  private static final String SUBREGION1C = "subregion1C";
  private static final String PR1 = "PR1";
  private static final String LOCALREGIONONSERVER1 = "LocalRegionOnServer1";

  private static final String SERVER1_NAME = "Server-1";
  private static final String SERVER2_NAME = "Server-2";
  private static final String GROUP1_NAME = "G1";
  private static final String GROUP2_NAME = "G2";

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void setupSystem() throws Exception {
    MemberVM locator = lsRule.startLocatorVM(0);
    MemberVM server1 = lsRule.startServerVM(1, GROUP1_NAME, locator.getPort());
    MemberVM server = lsRule.startServerVM(2, GROUP2_NAME, locator.getPort());

    server1.invoke(() -> {
      final Cache cache = ClusterStartupRule.getCache();
      RegionFactory<String, Integer> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      dataRegionFactory.create(PR1);
      createLocalRegion(LOCALREGIONONSERVER1);
    });

    server.invoke(() -> {
      final Cache cache = ClusterStartupRule.getCache();
      RegionFactory<String, Integer> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      dataRegionFactory.create(PR1);
      createRegionsWithSubRegions();
    });

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void listAllRegions() {
    String listRegions = new CommandStringBuilder(LIST_REGION).toString();
    gfsh.executeAndAssertThat(listRegions).statusIsSuccess().containsOutput(PR1,
        LOCALREGIONONSERVER1, REGION1, REGION2, REGION3);
  }

  @Test
  public void listRegionsOnManager() {
    String listRegions =
        new CommandStringBuilder(LIST_REGION).addOption(MEMBER, SERVER1_NAME).toString();
    gfsh.executeAndAssertThat(listRegions).statusIsSuccess().containsOutput(PR1,
        LOCALREGIONONSERVER1);
  }

  @Test
  public void listRegionsOnServer() {
    CommandStringBuilder csb = new CommandStringBuilder(LIST_REGION);
    csb.addOption(MEMBER, SERVER2_NAME);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess().containsOutput(PR1, REGION1,
        REGION2, REGION3, SUBREGION1A);
  }

  @Test
  public void listRegionsInGroup1() {
    CommandStringBuilder csb = new CommandStringBuilder(LIST_REGION);
    csb.addOption(GROUP, GROUP1_NAME);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess().containsOutput(PR1,
        LOCALREGIONONSERVER1);
  }

  @Test
  public void listRegionsInGroup2() {
    CommandStringBuilder csb = new CommandStringBuilder(LIST_REGION);
    csb.addOption(GROUP, GROUP2_NAME);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess().containsOutput(PR1, REGION1,
        REGION2, REGION3, SUBREGION1A);
  }

  @Test
  public void listNoMembersIsSuccess() {
    CommandStringBuilder csb = new CommandStringBuilder(LIST_REGION);
    csb.addOption(GROUP, "unknown");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private static void createLocalRegion(final String regionName) {
    final Cache cache = CacheFactory.getAnyInstance();
    // Create the data region
    RegionFactory<String, Integer> dataRegionFactory =
        cache.createRegionFactory(RegionShortcut.LOCAL);
    dataRegionFactory.create(regionName);
  }

  @SuppressWarnings("deprecation")
  private static void createRegionsWithSubRegions() {
    final Cache cache = CacheFactory.getAnyInstance();

    RegionFactory<String, Integer> dataRegionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    dataRegionFactory.setConcurrencyLevel(3);
    Region<String, Integer> region1 = dataRegionFactory.create(REGION1);
    region1.createSubregion(SUBREGION1C, region1.getAttributes());
    Region<String, Integer> subregion2 =
        region1.createSubregion(SUBREGION1A, region1.getAttributes());

    subregion2.createSubregion(SUBREGION1B, subregion2.getAttributes());
    dataRegionFactory.create(REGION2);
    dataRegionFactory.create(REGION3);
  }
}
