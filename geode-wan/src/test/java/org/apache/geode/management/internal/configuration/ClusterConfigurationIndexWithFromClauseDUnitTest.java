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
package org.apache.geode.management.internal.configuration;


import static org.junit.Assert.assertTrue;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({DistributedTest.class, WanTest.class})
@RunWith(JUnitParamsRunner.class)
public class ClusterConfigurationIndexWithFromClauseDUnitTest {

  final String REGION_NAME = "region";
  final String INDEX_NAME = "index";

  protected RegionShortcut[] getRegionTypes() {
    return new RegionShortcut[] {RegionShortcut.PARTITION, RegionShortcut.PARTITION_PERSISTENT,
        RegionShortcut.PARTITION_REDUNDANT, RegionShortcut.PARTITION_REDUNDANT_PERSISTENT,
        RegionShortcut.REPLICATE, RegionShortcut.REPLICATE_PERSISTENT};

  }

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfshCommandRule = new GfshCommandRule();

  private MemberVM locator = null;

  @Before
  public void before() throws Exception {
    locator = lsRule.startLocatorVM(0);
  }

  @Test
  @Parameters(method = "getRegionTypes")
  public void indexCreatedWithEntrySetInFromClauseMustPersist(RegionShortcut regionShortcut)
      throws Exception {
    MemberVM vm1 = lsRule.startServerVM(1, locator.getPort());
    gfshCommandRule.connectAndVerify(locator);
    createRegionUsingGfsh(REGION_NAME, regionShortcut, null);
    createIndexUsingGfsh("\"" + REGION_NAME + ".entrySet() z\"", "z.key", INDEX_NAME);
    String serverName = vm1.getName();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_MEMBER);
    gfshCommandRule.executeAndAssertThat(csb.toString()).statusIsSuccess();
    lsRule.stopMember(1);
    lsRule.startServerVM(1, locator.getPort());
    verifyIndexRecreated(INDEX_NAME);
  }

  @Test
  @Parameters(method = "getRegionTypes")
  public void indexCreatedWithDefinedIndexWithEntrySetInFromClauseMustPersistInClusterConfig(
      RegionShortcut regionShortcut)
      throws Exception {
    IgnoredException.addIgnoredException("java.lang.IllegalStateException");
    MemberVM vm1 = lsRule.startServerVM(1, locator.getPort());
    gfshCommandRule.connectAndVerify(locator);
    createRegionUsingGfsh(REGION_NAME, regionShortcut, null);
    createIndexUsingGfsh("\"" + REGION_NAME + ".entrySet() z\"", "z.key", INDEX_NAME);
    String serverName = vm1.getName();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_DEFINED_INDEXES);
    gfshCommandRule.executeAndAssertThat(csb.toString()).statusIsSuccess();
    lsRule.stopMember(1);
    lsRule.startServerVM(1, locator.getPort());
    verifyIndexRecreated(INDEX_NAME);

  }

  @Test
  @Parameters(method = "getRegionTypes")
  public void indexCreatedWithDefinedIndexWithAliasInFromClauseMustPersistInClusterConfig(
      RegionShortcut regionShortcut)
      throws Exception {
    IgnoredException.addIgnoredException("java.lang.IllegalStateException");
    MemberVM vm1 = lsRule.startServerVM(1, locator.getPort());
    gfshCommandRule.connectAndVerify(locator);
    createRegionUsingGfsh(REGION_NAME, regionShortcut, null);
    createIndexUsingGfsh("\"" + REGION_NAME + " z\"", "z.ID", INDEX_NAME);
    String serverName = vm1.getName();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_DEFINED_INDEXES);
    gfshCommandRule.executeAndAssertThat(csb.toString()).statusIsSuccess();
    lsRule.stopMember(1);
    lsRule.startServerVM(1, locator.getPort());
    verifyIndexRecreated(INDEX_NAME);

  }

  private void verifyIndexRecreated(String indexName) throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_INDEX);
    gfshCommandRule.executeAndAssertThat(csb.toString()).statusIsSuccess();
    String resultAsString = gfshCommandRule.getGfshOutput();
    assertTrue(resultAsString.contains(indexName));
  }

  private void createIndexUsingGfsh(String regionName, String expression, String indexName)
      throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, expression);
    csb.addOption(CliStrings.CREATE_INDEX__REGION, regionName);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    gfshCommandRule.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private void defineIndexUsingGfsh(String regionName, String expression, String indexName)
      throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DEFINE_INDEX);
    csb.addOption(CliStrings.DEFINE_INDEX__EXPRESSION, expression);
    csb.addOption(CliStrings.DEFINE_INDEX__REGION, regionName);
    csb.addOption(CliStrings.DEFINE_INDEX_NAME, indexName);
    gfshCommandRule.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private void createRegionUsingGfsh(String regionName, RegionShortcut regionShortCut, String group)
      throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortCut.name());
    csb.addOptionWithValueCheck(CliStrings.GROUP, group);
    gfshCommandRule.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }
}
