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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@Category(DistributedTest.class)
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
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfshShellConnectionRule = new GfshShellConnectionRule();

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
    gfshShellConnectionRule.connectAndVerify(locator);
    createRegionUsingGfsh(REGION_NAME, regionShortcut, null);
    createIndexUsingGfsh("\"" + REGION_NAME + ".entrySet() z\"", "z.key", INDEX_NAME);
    String serverName = vm1.getName();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_MEMBER);
    gfshShellConnectionRule.executeAndVerifyCommand(csb.toString());
    lsRule.stopMember(1);
    lsRule.startServerVM(1, lsRule.getMember(0).getPort());
    verifyIndexRecreated(INDEX_NAME);
  }

  private void verifyIndexRecreated(String indexName) throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_INDEX);
    CommandResult commandResult = gfshShellConnectionRule.executeAndVerifyCommand(csb.toString());
    String resultAsString = commandResultToString(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertTrue(resultAsString.contains(indexName));
  }

  private String commandResultToString(final CommandResult commandResult) {
    assertNotNull(commandResult);
    commandResult.resetToFirstLine();
    StringBuilder buffer = new StringBuilder(commandResult.getHeader());
    while (commandResult.hasNextLine()) {
      buffer.append(commandResult.nextLine());
    }
    buffer.append(commandResult.getFooter());
    return buffer.toString();
  }

  private void createIndexUsingGfsh(String regionName, String expression, String indexName)
      throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, expression);
    csb.addOption(CliStrings.CREATE_INDEX__REGION, regionName);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    gfshShellConnectionRule.executeAndVerifyCommand(csb.toString());
  }

  private void createRegionUsingGfsh(String regionName, RegionShortcut regionShortCut, String group)
      throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortCut.name());
    csb.addOptionWithValueCheck(CliStrings.GROUP, group);
    gfshShellConnectionRule.executeAndVerifyCommand(csb.toString());
  }
}
