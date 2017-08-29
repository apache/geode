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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.Stock;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.CleanupDUnitVMsRule;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class IndexCommandsDUnitTest {
  // This test seemed to be leaving behind state that caused ConnectCommandWithSSLTest to fail if it
  // was run afterwards. So, this rule ensures we leave behind a clean state.
  @ClassRule
  public static CleanupDUnitVMsRule cleanupDUnitVMsRule = new CleanupDUnitVMsRule();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Rule
  public LocatorServerStartupRule startupRule = new LocatorServerStartupRule();
  private static final String partitionedRegionName = "partitionedRegion";
  private static final String indexName = "index1";
  private static final String groupName = "group1";

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty("groups", groupName);
    MemberVM serverVM = startupRule.startServerAsJmxManager(0, props);
    serverVM.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      Region parReg =
          createPartitionedRegion(partitionedRegionName, cache, String.class, Stock.class);
      parReg.put("VMW", new Stock("VMW", 98));
      parReg.put("APPL", new Stock("APPL", 600));
    });
    connect(serverVM);
  }

  public void connect(MemberVM serverVM) throws Exception {
    gfsh.connectAndVerify(serverVM.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
  }

  @Test
  public void testCreateAndDestroyIndex() throws Exception {
    // Create an index
    CommandStringBuilder createStringBuilder = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__REGION, "/" + partitionedRegionName);
    gfsh.executeAndVerifyCommand(createStringBuilder.toString());

    assertTrue(indexIsListed());

    // Destroy the index
    CommandStringBuilder destroyStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    destroyStringBuilder.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    destroyStringBuilder.addOption(CliStrings.DESTROY_INDEX__REGION, "/" + partitionedRegionName);
    gfsh.executeAndVerifyCommand(destroyStringBuilder.toString());

    assertFalse(indexIsListed());
  }

  @Test
  public void testCreateIndexWithMultipleIterators() throws Exception {
    CommandStringBuilder createStringBuilder = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "\"h.low\"");
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__REGION,
        "\"/" + partitionedRegionName + " s, s.history h\"");

    gfsh.executeAndVerifyCommand(createStringBuilder.toString());

    assertTrue(indexIsListed());
  }

  @Test
  public void testListIndexValidField() throws Exception {
    CommandStringBuilder createStringBuilder = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "\"h.low\"");
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__REGION,
        "\"/" + partitionedRegionName + " s, s.history h\"");

    gfsh.executeAndVerifyCommand(createStringBuilder.toString());

    assertTrue(indexIsListedAsValid());
  }

  @Test
  public void testCannotCreateIndexWithExistingIndexName() throws Exception {
    createBaseIndexForTesting();

    // CREATE the same index
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/" + partitionedRegionName);
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "hash");

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void testCannotCreateIndexInIncorrectRegion() throws Exception {
    // Create index on a wrong regionPath
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/InvalidRegionName");
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "hash");

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }


  @Test
  public void testCannotCreateIndexWithInvalidIndexExpression() throws Exception {
    // Create index with wrong expression
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "InvalidExpressionOption");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/" + partitionedRegionName);
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "hash");

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void testCannotCreateIndexWithInvalidIndexType() throws Exception {
    // Create index with wrong type
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/" + partitionedRegionName);
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "InvalidIndexType");

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void testCannotDestroyIndexWithInvalidIndexName() throws Exception {
    createBaseIndexForTesting();

    // Destroy index with incorrect indexName
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__NAME, "IncorrectIndexName");
    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput()).contains(
        CliStrings.format(CliStrings.DESTROY_INDEX__INDEX__NOT__FOUND, "IncorrectIndexName"));
  }

  @Test
  public void testCannotDestroyIndexWithInvalidRegion() throws Exception {
    createBaseIndexForTesting();

    // Destroy index with incorrect region
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    csb.addOption(CliStrings.DESTROY_INDEX__REGION, "IncorrectRegion");
    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput()).contains(
        CliStrings.format(CliStrings.DESTROY_INDEX__REGION__NOT__FOUND, "IncorrectRegion"));
  }

  @Test
  public void testCannotDestroyIndexWithInvalidMember() throws Exception {
    createBaseIndexForTesting();

    // Destroy index with incorrect member name
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    csb.addOption(CliStrings.DESTROY_INDEX__REGION, "Region");
    csb.addOption(CliStrings.MEMBER, "InvalidMemberName");
    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void testCannotDestroyIndexWithNoOptions() throws Exception {
    createBaseIndexForTesting();

    // Destroy index with no option
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void testDestroyIndexViaRegion() throws Exception {
    createBaseIndexForTesting();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__REGION, partitionedRegionName);
    gfsh.executeAndVerifyCommand(csb.toString());

    assertFalse(indexIsListed());
  }

  @Test
  public void testDestroyIndexViaGroup() throws Exception {
    createBaseIndexForTesting();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.GROUP, groupName);
    gfsh.executeAndVerifyCommand(csb.toString());

    assertFalse(indexIsListed());

  }

  private void createBaseIndexForTesting() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/" + partitionedRegionName);
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "hash");
    gfsh.executeAndVerifyCommand(csb.toString());
    assertTrue(indexIsListed());
  }

  private boolean indexIsListed() throws Exception {
    gfsh.executeAndVerifyCommand(CliStrings.LIST_INDEX);
    return gfsh.getGfshOutput().contains(indexName);
  }

  private boolean indexIsListedAsValid() throws Exception {
    gfsh.executeAndVerifyCommand(CliStrings.LIST_INDEX);
    return gfsh.getGfshOutput().contains("true");
  }

  private static Region<?, ?> createPartitionedRegion(String regionName, Cache cache,
      Class keyConstraint, Class valueConstraint) {
    RegionFactory regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.PARTITION);
    regionFactory.setKeyConstraint(keyConstraint);
    regionFactory.setValueConstraint(valueConstraint);
    return regionFactory.create(regionName);
  }

}

