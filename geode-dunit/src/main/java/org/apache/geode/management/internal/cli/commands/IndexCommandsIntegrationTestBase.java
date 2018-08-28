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

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.query.Index;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.Stock;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.Server;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * this test class test: CreateIndexCommand, DestroyIndexCommand, ListIndexCommand
 */
@Category({GfshTest.class})
public class IndexCommandsIntegrationTestBase {
  private static final String regionName = "regionA";
  private static final String groupName = "groupA";
  private static final String indexName = "indexA";


  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withProperty(GROUPS, groupName).withJMXManager().withHttpService()
          .withAutoStart();

  @BeforeClass
  public static void beforeClass() throws Exception {
    InternalCache cache = server.getCache();
    Region region = createPartitionedRegion(regionName, cache, String.class, Stock.class);
    region.put("VMW", new Stock("VMW", 98));
    region.put("APPL", new Stock("APPL", 600));
  }

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    connect(server);
  }

  @After
  public void after() throws Exception {
    // destroy all existing indexes
    Collection<Index> indices = server.getCache().getQueryService().getIndexes();
    indices.stream().map(Index::getName).forEach(indexName -> {
      gfsh.executeAndAssertThat("destroy index --name=" + indexName).statusIsSuccess();
    });

    gfsh.executeAndAssertThat("list index").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("No Indexes Found");
  }

  public void connect(Server server) throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager);
  }

  @Test
  public void testCreate() throws Exception {
    createSimpleIndexA();
  }

  @Test
  public void testCreateIndexWithMultipleIterators() throws Exception {
    CommandStringBuilder createStringBuilder = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__NAME, "indexA");
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "\"h.low\"");
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__REGION,
        "\"/" + regionName + " s, s.history h\"");

    gfsh.executeAndAssertThat(createStringBuilder.toString()).statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("Index successfully created");

    gfsh.executeAndAssertThat("list index").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("indexA");
  }

  @Test
  public void testListIndexValidField() throws Exception {
    CommandStringBuilder createStringBuilder = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "\"h.low\"");
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__REGION,
        "\"/" + regionName + " s, s.history h\"");

    gfsh.executeAndAssertThat(createStringBuilder.toString()).statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("Index successfully created");

    gfsh.executeAndAssertThat("list index").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("indexA");
  }

  @Test
  public void testCannotCreateIndexWithExistingIndexName() throws Exception {
    createSimpleIndexA();

    // CREATE the same index
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/" + regionName);
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "hash");

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void creatIndexWithNoBeginningSlash() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, regionName);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("Index successfully created");

    gfsh.executeAndAssertThat("list index").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("indexA");
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
    assertThat(gfsh.getGfshOutput()).contains("Region not found : \"/InvalidRegionName\"");
  }

  @Test
  public void cannotCreateWithTheSameName() throws Exception {
    createSimpleIndexA();
    gfsh.executeAndAssertThat("create index --name=indexA --expression=key --region=/regionA")
        .statusIsError()
        .containsOutput("Index \"indexA\" already exists.  Create failed due to duplicate name");
  }

  @Test
  public void testCannotCreateIndexWithInvalidIndexExpression() throws Exception {
    // Create index with wrong expression
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "InvalidExpressionOption");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/" + regionName);
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "hash");

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void testCannotDestroyIndexWithInvalidIndexName() throws Exception {
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
    // Destroy index with incorrect region
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    csb.addOption(CliStrings.DESTROY_INDEX__REGION, "IncorrectRegion");
    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput()).contains("ERROR", "Region \"IncorrectRegion\" not found");
  }

  @Test
  public void testCannotDestroyIndexWithInvalidMember() throws Exception {
    // Destroy index with incorrect member name
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    csb.addOption(CliStrings.DESTROY_INDEX__REGION, "Region");
    csb.addOption(CliStrings.MEMBER, "InvalidMemberName");
    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput()).contains("No Members Found");
  }

  @Test
  public void testCannotDestroyIndexWithNoOptions() throws Exception {
    // Destroy index with no option
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput()).contains("requires that one or more parameters be provided.");
  }

  @Test
  public void testDestroyIndexViaRegion() throws Exception {
    createSimpleIndexA();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__REGION, regionName);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Destroyed all indexes on region regionA");
  }

  @Test
  public void testDestroyIndexViaGroup() throws Exception {
    createSimpleIndexA();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.GROUP, groupName);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Destroyed all indexes");
  }

  @Test
  public void testFailWhenDestroyIndexIsNotIdempotent() throws Exception {
    createSimpleIndexA();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    csb.addOption(CliStrings.IFEXISTS, "false");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Destroyed index " + indexName);
    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput("Index named \"" + indexName + "\" not found");
  }

  @Test
  public void testDestroyIndexOnRegionIsIdempotent() throws Exception {
    createSimpleIndexA();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__REGION, regionName);
    csb.addOption(CliStrings.IFEXISTS, "true");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Destroyed all indexes on region regionA");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Destroyed all indexes on region regionA");
  }

  @Test
  public void testDestroyIndexByNameIsIdempotent() throws Exception {
    createSimpleIndexA();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    csb.addOption(CliStrings.IFEXISTS);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Destroyed index " + indexName);
    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    Map<String, List<String>> table =
        result.getMapFromTableContent(ResultModel.MEMBER_STATUS_SECTION);
    assertThat(table.get("Status")).containsExactly("IGNORED");
    assertThat(table.get("Message")).containsExactly("Index named \"" + indexName + "\" not found");
  }

  private void createSimpleIndexA() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/" + regionName);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Index successfully created");
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
