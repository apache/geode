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
package org.apache.geode.cache.lucene.internal.configuration;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.cli.LuceneCliStrings;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexCommands;
import org.apache.geode.management.internal.cli.CommandManager;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.Locator;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.Member;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.lucene.analysis.Analyzer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;


@Category(DistributedTest.class)
public class LuceneClusterConfigurationDUnitTest {

  private String groupName = "Lucene";

  @Rule
  public LocatorServerStartupRule ls = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfshConnector = new GfshShellConnectionRule();

  private Locator locator = null;

  @Before
  public void before() throws Exception {
    locator = ls.startLocatorVM(0);
  }

  @Test
  public void indexGetsCreatedUsingClusterConfiguration() throws Exception {
    Member vm1 = startNodeUsingClusterConfiguration(1, false);

    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    // Create lucene index.
    createLuceneIndexUsingGfsh(false);

    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);

    // Start vm2. This should have lucene index created using cluster
    // configuration.
    Member vm2 = startNodeUsingClusterConfiguration(2, false);
    vm2.invoke(() -> {
      LuceneService luceneService =
          LuceneServiceProvider.get(LocatorServerStartupRule.serverStarter.cache);
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
      validateIndexFields(new String[] {"field1", "field2", "field3"}, index);
    });
  }


  @Test
  public void indexWithAnalyzerGetsCreatedUsingClusterConfiguration() throws Exception {
    startNodeUsingClusterConfiguration(1, false);

    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    // Create lucene index.
    // createLuceneIndexUsingGfsh(false);
    createLuceneIndexWithAnalyzerUsingGfsh(false);

    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);

    // Start vm2. This should have lucene index created using cluster
    // configuration.
    Member vm2 = startNodeUsingClusterConfiguration(2, false);
    vm2.invoke(() -> {
      LuceneService luceneService =
          LuceneServiceProvider.get(LocatorServerStartupRule.serverStarter.cache);
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
      String[] fields = new String[] {"field1", "field2", "field3"};
      validateIndexFields(fields, index);
      // Add this check back when we complete xml generation for analyzer.
      validateIndexFieldAnalyzer(fields,
          new String[] {"org.apache.lucene.analysis.standard.StandardAnalyzer",
              "org.apache.lucene.analysis.standard.StandardAnalyzer",
              "org.apache.lucene.analysis.standard.StandardAnalyzer"},
          index);
    });
  }

  @Test
  public void indexGetsCreatedOnGroupOfNodes() throws Exception {

    // Start vm1, vm2 in group
    Member vm1 = startNodeUsingClusterConfiguration(1, true);
    Member vm2 = startNodeUsingClusterConfiguration(2, true);

    // Start vm3 outside the group. The Lucene index should not be present here.
    Member vm3 = startNodeUsingClusterConfiguration(3, true);

    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    // Create lucene index on group.
    createLuceneIndexUsingGfsh(true);

    // Create region.
    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, groupName);

    // VM2 should have lucene index created using gfsh execution.
    vm2.invoke(() -> {
      LuceneService luceneService =
          LuceneServiceProvider.get(LocatorServerStartupRule.serverStarter.cache);
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
      validateIndexFields(new String[] {"field1", "field2", "field3"}, index);
    });

    // The Lucene index is present in vm3.
    vm3.invoke(() -> {
      LuceneService luceneService =
          LuceneServiceProvider.get(LocatorServerStartupRule.serverStarter.cache);
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
    });
  }

  @Test
  public void indexNotCreatedOnNodeOutSideTheGroup() throws Exception {
    // Start vm1, vm2 in group
    Member vm1 = startNodeUsingClusterConfiguration(1, true);
    Member vm2 = startNodeUsingClusterConfiguration(2, true);

    // Start vm3 outside the group. The Lucene index should not be present here.
    Member vm3 = startNodeUsingClusterConfiguration(3, false);

    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    // Create lucene index on group.
    createLuceneIndexUsingGfsh(true);

    // Create region.
    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, groupName);

    // VM2 should have lucene index created using gfsh execution
    vm2.invoke(() -> {
      LuceneService luceneService =
          LuceneServiceProvider.get(LocatorServerStartupRule.serverStarter.cache);
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
      validateIndexFields(new String[] {"field1", "field2", "field3"}, index);
    });

    // The Lucene index should not be present in vm3.
    vm3.invoke(() -> {
      LuceneService luceneService =
          LuceneServiceProvider.get(LocatorServerStartupRule.serverStarter.cache);
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNull(index);
    });
  }

  @Test
  public void indexAreCreatedInValidGroupOfNodesJoiningLater() throws Exception {
    // Start vm1 in group
    startNodeUsingClusterConfiguration(1, true);
    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    // Create lucene index on group.
    createLuceneIndexUsingGfsh(true);

    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, groupName);

    // Start vm2 in group
    Member vm2 = startNodeUsingClusterConfiguration(2, true);

    // Start vm3 outside the group. The Lucene index should not be present here.
    Member vm3 = startNodeUsingClusterConfiguration(3, false);

    // VM2 should have lucene index created using gfsh execution
    vm2.invoke(() -> {
      LuceneService luceneService =
          LuceneServiceProvider.get(LocatorServerStartupRule.serverStarter.cache);
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
      validateIndexFields(new String[] {"field1", "field2", "field3"}, index);
    });

    // The Lucene index should not be present in vm3.
    vm3.invoke(() -> {
      LuceneService luceneService =
          LuceneServiceProvider.get(LocatorServerStartupRule.serverStarter.cache);
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNull(index);
    });
  }


  private Member startNodeUsingClusterConfiguration(int vmIndex, boolean addGroup)
      throws Exception {
    Properties nodeProperties = new Properties();
    if (addGroup) {
      nodeProperties.setProperty(GROUPS, groupName);
    }
    return ls.startServerVM(vmIndex, nodeProperties, ls.getMember(0).getPort());
  }

  private void createLuceneIndexUsingGfsh(boolean addGroup) throws Exception {
    // Execute Gfsh command to create lucene index.
    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    if (addGroup) {
      csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__GROUP, groupName);
    }
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    gfshConnector.executeAndVerifyCommand(csb.toString());
  }

  private void createLuceneIndexWithAnalyzerUsingGfsh(boolean addGroup) throws Exception {
    // Gfsh command to create lucene index.
    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER,
        "org.apache.lucene.analysis.standard.StandardAnalyzer,"
            + "org.apache.lucene.analysis.standard.StandardAnalyzer,"
            + "org.apache.lucene.analysis.standard.StandardAnalyzer");

    if (addGroup) {
      csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__GROUP, groupName);
    }
    // Execute Gfsh command.
    gfshConnector.executeAndVerifyCommand(csb.toString());
  }

  private void createRegionUsingGfsh(String regionName, RegionShortcut regionShortCut, String group)
      throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortCut.name());
    csb.addOptionWithValueCheck(CliStrings.CREATE_REGION__GROUP, group);
    gfshConnector.executeAndVerifyCommand(csb.toString());
  }


  private static void validateIndexFields(String[] indexFields, LuceneIndex index) {
    String[] indexFieldNames = index.getFieldNames();
    Arrays.sort(indexFieldNames);
    assertArrayEquals(indexFields, indexFieldNames);
  }

  private static void validateIndexFieldAnalyzer(String[] fields, String[] analyzers,
      LuceneIndex index) {
    Map<String, Analyzer> indexfieldAnalyzers = index.getFieldAnalyzers();
    for (int i = 0; i < fields.length; i++) {
      Analyzer a = indexfieldAnalyzers.get(fields[i]);
      System.out.println("#### Analyzer name :" + a.getClass().getName());
      assertEquals(analyzers[i], a.getClass().getName());
    }
  }

}
