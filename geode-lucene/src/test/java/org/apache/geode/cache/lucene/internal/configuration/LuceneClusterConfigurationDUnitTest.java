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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.cli.LuceneCliStrings;
import org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.Member;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.lucene.analysis.Analyzer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;


@Category(DistributedTest.class)
public class LuceneClusterConfigurationDUnitTest {

  @Rule
  public LocatorServerStartupRule ls = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfshConnector = new GfshShellConnectionRule();

  private MemberVM locator = null;

  @Before
  public void before() throws Exception {
    locator = ls.startLocatorVM(0);
  }

  @Test
  public void indexGetsCreatedUsingClusterConfiguration() throws Exception {
    Member vm1 = startNodeUsingClusterConfiguration(1);

    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    // Create lucene index.
    createLuceneIndexUsingGfsh();

    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);

    // Start vm2. This should have lucene index created using cluster
    // configuration.
    MemberVM vm2 = startNodeUsingClusterConfiguration(2);
    vm2.invoke(() -> {
      LuceneService luceneService =
          LuceneServiceProvider.get(LocatorServerStartupRule.serverStarter.getCache());
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
      validateIndexFields(new String[] {"field1", "field2", "field3"}, index);
    });
  }


  @Test
  public void indexWithAnalyzerGetsCreatedUsingClusterConfiguration() throws Exception {
    startNodeUsingClusterConfiguration(1);

    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    // Create lucene index.
    // createLuceneIndexUsingGfsh();
    createLuceneIndexWithAnalyzerUsingGfsh(false);

    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);

    // Start vm2. This should have lucene index created using cluster
    // configuration.
    MemberVM vm2 = startNodeUsingClusterConfiguration(2);
    vm2.invoke(() -> {
      LuceneService luceneService =
          LuceneServiceProvider.get(LocatorServerStartupRule.serverStarter.getCache());
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
  public void verifyClusterConfigurationAfterDestroyIndex() throws Exception {
    Member vm1 = startNodeUsingClusterConfiguration(1);

    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    // Create and add indexes
    createAndAddIndexes();

    // Destroy one index
    destroyLuceneIndexUsingGfsh(INDEX_NAME + "0");

    // Destroy other index
    destroyLuceneIndexUsingGfsh(INDEX_NAME + "1");

    // Verify cluster configuration no longer contains any indexes
    locator.invoke(verifyClusterConfiguration(false));
  }

  @Test
  public void verifyClusterConfigurationAfterDestroyIndexes() throws Exception {
    Member vm1 = startNodeUsingClusterConfiguration(1);

    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    // Create and add indexes
    createAndAddIndexes();

    // Destroy all indexes
    destroyLuceneIndexUsingGfsh(null);

    // Verify cluster configuration no longer contains indexes
    locator.invoke(verifyClusterConfiguration(false));
  }

  @Test
  public void verifyMemberWithGroupStartsAfterAlterRegion() throws Exception {
    // Start a member with no group
    startNodeUsingClusterConfiguration(1);

    // Start a member with group
    String group = "group1";
    Properties properties = new Properties();
    properties.setProperty(GROUPS, group);
    MemberVM vm2 = startNodeUsingClusterConfiguration(2, properties);

    // Connect Gfsh to locator
    gfshConnector.connectAndVerify(locator);

    // Create index and region in no group
    createLuceneIndexUsingGfsh();
    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);

    // Alter region in group
    CommandResult alterRegionResult = alterRegionUsingGfsh(group);
    TabularResultData alterRegionResultData = (TabularResultData) alterRegionResult.getResultData();
    List<String> alterRegionResultDataStatus = alterRegionResultData.retrieveAllValues("Status");

    // Verify region is altered on only one server
    assertEquals(1, alterRegionResultDataStatus.size());
    assertEquals("Region \"/" + REGION_NAME + "\" altered on \"" + vm2.getName() + "\"",
        alterRegionResultDataStatus.get(0));

    // Start another member with group
    startNodeUsingClusterConfiguration(3, properties);

    // Verify all members have indexes
    CommandResult listIndexesResult = listIndexesUsingGfsh();
    TabularResultData listIndexesResultData = (TabularResultData) listIndexesResult.getResultData();
    List<String> listIndexesResultDataStatus = listIndexesResultData.retrieveAllValues("Status");
    assertEquals(3, listIndexesResultDataStatus.size());
    for (String status : listIndexesResultDataStatus) {
      assertEquals("Initialized", status);
    }
  }

  private void createAndAddIndexes() throws Exception {
    // Create lucene index.
    createLuceneIndexUsingGfsh(INDEX_NAME + "0");

    // Create another lucene index.
    createLuceneIndexUsingGfsh(INDEX_NAME + "1");

    // Create region
    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);

    // Verify cluster configuration contains the indexes
    locator.invoke(verifyClusterConfiguration(true));
  }

  private SerializableRunnableIF verifyClusterConfiguration(boolean verifyIndexesExist) {
    return () -> {
      InternalLocator internalLocator = LocatorServerStartupRule.locatorStarter.getLocator();
      ClusterConfigurationService sc = internalLocator.getSharedConfiguration();
      Configuration config = sc.getConfiguration(ClusterConfigurationService.CLUSTER_CONFIG);
      String xmlContent = config.getCacheXmlContent();
      String luceneIndex0Config = "<" + LuceneXmlConstants.PREFIX + ":" + LuceneXmlConstants.INDEX
          + " xmlns:lucene=\"" + LuceneXmlConstants.NAMESPACE + "\" " + LuceneXmlConstants.NAME
          + "=\"" + INDEX_NAME + "0" + "\">";
      String luceneIndex1Config = "<" + LuceneXmlConstants.PREFIX + ":" + LuceneXmlConstants.INDEX
          + " xmlns:lucene=\"" + LuceneXmlConstants.NAMESPACE + "\" " + LuceneXmlConstants.NAME
          + "=\"" + INDEX_NAME + "1" + "\">";
      if (verifyIndexesExist) {
        assertTrue(xmlContent.contains(luceneIndex0Config));
        assertTrue(xmlContent.contains(luceneIndex1Config));
      } else {
        assertFalse(xmlContent.contains(luceneIndex0Config));
        assertFalse(xmlContent.contains(luceneIndex1Config));
      }
    };
  }

  private MemberVM startNodeUsingClusterConfiguration(int vmIndex) throws Exception {
    return startNodeUsingClusterConfiguration(vmIndex, new Properties());
  }

  private MemberVM startNodeUsingClusterConfiguration(int vmIndex, Properties nodeProperties)
      throws Exception {
    return ls.startServerVM(vmIndex, nodeProperties, ls.getMember(0).getPort());
  }

  private void createLuceneIndexUsingGfsh() throws Exception {
    createLuceneIndexUsingGfsh(INDEX_NAME);
  }

  private void createLuceneIndexUsingGfsh(String indexName) throws Exception {
    // Execute Gfsh command to create lucene index.
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, indexName);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "'field1, field2, field3'");
    gfshConnector.executeAndVerifyCommand(csb.toString());
  }

  private void createLuceneIndexWithAnalyzerUsingGfsh(boolean addGroup) throws Exception {
    // Gfsh command to create lucene index.
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER,
        "org.apache.lucene.analysis.standard.StandardAnalyzer,"
            + "org.apache.lucene.analysis.standard.StandardAnalyzer,"
            + "org.apache.lucene.analysis.standard.StandardAnalyzer");

    // Execute Gfsh command.
    gfshConnector.executeAndVerifyCommand(csb.toString());
  }

  private void destroyLuceneIndexUsingGfsh(String indexName) throws Exception {
    // Execute Gfsh command to destroy lucene index.
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESTROY_INDEX);
    if (indexName != null) {
      csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, indexName);
    }
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    gfshConnector.executeAndVerifyCommand(csb.toString());
  }

  private void createRegionUsingGfsh(String regionName, RegionShortcut regionShortCut, String group)
      throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortCut.name());
    csb.addOptionWithValueCheck(CliStrings.GROUP, group);
    gfshConnector.executeAndVerifyCommand(csb.toString());
  }

  private CommandResult alterRegionUsingGfsh(String group) throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, REGION_NAME);
    csb.addOption(CliStrings.GROUP, group);
    csb.addOption(CliStrings.ALTER_REGION__EVICTIONMAX, "5764");
    return gfshConnector.executeAndVerifyCommand(csb.toString());
  }

  private CommandResult listIndexesUsingGfsh() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    return gfshConnector.executeAndVerifyCommand(csb.toString());
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
