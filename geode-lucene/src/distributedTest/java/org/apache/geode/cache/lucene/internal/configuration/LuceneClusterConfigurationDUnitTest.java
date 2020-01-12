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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.lucene.analysis.Analyzer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.cli.LuceneCliStrings;
import org.apache.geode.cache.lucene.internal.repository.serializer.PrimitiveSerializer;
import org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;


@Category({LuceneTest.class})
public class LuceneClusterConfigurationDUnitTest {

  @Rule
  public ClusterStartupRule ls = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfshConnector = new GfshCommandRule();

  private MemberVM locator = null;

  @Before
  public void before() throws Exception {
    locator = ls.startLocatorVM(0);
  }

  @Test
  public void indexGetsCreatedUsingClusterConfiguration() throws Exception {
    createServer(1);

    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    // Create lucene index.
    createLuceneIndexAndDataRegion();

    // Start vm2. This should have lucene index created using cluster
    // configuration.
    MemberVM vm2 = ls.startServerVM(2, locator.getPort());
    vm2.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(ClusterStartupRule.getCache());
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
      validateIndexFields(new String[] {"field1", "field2", "field3"}, index);
    });
  }

  void createLuceneIndexAndDataRegion() throws Exception {
    createLuceneIndexUsingGfsh();
    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);
  }

  MemberVM createServer(int index) throws IOException {
    return ls.startServerVM(index, locator.getPort());
  }


  @Test
  public void indexWithAnalyzerGetsCreatedUsingClusterConfiguration() throws Exception {
    createServer(1);
    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    createLuceneIndexWithAnalyzerAndDataRegion();

    // Start vm2. This should have lucene index created using cluster
    // configuration.
    MemberVM vm2 = createServer(2);
    vm2.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(ClusterStartupRule.getCache());
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

  void createLuceneIndexWithAnalyzerAndDataRegion() throws Exception {
    // Create lucene index.
    createLuceneIndexWithAnalyzerUsingGfsh();
    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);
  }

  @Test
  public void indexWithSerializerGetsCreatedUsingClusterConfiguration() throws Exception {
    createServer(1);

    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    // Create lucene index.
    createLuceneIndexWithSerializerAndDataRegion();

    // Start vm2. This should have lucene index created using cluster
    // configuration.
    MemberVM vm2 = ls.startServerVM(2, locator.getPort());
    vm2.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(ClusterStartupRule.getCache());
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
      String[] fields = new String[] {"field1", "field2", "field3"};
      validateIndexFields(fields, index);
      // Add this check back when we complete xml generation for analyzer.
      assertThat(index.getLuceneSerializer()).isInstanceOf(PrimitiveSerializer.class);
    });
  }

  void createLuceneIndexWithSerializerAndDataRegion() throws Exception {
    createLuceneIndexWithSerializerUsingGfsh();

    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);
  }

  @Test
  public void verifyClusterConfigurationAfterDestroyIndex() throws Exception {
    createServer(1);

    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    // Create and add indexes
    createAndAddIndexes();

    // Verify cluster configuration contains the indexes
    locator.invoke(verifyClusterConfiguration(true));

    // Destroy one index
    destroyLuceneIndexUsingGfsh(INDEX_NAME + "0");

    // Destroy other index
    destroyLuceneIndexUsingGfsh(INDEX_NAME + "1");

    // Verify cluster configuration no longer contains any indexes
    locator.invoke(verifyClusterConfiguration(false));
  }

  @Test
  public void verifyClusterConfigurationAfterDestroyIndexes() throws Exception {
    createServer(1);

    // Connect Gfsh to locator.
    gfshConnector.connectAndVerify(locator);

    // Create and add indexes
    createAndAddIndexes();

    // Verify cluster configuration contains the indexes
    locator.invoke(verifyClusterConfiguration(true));

    // Destroy all indexes
    destroyLuceneIndexUsingGfsh(null);

    // Verify cluster configuration no longer contains indexes
    locator.invoke(verifyClusterConfiguration(false));
  }

  @Test
  public void verifyMemberWithGroupStarts() throws Exception {
    // Start a member with no group
    createServer(1);

    // Start a member with group
    String group = "group1";
    Properties properties = new Properties();
    properties.setProperty(GROUPS, group);
    createServer(properties, 2);

    // Connect Gfsh to locator
    gfshConnector.connectAndVerify(locator);

    // Create index and region in no group
    createLuceneIndexAndDataRegion();

    // Start another member with group
    createServer(properties, 3);

    // Verify all members have indexes
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    gfshConnector.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .hasTableSection()
        .hasColumn("Status")
        .containsExactly("INITIALIZED", "INITIALIZED", "INITIALIZED");
  }

  MemberVM createServer(Properties properties, int index) throws Exception {
    return ls.startServerVM(index, properties, locator.getPort());
  }

  void createAndAddIndexes() throws Exception {
    // Create lucene index.
    createLuceneIndexUsingGfsh(INDEX_NAME + "0");

    // Create another lucene index.
    createLuceneIndexUsingGfsh(INDEX_NAME + "1");

    // Create region
    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);

  }

  SerializableRunnableIF verifyClusterConfiguration(boolean verifyIndexesExist) {
    return () -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      InternalConfigurationPersistenceService sc =
          internalLocator.getConfigurationPersistenceService();
      Configuration config = sc.getConfiguration(ConfigurationPersistenceService.CLUSTER_CONFIG);
      String xmlContent = config.getCacheXmlContent();
      String luceneIndex0Config = "<" + LuceneXmlConstants.PREFIX + ":" + LuceneXmlConstants.INDEX
          + " " + LuceneXmlConstants.NAME
          + "=\"" + INDEX_NAME + "0" + "\">";
      String luceneIndex1Config = "<" + LuceneXmlConstants.PREFIX + ":" + LuceneXmlConstants.INDEX
          + " " + LuceneXmlConstants.NAME
          + "=\"" + INDEX_NAME + "1" + "\">";
      if (verifyIndexesExist) {
        assertThat(xmlContent).contains(luceneIndex0Config);
        assertThat(xmlContent).contains(luceneIndex1Config);
      } else {
        assertThat(xmlContent).doesNotContain(luceneIndex0Config);
        assertThat(xmlContent).doesNotContain(luceneIndex1Config);
      }
    };
  }


  void createLuceneIndexUsingGfsh() throws Exception {
    createLuceneIndexUsingGfsh(INDEX_NAME);
  }

  void createLuceneIndexUsingGfsh(String indexName) throws Exception {
    // Execute Gfsh command to create lucene index.
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, indexName);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "'field1, field2, field3'");
    gfshConnector.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  void createLuceneIndexWithAnalyzerUsingGfsh() throws Exception {
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
    gfshConnector.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  void createLuceneIndexWithSerializerUsingGfsh() throws Exception {
    // Gfsh command to create lucene index.
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER,
        PrimitiveSerializer.class.getCanonicalName());

    // Execute Gfsh command.
    gfshConnector.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private void destroyLuceneIndexUsingGfsh(String indexName) throws Exception {
    // Execute Gfsh command to destroy lucene index.
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESTROY_INDEX);
    if (indexName != null) {
      csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, indexName);
    }
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    gfshConnector.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  void createRegionUsingGfsh(String regionName, RegionShortcut regionShortCut, String group)
      throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortCut.name());
    csb.addOptionWithValueCheck(CliStrings.GROUP, group);
    gfshConnector.executeAndAssertThat(csb.toString()).statusIsSuccess();
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
