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
package org.apache.geode.cache.lucene.internal.cli;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.LuceneIndexCreationProfile;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.repository.serializer.PrimitiveSerializer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(LuceneTest.class)
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class LuceneIndexCommandsIntegrationTest {

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public ServerStarterRule server = new ServerStarterRule();

  @Before
  public void before() throws Exception {
    server.withProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.cache.lucene.internal.cli.LuceneIndexCommandsWithReindexAllowedIntegrationTest"
            + ";org.apache.geode.cache.lucene.internal.cli.LuceneIndexCommandsIntegrationTest*"
            + ";org.apache.geode.test.**")
        .withJMXManager().startServer();

    gfsh.connectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager);
  }

  @Test
  public void listIndexShouldReturnExistingIndexWithStats() {
    createIndex();

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_LIST_INDEX__STATS, "true");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Documents", "0")
        .tableHasColumnWithExactValuesInAnyOrder("Index Name", "index");
  }

  @Test
  public void listIndexShouldReturnExistingIndexWithoutStats() {
    createIndex();
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess().containsOutput(INDEX_NAME)
        .doesNotContainOutput("Documents");
  }

  @Test
  public void listIndexWhenNoExistingIndexShouldReturnNoIndex() {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    gfsh.executeAndAssertThat(csb.toString()).containsOutput("No lucene indexes found");
  }

  @Test
  public void listIndexShouldReturnCorrectStatus() {
    createIndexWithoutRegion();

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_LIST_INDEX__STATS, "true");

    gfsh.executeAndAssertThat(csb.toString())
        .tableHasColumnWithExactValuesInAnyOrder("Status", "NOT_INITIALIZED")
        .tableHasColumnWithExactValuesInAnyOrder("Index Name", INDEX_NAME);
  }

  @Test
  public void listIndexWithStatsShouldReturnCorrectStats() throws Exception {
    createIndex();
    Map<String, TestObject> entries = new HashMap<>();
    entries.put("A", new TestObject("field1:value1", "field2:value2", "field3:value3"));
    entries.put("B", new TestObject("ABC", "EFG", "HIJ"));

    putEntries(entries, 2);
    queryAndVerify("field1:value1", "field1");

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_LIST_INDEX__STATS, "true");

    CommandResult result = gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .tableHasColumnOnlyWithValues("Index Name", INDEX_NAME)
        .tableHasColumnOnlyWithValues("Status", "INITIALIZED")
        .tableHasColumnOnlyWithValues("Region Path", "/region")
        .tableHasColumnOnlyWithValues("Query Executions", "1")
        .getCommandResult();

    // the document count could be greater than 2
    List<String> documents = result.getTableColumnValues("Documents");
    assertThat(documents).hasSize(1);
    assertThat(Integer.parseInt(documents.get(0))).isGreaterThanOrEqualTo(2);
  }


  @Test
  public void createIndexShouldCreateANewIndex() {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    createRegion();

    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    assertThat(index.getFieldNames()).isEqualTo(new String[] {"field1", "field2", "field3"});
  }

  @Test
  public void createIndexWithAnalyzersShouldCreateANewIndex() {
    List<String> analyzerNames = new ArrayList<>();
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());
    analyzerNames.add(KeywordAnalyzer.class.getCanonicalName());
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, String.join(",", analyzerNames));

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    createRegion();

    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    final Map<String, Analyzer> fieldAnalyzers = index.getFieldAnalyzers();
    assertThat(fieldAnalyzers.get("field1").getClass()).isEqualTo(StandardAnalyzer.class);
    assertThat(fieldAnalyzers.get("field2").getClass()).isEqualTo(KeywordAnalyzer.class);
    assertThat(fieldAnalyzers.get("field3").getClass()).isEqualTo(StandardAnalyzer.class);

  }

  @Test
  public void createIndexWithALuceneSerializerShouldCreateANewIndex() {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER,
        PrimitiveSerializer.class.getCanonicalName());

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    createRegion();

    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    assertThat(index.getLuceneSerializer()).isInstanceOf(PrimitiveSerializer.class);

  }

  @Test
  public void createIndexShouldNotAcceptBadIndexOrRegionNames() {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, "\'__\'");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess().containsOutput(
        "Region names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens, underscores, or forward slashes:");

    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, "\' @@@*%\'");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess().containsOutput(
        "Region names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens, underscores, or forward slashes:");

    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "\'__\'");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess().containsOutput(
        "Index names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens or underscores:");

    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "\' @@@*%\'");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess().containsOutput(
        "Index names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens or underscores:");
  }

  @Test
  public void createIndexShouldTrimAnalyzerNames() {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER,
        "\"org.apache.lucene.analysis.standard.StandardAnalyzer, org.apache.lucene.analysis.core.KeywordAnalyzer, org.apache.lucene.analysis.standard.StandardAnalyzer\"");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    createRegion();

    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    final Map<String, Analyzer> fieldAnalyzers = index.getFieldAnalyzers();
    assertThat(fieldAnalyzers.get("field1").getClass()).isEqualTo(StandardAnalyzer.class);
    assertThat(fieldAnalyzers.get("field2").getClass()).isEqualTo(KeywordAnalyzer.class);
    assertThat(fieldAnalyzers.get("field3").getClass()).isEqualTo(StandardAnalyzer.class);
  }

  @Test
  public void createIndexWithoutRegionShouldReturnCorrectResults() {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    LuceneServiceImpl luceneService =
        (LuceneServiceImpl) LuceneServiceProvider.get(server.getCache());
    final ArrayList<LuceneIndexCreationProfile> profiles =
        new ArrayList<>(luceneService.getAllDefinedIndexes());
    assertThat(profiles.size()).isEqualTo(1);
    assertThat(profiles.get(0).getIndexName()).isEqualTo(INDEX_NAME);
  }

  @Test
  public void createIndexWithWhitespaceOrDefaultKeywordAnalyzerShouldUseStandardAnalyzer() {
    // Test whitespace analyzer name
    String analyzerList = StandardAnalyzer.class.getCanonicalName() + ",     ,"
        + KeywordAnalyzer.class.getCanonicalName();
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "space");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, "'" + analyzerList + "'");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    // Test empty analyzer name
    analyzerList =
        StandardAnalyzer.class.getCanonicalName() + ",," + KeywordAnalyzer.class.getCanonicalName();
    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "empty");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, analyzerList);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    // Test keyword analyzer name
    analyzerList = StandardAnalyzer.class.getCanonicalName() + ",DEFAULT,"
        + KeywordAnalyzer.class.getCanonicalName();
    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "keyword");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, analyzerList);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    createRegion();
    final LuceneIndex spaceIndex = luceneService.getIndex("space", REGION_NAME);
    final Map<String, Analyzer> spaceFieldAnalyzers = spaceIndex.getFieldAnalyzers();

    final LuceneIndex emptyIndex = luceneService.getIndex("empty", REGION_NAME);
    final Map<String, Analyzer> emptyFieldAnalyzers2 = emptyIndex.getFieldAnalyzers();

    final LuceneIndex keywordIndex = luceneService.getIndex("keyword", REGION_NAME);
    final Map<String, Analyzer> keywordFieldAnalyzers = keywordIndex.getFieldAnalyzers();

    // Test whitespace analyzers
    assertThat(spaceFieldAnalyzers.get("field1").getClass().getCanonicalName())
        .isEqualTo(StandardAnalyzer.class.getCanonicalName());
    assertThat(spaceFieldAnalyzers.get("field2").getClass().getCanonicalName())
        .isEqualTo(StandardAnalyzer.class.getCanonicalName());
    assertThat(spaceFieldAnalyzers.get("field3").getClass().getCanonicalName())
        .isEqualTo(KeywordAnalyzer.class.getCanonicalName());

    // Test empty analyzers
    assertThat(emptyFieldAnalyzers2.get("field1").getClass().getCanonicalName())
        .isEqualTo(StandardAnalyzer.class.getCanonicalName());
    assertThat(emptyFieldAnalyzers2.get("field2").getClass().getCanonicalName())
        .isEqualTo(StandardAnalyzer.class.getCanonicalName());
    assertThat(emptyFieldAnalyzers2.get("field3").getClass().getCanonicalName())
        .isEqualTo(KeywordAnalyzer.class.getCanonicalName());

    // Test keyword analyzers
    assertThat(keywordFieldAnalyzers.get("field1").getClass().getCanonicalName())
        .isEqualTo(StandardAnalyzer.class.getCanonicalName());
    assertThat(keywordFieldAnalyzers.get("field2").getClass().getCanonicalName())
        .isEqualTo(StandardAnalyzer.class.getCanonicalName());
    assertThat(keywordFieldAnalyzers.get("field3").getClass().getCanonicalName())
        .isEqualTo(KeywordAnalyzer.class.getCanonicalName());

  }

  @Test
  public void describeIndexShouldReturnExistingIndex() {
    createIndex();

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess().containsOutput(INDEX_NAME);
  }

  @Test
  public void describeIndexShouldShowSerializer() {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER,
        PrimitiveSerializer.class.getCanonicalName());

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
    createRegion();

    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput(PrimitiveSerializer.class.getSimpleName());
  }

  @Test
  public void describeIndexShouldNotReturnResultWhenIndexNotFound() {
    createIndex();

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "notAnIndex");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("No lucene indexes found");
  }

  @Test
  public void describeIndexWithoutRegionShouldReturnErrorMessage() {
    createIndexWithoutRegion();
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "notAnIndex");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    gfsh.executeAndAssertThat(csb.toString()).statusIsError().containsOutput(REGION_NAME);
  }

  @Test
  public void searchShouldReturnCorrectResults() throws Exception {
    createIndex();
    Map<String, TestObject> entries = new HashMap<>();
    entries.put("A", new TestObject("value1 ", "value2", "value3"));
    entries.put("B", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("C", new TestObject("value1", "QWE", "RTY"));
    entries.put("D", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("E", new TestObject("value1", "ABC", "EFG"));
    entries.put("F", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("G", new TestObject(" value1", "JKR", "POW"));
    entries.put("H", new TestObject("ABC", "EFG", "H2J"));
    putEntries(entries, 8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "field1:value1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field1");

    gfsh.executeAndAssertThat(csb.toString()).tableHasColumnWithExactValuesInAnyOrder("key", "E",
        "A", "G", "C");
  }

  @Test
  public void searchShouldReturnResultsInCorrectOrderOfScore() throws Exception {
    createIndex();
    Map<String, TestObject> entries = new HashMap<>();

    entries.put("A", new TestObject("jon ", "value2", "value3"));
    entries.put("B", new TestObject("don", "EFG", "HIJ"));
    entries.put("C", new TestObject("eon", "QWE", "RTY"));
    entries.put("D", new TestObject("kion", "QWE", "RTY"));
    entries.put("E", new TestObject("ryan", "QWE", "RTY"));
    putEntries(entries, 5);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "field1:jon~");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field1");

    CommandResultAssert assertion = gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    try {
      assertion.tableHasColumnWithExactValuesInExactOrder("key", "A", "B", "C", "D");
    } catch (AssertionError e) {
      // Since B and C have the same score, we can expect them to appear in either order
      assertion.tableHasColumnWithExactValuesInExactOrder("key", "A", "C", "B", "D");
    }
  }

  @Test
  public void searchShouldReturnNoResults() throws Exception {
    createIndex();
    Map<String, TestObject> entries = new HashMap<>();
    entries.put("A", new TestObject("value1 ", "value2", "value3"));
    entries.put("B", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("C", new TestObject("value1", "QWE", "RTY"));
    entries.put("D", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("E", new TestObject(":value1", "ABC", "EFG"));
    entries.put("F", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("G", new TestObject(" value1", "JKR", "POW"));
    entries.put("H", new TestObject("ABC", "EFG", "H2J"));
    putEntries(entries, 8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "NotAnExistingValue");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field1");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput(LuceneCliStrings.LUCENE_SEARCH_INDEX__NO_RESULTS_MESSAGE);
  }

  @Test
  public void searchWithLimitShouldReturnCorrectResults() throws Exception {
    createIndex();
    Map<String, TestObject> entries = new HashMap<>();
    entries.put("A", new TestObject("value1 ", "value2", "value3"));
    entries.put("B", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("C", new TestObject("value1", "QWE", "RTY"));
    entries.put("D", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("E", new TestObject("value1", "ABC", "EFG"));
    entries.put("F", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("G", new TestObject(" value1", "JKR", "POW"));
    entries.put("H", new TestObject("ABC", "EFG", "H2J"));
    putEntries(entries, 8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "field1:value1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__LIMIT, "2");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("key", "A", "G");
  }

  @Test
  public void searchWithoutFieldNameShouldReturnCorrectResults() throws Exception {
    createIndex();
    Map<String, TestObject> entries = new HashMap<>();
    entries.put("A", new TestObject("value1 ", "value2", "value3"));
    entries.put("B", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("C", new TestObject("value1", "QWE", "RTY"));
    entries.put("D", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("E", new TestObject("value1", "ABC", "EFG"));
    entries.put("F", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("G", new TestObject("value1", "JKR", "POW"));
    entries.put("H", new TestObject("ABC", "EFG", "H2J"));
    putEntries(entries, 8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "QWE");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field2");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("key", "C");
  }

  @Test
  public void searchOnIndexWithoutRegionShouldReturnError() {
    createIndexWithoutRegion();
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "EFG");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field2");

    gfsh.executeAndAssertThat(csb.toString()).statusIsError()
        .containsOutput(getRegionNotFoundErrorMessage("/region"));
  }

  @Test
  public void searchWithoutIndexShouldReturnError() {
    createRegion();

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "EFG");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field2");

    String commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    CommandResult commandResult = gfsh.executeCommand(commandString);
    String resultAsString = gfsh.getGfshOutput();
    writeToLog("Result String :\n ", resultAsString);
    assertThat(commandResult.getStatus()).isEqualTo(Status.ERROR);
    assertThat(resultAsString.contains("Index " + INDEX_NAME + " not found"))
        .as("Unexpected CommandResult string :" + resultAsString).isTrue();
  }

  @Test
  public void searchIndexShouldReturnCorrectKeys() throws Exception {
    createIndex();
    Map<String, TestObject> entries = new HashMap<>();
    entries.put("A", new TestObject("value1 ", "value2", "value3"));
    entries.put("B", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("C", new TestObject("value1", "QWE", "RTY"));
    entries.put("D", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("E", new TestObject("value1", "ABC", "EFG"));
    entries.put("F", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("G", new TestObject("value1", "JKR", "POW"));
    entries.put("H", new TestObject("ABC", "EFG", "H2J"));
    putEntries(entries, 8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "value1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__KEYSONLY, "true");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("key", "C", "G", "E", "A");
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroySingleIndex(boolean createRegion) {
    if (createRegion) {
      createIndex();
    } else {
      createIndexWithoutRegion();
    }

    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEX_0_FROM_REGION_1,
        "index", "/region");
    gfsh.executeAndAssertThat("destroy lucene index --name=index --region=region").statusIsSuccess()
        .containsOutput(expectedStatus);
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroyAllIndexes(boolean createRegion) {
    if (createRegion) {
      createIndex();
    } else {
      createIndexWithoutRegion();
    }

    // Verify destroy all indexes is successful
    String expectedOutput = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEXES_FROM_REGION_0,
        new Object[] {"/region"});

    gfsh.executeAndAssertThat("destroy lucene index --region=region").statusIsSuccess()
        .containsOutput(expectedOutput);

    // Verify destroy all indexes again reports no indexes exist
    expectedOutput = String.format("No Lucene indexes were found in region %s", "/region");

    gfsh.executeAndAssertThat("destroy lucene index --region=region").statusIsSuccess()
        .containsOutput(expectedOutput);
  }

  @Test
  public void testDestroyNonExistentSingleIndex() {
    createRegion();
    String expectedStatus =
        String.format("Lucene index %s was not found in region %s", INDEX_NAME, '/' + REGION_NAME);

    gfsh.executeAndAssertThat("destroy lucene index --name=index --region=region").statusIsSuccess()
        .containsOutput(expectedStatus);
  }

  @Test
  public void testDestroyNonExistentIndexes() {
    createRegion();

    String expectedOutput = String.format("No Lucene indexes were found in region %s", "/region");
    gfsh.executeAndAssertThat("destroy lucene index --region=region").statusIsSuccess()
        .containsOutput(expectedOutput);
  }

  protected void createRegion() {
    server.getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
  }

  private void createIndex() {
    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new StandardAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    fieldAnalyzers.put("field3", null);
    luceneService.createIndexFactory().setFields(fieldAnalyzers).create(INDEX_NAME, REGION_NAME);
    createRegion();
  }

  private void createIndexWithoutRegion() {
    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new StandardAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    fieldAnalyzers.put("field3", null);
    luceneService.createIndexFactory().setFields(fieldAnalyzers).create(INDEX_NAME, REGION_NAME);
  }

  private void writeToLog(String text, String resultAsString) {
    System.out.println(text + ": " + testName.getMethodName() + " : ");
    System.out.println(text + ":" + resultAsString);
  }

  private void putEntries(Map<String, TestObject> entries, int countOfDocuments)
      throws InterruptedException {
    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    Region<String, TestObject> region = server.getCache().getRegion(REGION_NAME);
    region.putAll(entries);
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);
    LuceneIndexImpl index = (LuceneIndexImpl) luceneService.getIndex(INDEX_NAME, REGION_NAME);
    Awaitility.await().atMost(65, TimeUnit.SECONDS).until(
        () -> index.getIndexStats().getDocuments() >= countOfDocuments);
  }

  private void queryAndVerify(String queryString, String defaultField)
      throws LuceneQueryException {
    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    final LuceneQuery<String, TestObject> query = luceneService.createLuceneQueryFactory()
        .create(INDEX_NAME, REGION_NAME, queryString, defaultField);
    assertThat(query.findKeys()).isEqualTo(Collections.singletonList("A"));
  }

  private String getRegionNotFoundErrorMessage(String regionPath) {
    return CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__COULDNOT_FIND_MEMBERS_FOR_REGION_0,
        new Object[] {regionPath});
  }

  protected static class TestObject implements Serializable {
    private String field1;
    private String field2;
    private String field3;

    protected TestObject(String value1, String value2, String value3) {
      this.field1 = value1;
      this.field2 = value2;
      this.field3 = value3;
    }

    public String toString() {
      return "field1=" + field1 + " field2=" + field2 + " field3=" + field3;
    }
  }
}
