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
import static org.apache.geode.test.dunit.Assert.assertArrayEquals;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.LuceneIndexCreationProfile;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.repository.serializer.PrimitiveSerializer;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class LuceneIndexCommandsDUnitTest implements Serializable {

  @Rule
  public transient GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Rule
  public LocatorServerStartupRule startupRule = new LocatorServerStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM serverVM;

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    serverVM = startupRule.startServerAsJmxManager(0, props);
    connect(serverVM);
  }

  public void connect(MemberVM serverVM) throws Exception {
    gfsh.connectAndVerify(serverVM.getJmxPort(), GfshShellConnectionRule.PortType.jmxManager);
  }

  @Test
  public void listIndexShouldReturnExistingIndexWithStats() throws Exception {
    createIndex();

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_LIST_INDEX__STATS, "true");
    gfsh.executeAndVerifyCommand(csb.toString(), INDEX_NAME, "Documents");
  }

  @Test
  public void listIndexShouldReturnExistingIndexWithoutStats() throws Exception {
    createIndex();
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    gfsh.executeAndVerifyCommand(csb.toString(), INDEX_NAME);
    assertThat(gfsh.getGfshOutput()).doesNotContain("Documents");
  }

  @Test
  public void listIndexWhenNoExistingIndexShouldReturnNoIndex() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    gfsh.executeAndVerifyCommand(csb.toString(), "No lucene indexes found");
  }

  @Test
  public void listIndexShouldReturnCorrectStatus() throws Exception {
    createIndexWithoutRegion();

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_LIST_INDEX__STATS, "true");
    TabularResultData data =
        (TabularResultData) gfsh.executeAndVerifyCommand(csb.toString()).getResultData();
    assertEquals(Collections.singletonList(INDEX_NAME), data.retrieveAllValues("Index Name"));
    assertEquals(Collections.singletonList("Defined"), data.retrieveAllValues("Status"));
  }

  @Test
  public void listIndexWithStatsShouldReturnCorrectStats() throws Exception {
    createIndex();
    Map<String, TestObject> entries = new HashMap<>();
    entries.put("A", new TestObject("field1:value1", "field2:value2", "field3:value3"));
    entries.put("B", new TestObject("ABC", "EFG", "HIJ"));

    putEntries(entries, 2);
    queryAndVerify("field1:value1", "field1", Collections.singletonList("A"));

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_LIST_INDEX__STATS, "true");
    TabularResultData data =
        (TabularResultData) gfsh.executeAndVerifyCommand(csb.toString()).getResultData();

    assertEquals(Collections.singletonList(INDEX_NAME), data.retrieveAllValues("Index Name"));
    assertEquals(Collections.singletonList("Initialized"), data.retrieveAllValues("Status"));
    assertEquals(Collections.singletonList("/region"), data.retrieveAllValues("Region Path"));
    assertEquals(Collections.singletonList("1"), data.retrieveAllValues("Query Executions"));
    assertEquals(Collections.singletonList("2"), data.retrieveAllValues("Commits"));
    assertEquals(Collections.singletonList("2"), data.retrieveAllValues("Updates"));
    assertEquals(Collections.singletonList("2"), data.retrieveAllValues("Documents"));
  }

  @Test
  public void createIndexShouldCreateANewIndex() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    gfsh.executeAndVerifyCommand(csb.toString());

    serverVM.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      createRegion();
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertArrayEquals(new String[] {"field1", "field2", "field3"}, index.getFieldNames());
    });
  }

  @Test
  public void createIndexWithAnalyzersShouldCreateANewIndex() throws Exception {
    List<String> analyzerNames = new ArrayList<>();
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());
    analyzerNames.add(KeywordAnalyzer.class.getCanonicalName());
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, String.join(",", analyzerNames));

    gfsh.executeAndVerifyCommand(csb.toString());

    serverVM.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      createRegion();
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      final Map<String, Analyzer> fieldAnalyzers = index.getFieldAnalyzers();
      assertEquals(StandardAnalyzer.class, fieldAnalyzers.get("field1").getClass());
      assertEquals(KeywordAnalyzer.class, fieldAnalyzers.get("field2").getClass());
      assertEquals(StandardAnalyzer.class, fieldAnalyzers.get("field3").getClass());
    });
  }

  @Test
  public void createIndexWithALuceneSerializerShouldCreateANewIndex() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER,
        PrimitiveSerializer.class.getCanonicalName());

    gfsh.executeAndVerifyCommand(csb.toString());

    serverVM.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      createRegion();
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertThat(index.getLuceneSerializer()).isInstanceOf(PrimitiveSerializer.class);
    });
  }

  @Test
  public void createIndexShouldNotAcceptBadIndexOrRegionNames() {
    // CommandStringBuilder csb;
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, "\'__\'");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    gfsh.executeAndVerifyCommand(csb.toString(),
        "Region names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens, underscores, or forward slashes:");

    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, "\' @@@*%\'");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    gfsh.executeAndVerifyCommand(csb.toString(),
        "Region names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens, underscores, or forward slashes:");

    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "\'__\'");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    gfsh.executeAndVerifyCommand(csb.toString(),
        "Index names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens or underscores:");

    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "\' @@@*%\'");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    gfsh.executeAndVerifyCommand(csb.toString(),
        "Index names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens or underscores:");
  }

  @Test
  public void createIndexShouldTrimAnalyzerNames() throws Exception {
    List<String> analyzerNames = new ArrayList<>();
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());
    analyzerNames.add(KeywordAnalyzer.class.getCanonicalName());
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER,
        "\"org.apache.lucene.analysis.standard.StandardAnalyzer, org.apache.lucene.analysis.core.KeywordAnalyzer, org.apache.lucene.analysis.standard.StandardAnalyzer\"");

    gfsh.executeAndVerifyCommand(csb.toString());

    serverVM.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      createRegion();
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      final Map<String, Analyzer> fieldAnalyzers = index.getFieldAnalyzers();
      assertEquals(StandardAnalyzer.class, fieldAnalyzers.get("field1").getClass());
      assertEquals(KeywordAnalyzer.class, fieldAnalyzers.get("field2").getClass());
      assertEquals(StandardAnalyzer.class, fieldAnalyzers.get("field3").getClass());
    });
  }

  @Test
  public void createIndexWithoutRegionShouldReturnCorrectResults() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    gfsh.executeAndVerifyCommand(csb.toString());

    serverVM.invoke(() -> {
      LuceneServiceImpl luceneService = (LuceneServiceImpl) LuceneServiceProvider.get(getCache());
      final ArrayList<LuceneIndexCreationProfile> profiles =
          new ArrayList<>(luceneService.getAllDefinedIndexes());
      assertEquals(1, profiles.size());
      assertEquals(INDEX_NAME, profiles.get(0).getIndexName());
    });
  }

  @Test
  public void createIndexWithWhitespaceOrDefaultKeywordAnalyzerShouldUseStandardAnalyzer()
      throws Exception {
    // Test whitespace analyzer name
    String analyzerList = StandardAnalyzer.class.getCanonicalName() + ",     ,"
        + KeywordAnalyzer.class.getCanonicalName();
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "space");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, "'" + analyzerList + "'");

    gfsh.executeAndVerifyCommand(csb.toString());

    // Test empty analyzer name
    analyzerList =
        StandardAnalyzer.class.getCanonicalName() + ",," + KeywordAnalyzer.class.getCanonicalName();
    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "empty");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, analyzerList);

    gfsh.executeAndVerifyCommand(csb.toString());

    // Test keyword analyzer name
    analyzerList = StandardAnalyzer.class.getCanonicalName() + ",DEFAULT,"
        + KeywordAnalyzer.class.getCanonicalName();
    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "keyword");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, analyzerList);

    gfsh.executeAndVerifyCommand(csb.toString());

    serverVM.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      createRegion();
      final LuceneIndex spaceIndex = luceneService.getIndex("space", REGION_NAME);
      final Map<String, Analyzer> spaceFieldAnalyzers = spaceIndex.getFieldAnalyzers();

      final LuceneIndex emptyIndex = luceneService.getIndex("empty", REGION_NAME);
      final Map<String, Analyzer> emptyFieldAnalyzers2 = emptyIndex.getFieldAnalyzers();

      final LuceneIndex keywordIndex = luceneService.getIndex("keyword", REGION_NAME);
      final Map<String, Analyzer> keywordFieldAnalyzers = keywordIndex.getFieldAnalyzers();

      // Test whitespace analyzers
      assertEquals(StandardAnalyzer.class.getCanonicalName(),
          spaceFieldAnalyzers.get("field1").getClass().getCanonicalName());
      assertEquals(StandardAnalyzer.class.getCanonicalName(),
          spaceFieldAnalyzers.get("field2").getClass().getCanonicalName());
      assertEquals(KeywordAnalyzer.class.getCanonicalName(),
          spaceFieldAnalyzers.get("field3").getClass().getCanonicalName());

      // Test empty analyzers
      assertEquals(StandardAnalyzer.class.getCanonicalName(),
          emptyFieldAnalyzers2.get("field1").getClass().getCanonicalName());
      assertEquals(StandardAnalyzer.class.getCanonicalName(),
          emptyFieldAnalyzers2.get("field2").getClass().getCanonicalName());
      assertEquals(KeywordAnalyzer.class.getCanonicalName(),
          emptyFieldAnalyzers2.get("field3").getClass().getCanonicalName());

      // Test keyword analyzers
      assertEquals(StandardAnalyzer.class.getCanonicalName(),
          keywordFieldAnalyzers.get("field1").getClass().getCanonicalName());
      assertEquals(StandardAnalyzer.class.getCanonicalName(),
          keywordFieldAnalyzers.get("field2").getClass().getCanonicalName());
      assertEquals(KeywordAnalyzer.class.getCanonicalName(),
          keywordFieldAnalyzers.get("field3").getClass().getCanonicalName());
    });
  }

  @Test
  public void describeIndexShouldReturnExistingIndex() throws Exception {
    createIndex();

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    gfsh.executeAndVerifyCommand(csb.toString(), INDEX_NAME);
  }

  @Test
  public void describeIndexShouldShowSerializer() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER,
        PrimitiveSerializer.class.getCanonicalName());

    gfsh.executeAndVerifyCommand(csb.toString());
    serverVM.invoke(() -> createRegion());

    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    String resultAsString = gfsh.executeAndVerifyCommand(csb.toString()).toString();
    assertThat(resultAsString).contains(PrimitiveSerializer.class.getSimpleName());
  }

  @Test
  public void describeIndexShouldNotReturnResultWhenIndexNotFound() throws Exception {
    createIndex();

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "notAnIndex");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    gfsh.executeAndVerifyCommand(csb.toString(), "No lucene indexes found");
  }

  @Test
  public void describeIndexWithoutRegionShouldReturnErrorMessage() throws Exception {
    createIndexWithoutRegion();
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "notAnIndex");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    gfsh.executeAndVerifyCommand(csb.toString(), REGION_NAME);
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

    TabularResultData data =
        (TabularResultData) gfsh.executeAndVerifyCommand(csb.toString()).getResultData();
    assertEquals(4, data.retrieveAllValues("key").size());
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

    TabularResultData data =
        (TabularResultData) gfsh.executeAndVerifyCommand(csb.toString()).getResultData();
    assertEquals(4, data.retrieveAllValues("key").size());

    // confirm the order
    List<String> scoreRatings = data.retrieveAllValues("score");
    boolean inOrder = IntStream.range(0, scoreRatings.size() - 1)
        .allMatch(index -> scoreRatings.get(index).compareTo(scoreRatings.get(index + 1)) >= 0);
    assertTrue("Lucene search result not in expected order", inOrder);

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

    gfsh.executeAndVerifyCommand(csb.toString(),
        LuceneCliStrings.LUCENE_SEARCH_INDEX__NO_RESULTS_MESSAGE);
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
    TabularResultData data =
        (TabularResultData) gfsh.executeAndVerifyCommand(csb.toString()).getResultData();
    assertEquals(2, data.retrieveAllValues("key").size());
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

    TabularResultData data =
        (TabularResultData) gfsh.executeAndVerifyCommand(csb.toString()).getResultData();
    assertEquals(1, data.retrieveAllValues("key").size());
  }

  @Test
  public void searchOnIndexWithoutRegionShouldReturnError() throws Exception {

    createIndexWithoutRegion();
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "EFG");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field2");

    gfsh.executeAndVerifyCommand(csb.toString(), getRegionNotFoundErrorMessage("/region"));
  }

  @Test
  public void searchWithoutIndexShouldReturnError() throws Exception {

    serverVM.invoke(() -> createRegion());

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
    assertEquals(Status.ERROR, commandResult.getStatus());
    assertEquals("Unexpected CommandResult string :" + resultAsString, true,
        resultAsString.contains("Index " + INDEX_NAME + " not found"));
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

    TabularResultData data =
        (TabularResultData) gfsh.executeAndVerifyCommand(csb.toString()).getResultData();
    assertEquals(4, data.retrieveAllValues("key").size());
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroySingleIndex(boolean createRegion) throws Exception {
    if (createRegion) {
      createIndex();
    } else {
      createIndexWithoutRegion();
    }

    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEX_0_FROM_REGION_1,
        new Object[] {"index", "/region"});
    gfsh.executeAndVerifyCommand("destroy lucene index --name=index --region=region",
        expectedStatus);
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroyAllIndexes(boolean createRegion) throws Exception {
    if (createRegion) {
      createIndex();
    } else {
      createIndexWithoutRegion();
    }
    gfsh.executeAndVerifyCommand("destroy lucene index --region=region",
        CliStrings.format(
            LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEXES_FROM_REGION_0,
            new Object[] {"/region"}));
  }

  @Test
  public void testDestroyNonExistentSingleIndex() throws Exception {
    serverVM.invoke(() -> createRegion());
    String expectedStatus = LocalizedStrings.LuceneService_INDEX_0_NOT_FOUND_IN_REGION_1
        .toLocalizedString(new Object[] {INDEX_NAME, '/' + REGION_NAME});

    gfsh.executeAndVerifyCommand("destroy lucene index --name=index --region=region",
        expectedStatus);
  }

  @Test
  public void testDestroyNonExistentIndexes() throws Exception {
    serverVM.invoke(() -> createRegion());
    gfsh.executeAndVerifyCommand("destroy lucene index --region=region",
        LocalizedStrings.LuceneService_NO_INDEXES_WERE_FOUND_IN_REGION_0
            .toLocalizedString(new Object[] {"/region"}));
  }

  private void createRegion() {
    getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
  }

  private void createIndex() {
    serverVM.invoke(() -> {
      LuceneService luceneService =
          LuceneServiceProvider.get(LocatorServerStartupRule.serverStarter.getCache());
      Map<String, Analyzer> fieldAnalyzers = new HashMap();
      fieldAnalyzers.put("field1", new StandardAnalyzer());
      fieldAnalyzers.put("field2", new KeywordAnalyzer());
      fieldAnalyzers.put("field3", null);
      luceneService.createIndexFactory().setFields(fieldAnalyzers).create(INDEX_NAME, REGION_NAME);
      createRegion();
    });
  }

  private void createIndexWithoutRegion() {
    serverVM.invoke(() -> {
      LuceneService luceneService =
          LuceneServiceProvider.get(LocatorServerStartupRule.serverStarter.getCache());
      Map<String, Analyzer> fieldAnalyzers = new HashMap();
      fieldAnalyzers.put("field1", new StandardAnalyzer());
      fieldAnalyzers.put("field2", new KeywordAnalyzer());
      fieldAnalyzers.put("field3", null);
      luceneService.createIndexFactory().setFields(fieldAnalyzers).create(INDEX_NAME, REGION_NAME);
    });
  }

  private void writeToLog(String text, String resultAsString) {
    System.out.println(text + ": " + testName.getMethodName() + " : ");
    System.out.println(text + ":" + resultAsString);
  }

  private void putEntries(Map<String, TestObject> entries, int countOfDocuments) {
    serverVM.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      Region region = getCache().getRegion(REGION_NAME);
      region.putAll(entries);
      luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);
      LuceneIndexImpl index = (LuceneIndexImpl) luceneService.getIndex(INDEX_NAME, REGION_NAME);
      Awaitility.await().atMost(65, TimeUnit.SECONDS)
          .until(() -> assertEquals(countOfDocuments, index.getIndexStats().getDocuments()));

    });
  }

  private void queryAndVerify(String queryString, String defaultField, List<String> expectedKeys) {
    serverVM.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      final LuceneQuery<String, TestObject> query = luceneService.createLuceneQueryFactory()
          .create(INDEX_NAME, REGION_NAME, queryString, defaultField);
      assertEquals(Collections.singletonList("A"), query.findKeys());
    });
  }

  private String getRegionNotFoundErrorMessage(String regionPath) {
    return CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__COULDNOT_FIND_MEMBERS_FOR_REGION_0,
        new Object[] {regionPath});
  }

  private static Cache getCache() {
    return LocatorServerStartupRule.serverStarter.getCache();
  }

  protected class TestObject implements Serializable {
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
