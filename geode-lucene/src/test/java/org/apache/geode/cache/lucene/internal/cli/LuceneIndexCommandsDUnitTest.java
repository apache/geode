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
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertTrue;

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
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.commands.CliCommandTestBase;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
public class LuceneIndexCommandsDUnitTest extends CliCommandTestBase {


  @Before
  public void createJMXManager() {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(null);
  }

  @Test
  public void listIndexShouldReturnExistingIndexWithStats() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_LIST_INDEX__STATS, "true");
    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(INDEX_NAME));
    assertTrue(resultAsString.contains("Documents"));
  }

  @Test
  public void listIndexShouldReturnExistingIndexWithoutStats() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(INDEX_NAME));
    assertFalse(resultAsString.contains("Documents"));
  }

  @Test
  public void listIndexWhenNoExistingIndexShouldReturnNoIndex() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains("No lucene indexes found"));
  }

  @Test
  public void listIndexShouldReturnCorrectStatus() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndexWithoutRegion(vm1);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_LIST_INDEX__STATS, "true");
    TabularResultData data = (TabularResultData) executeCommandAndGetResult(csb).getResultData();
    assertEquals(Collections.singletonList(INDEX_NAME), data.retrieveAllValues("Index Name"));
    assertEquals(Collections.singletonList("Defined"), data.retrieveAllValues("Status"));
  }

  @Test
  public void listIndexWithStatsShouldReturnCorrectStats() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    Map<String, TestObject> entries = new HashMap<>();
    entries.put("A", new TestObject("field1:value1", "field2:value2", "field3:value3"));
    entries.put("B", new TestObject("ABC", "EFG", "HIJ"));

    putEntries(vm1, entries, 2);
    queryAndVerify(vm1, "field1:value1", "field1", Collections.singletonList("A"));

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_LIST_INDEX__STATS, "true");
    TabularResultData data = (TabularResultData) executeCommandAndGetResult(csb).getResultData();

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
    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(() -> {
      getCache();
    });

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    String resultAsString = executeCommandAndLogResult(csb);

    vm1.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      createRegion();
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertArrayEquals(new String[] {"field1", "field2", "field3"}, index.getFieldNames());
    });
  }

  @Test
  public void createIndexWithAnalyzersShouldCreateANewIndex() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(() -> {
      getCache();
    });

    List<String> analyzerNames = new ArrayList<>();
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());
    analyzerNames.add(KeywordAnalyzer.class.getCanonicalName());
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());


    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, String.join(",", analyzerNames));

    String resultAsString = executeCommandAndLogResult(csb);

    vm1.invoke(() -> {
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
  public void createIndexShouldNotAcceptBadIndexOrRegionNames() {
    final VM vm1 = Host.getHost(0).getVM(-1);
    vm1.invoke(() -> {
      getCache();
    });

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, "\'__\'");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(
        "Region names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens, underscores, or forward slashes:"));

    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, "\' @@@*%\'");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(
        "Region names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens, underscores, or forward slashes:"));

    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "\'__\'");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(
        "Index names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens or underscores:"));

    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "\' @@@*%\'");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(
        "Index names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens or underscores:"));
  }

  @Test
  public void createIndexShouldTrimAnalyzerNames() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(-1);
    vm1.invoke(() -> {
      getCache();
    });

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

    String resultAsString = executeCommandAndLogResult(csb);

    vm1.invoke(() -> {
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
    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(() -> {
      getCache();
    });

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    String resultAsString = executeCommandAndLogResult(csb);

    vm1.invoke(() -> {
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
    final VM vm1 = Host.getHost(0).getVM(-1);
    vm1.invoke(() -> {
      getCache();
    });

    // Test whitespace analyzer name
    String analyzerList = StandardAnalyzer.class.getCanonicalName() + ",     ,"
        + KeywordAnalyzer.class.getCanonicalName();
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "space");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, "'" + analyzerList + "'");

    String resultAsString = executeCommandAndLogResult(csb);

    // Test empty analyzer name
    analyzerList =
        StandardAnalyzer.class.getCanonicalName() + ",," + KeywordAnalyzer.class.getCanonicalName();
    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "empty");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, analyzerList);

    resultAsString = executeCommandAndLogResult(csb);

    // Test keyword analyzer name
    analyzerList = StandardAnalyzer.class.getCanonicalName() + ",DEFAULT,"
        + KeywordAnalyzer.class.getCanonicalName();
    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "keyword");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, analyzerList);

    resultAsString = executeCommandAndLogResult(csb);

    vm1.invoke(() -> {
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
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(INDEX_NAME));
  }

  @Test
  public void describeIndexShouldNotReturnResultWhenIndexNotFound() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "notAnIndex");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    String resultAsString = executeCommandAndLogResult(csb);

    assertTrue(resultAsString.contains("No lucene indexes found"));
  }

  @Test
  public void describeIndexWithoutRegionShouldReturnErrorMessage() throws Exception {

    final VM vm1 = Host.getHost(0).getVM(1);

    createIndexWithoutRegion(vm1);
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "notAnIndex");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(getRegionNotFoundErrorMessage(REGION_NAME)));
  }

  @Test
  public void searchShouldReturnCorrectResults() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    Map<String, TestObject> entries = new HashMap<>();
    entries.put("A", new TestObject("value1 ", "value2", "value3"));
    entries.put("B", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("C", new TestObject("value1", "QWE", "RTY"));
    entries.put("D", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("E", new TestObject("value1", "ABC", "EFG"));
    entries.put("F", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("G", new TestObject(" value1", "JKR", "POW"));
    entries.put("H", new TestObject("ABC", "EFG", "H2J"));
    putEntries(vm1, entries, 8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "field1:value1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field1");
    executeCommandAndLogResult(csb);

    TabularResultData data = (TabularResultData) executeCommandAndGetResult(csb).getResultData();
    assertEquals(4, data.retrieveAllValues("key").size());
  }

  @Test
  public void searchShouldReturnNoResults() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    Map<String, TestObject> entries = new HashMap<>();
    entries.put("A", new TestObject("value1 ", "value2", "value3"));
    entries.put("B", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("C", new TestObject("value1", "QWE", "RTY"));
    entries.put("D", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("E", new TestObject(":value1", "ABC", "EFG"));
    entries.put("F", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("G", new TestObject(" value1", "JKR", "POW"));
    entries.put("H", new TestObject("ABC", "EFG", "H2J"));
    putEntries(vm1, entries, 8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "NotAnExistingValue");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field1");
    executeCommandAndLogResult(csb);

    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(LuceneCliStrings.LUCENE_SEARCH_INDEX__NO_RESULTS_MESSAGE));
  }

  @Test
  public void searchWithLimitShouldReturnCorrectResults() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    Map<String, TestObject> entries = new HashMap<>();
    entries.put("A", new TestObject("value1 ", "value2", "value3"));
    entries.put("B", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("C", new TestObject("value1", "QWE", "RTY"));
    entries.put("D", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("E", new TestObject("value1", "ABC", "EFG"));
    entries.put("F", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("G", new TestObject(" value1", "JKR", "POW"));
    entries.put("H", new TestObject("ABC", "EFG", "H2J"));
    putEntries(vm1, entries, 8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "field1:value1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__LIMIT, "2");
    executeCommandAndLogResult(csb);
    TabularResultData data = (TabularResultData) executeCommandAndGetResult(csb).getResultData();
    assertEquals(2, data.retrieveAllValues("key").size());
  }

  @Test
  public void searchWithoutFieldNameShouldReturnCorrectResults() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    Map<String, TestObject> entries = new HashMap<>();
    entries.put("A", new TestObject("value1 ", "value2", "value3"));
    entries.put("B", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("C", new TestObject("value1", "QWE", "RTY"));
    entries.put("D", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("E", new TestObject("value1", "ABC", "EFG"));
    entries.put("F", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("G", new TestObject("value1", "JKR", "POW"));
    entries.put("H", new TestObject("ABC", "EFG", "H2J"));
    putEntries(vm1, entries, 8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "QWE");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field2");
    executeCommandAndLogResult(csb);

    TabularResultData data = (TabularResultData) executeCommandAndGetResult(csb).getResultData();
    assertEquals(1, data.retrieveAllValues("key").size());
  }

  @Test
  public void searchOnIndexWithoutRegionShouldReturnError() throws Exception {

    final VM vm1 = Host.getHost(0).getVM(1);

    createIndexWithoutRegion(vm1);
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "EFG");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field2");

    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(getRegionNotFoundErrorMessage(REGION_NAME)));
  }

  @Test
  public void searchWithoutIndexShouldReturnError() throws Exception {

    final VM vm1 = Host.getHost(0).getVM(1);

    vm1.invoke(() -> createRegion());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "EFG");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field2");

    String commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Result String :\n ", resultAsString);
    assertEquals(Status.ERROR, commandResult.getStatus());
    assertEquals("Unexpected CommandResult string :" + resultAsString, true,
        resultAsString.contains("Index " + INDEX_NAME + " not found"));
  }

  @Test
  public void searchIndexShouldReturnCorrectKeys() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    Map<String, TestObject> entries = new HashMap<>();
    entries.put("A", new TestObject("value1 ", "value2", "value3"));
    entries.put("B", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("C", new TestObject("value1", "QWE", "RTY"));
    entries.put("D", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("E", new TestObject("value1", "ABC", "EFG"));
    entries.put("F", new TestObject("ABC", "EFG", "HIJ"));
    entries.put("G", new TestObject("value1", "JKR", "POW"));
    entries.put("H", new TestObject("ABC", "EFG", "H2J"));
    putEntries(vm1, entries, 8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "value1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__KEYSONLY, "true");
    executeCommandAndLogResult(csb);

    TabularResultData data = (TabularResultData) executeCommandAndGetResult(csb).getResultData();
    assertEquals(4, data.retrieveAllValues("key").size());
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroySingleIndex(boolean createRegion) throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);
    if (createRegion) {
      createIndex(vm1);
    } else {
      createIndexWithoutRegion(vm1);
    }
    CommandResult result = createAndExecuteDestroyIndexCommand(INDEX_NAME, REGION_NAME);
    String resultAsString = commandResultToString(result);
    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEX_0_FROM_REGION_1,
        new Object[] {INDEX_NAME, REGION_NAME});
    assertTrue(resultAsString.contains(expectedStatus));
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroyAllIndexes(boolean createRegion) throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);
    if (createRegion) {
      createIndex(vm1);
    } else {
      createIndexWithoutRegion(vm1);
    }
    CommandResult result = createAndExecuteDestroyIndexCommand(null, REGION_NAME);
    String resultAsString = commandResultToString(result);
    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEXES_FROM_REGION_0,
        new Object[] {REGION_NAME});
    assertTrue(resultAsString.contains(expectedStatus));
  }

  @Test
  public void testDestroyNonExistentSingleIndex() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(() -> createRegion());
    CommandResult result = createAndExecuteDestroyIndexCommand(INDEX_NAME, REGION_NAME);
    String resultAsString = commandResultToString(result);
    String expectedStatus = LocalizedStrings.LuceneService_INDEX_0_NOT_FOUND_IN_REGION_1
        .toLocalizedString(new Object[] {INDEX_NAME, '/' + REGION_NAME});
    assertTrue(resultAsString.contains(expectedStatus));
  }

  @Test
  public void testDestroyNonExistentIndexes() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(() -> createRegion());
    CommandResult result = createAndExecuteDestroyIndexCommand(null, REGION_NAME);
    String resultAsString = commandResultToString(result);
    String expectedStatus = LocalizedStrings.LuceneService_NO_INDEXES_WERE_FOUND_IN_REGION_0
        .toLocalizedString(new Object[] {'/' + REGION_NAME});
    assertTrue(resultAsString.contains(expectedStatus));
  }

  private CommandResult createAndExecuteDestroyIndexCommand(String indexName, String regionPath)
      throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESTROY_INDEX);
    if (indexName != null) {
      csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, indexName);
    }
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, regionPath);
    return executeCommandAndGetResult(csb);
  }

  private void createRegion() {
    getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
  }

  private String executeCommandAndLogResult(final CommandStringBuilder csb) {
    String commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Result String :\n ", resultAsString);
    assertEquals("Command failed\n" + resultAsString, Status.OK, commandResult.getStatus());
    return resultAsString;
  }

  private CommandResult executeCommandAndGetResult(final CommandStringBuilder csb) {
    String commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Result String :\n ", resultAsString);
    assertEquals("Command failed\n" + resultAsString, Status.OK, commandResult.getStatus());
    return commandResult;
  }

  private void createIndex(final VM vm1) {
    vm1.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      Map<String, Analyzer> fieldAnalyzers = new HashMap();
      fieldAnalyzers.put("field1", new StandardAnalyzer());
      fieldAnalyzers.put("field2", new KeywordAnalyzer());
      fieldAnalyzers.put("field3", null);
      luceneService.createIndexFactory().setFields(fieldAnalyzers).create(INDEX_NAME, REGION_NAME);
      createRegion();
    });
  }

  private void createIndexWithoutRegion(final VM vm1) {
    vm1.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      Map<String, Analyzer> fieldAnalyzers = new HashMap();
      fieldAnalyzers.put("field1", new StandardAnalyzer());
      fieldAnalyzers.put("field2", new KeywordAnalyzer());
      fieldAnalyzers.put("field3", null);
      luceneService.createIndexFactory().setFields(fieldAnalyzers).create(INDEX_NAME, REGION_NAME);
    });
  }

  private void writeToLog(String text, String resultAsString) {
    System.out.println(text + ": " + getTestMethodName() + " : ");
    System.out.println(text + ":" + resultAsString);
  }

  private void putEntries(final VM vm1, Map<String, TestObject> entries, int countOfDocuments) {
    Cache cache = getCache();
    vm1.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      Region region = getCache().getRegion(REGION_NAME);
      region.putAll(entries);
      luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);
      LuceneIndexImpl index = (LuceneIndexImpl) luceneService.getIndex(INDEX_NAME, REGION_NAME);
      Awaitility.await().atMost(65, TimeUnit.SECONDS)
          .until(() -> assertEquals(countOfDocuments, index.getIndexStats().getDocuments()));

    });
  }

  private void queryAndVerify(VM vm1, String queryString, String defaultField,
      List<String> expectedKeys) {
    vm1.invoke(() -> {
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
