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

import static junit.framework.TestCase.assertSame;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneCreateIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneDescribeIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneDestroyIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneListIndexFunction;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.util.CollectionUtils;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
 * The LuceneIndexCommandsJUnitTest class is a test suite of test cases testing the contract and
 * functionality of the LuceneIndexCommands class.
 *
 * @see LuceneIndexCommands
 * @see LuceneIndexDetails
 * @see LuceneListIndexFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.jmock.lib.legacy.ClassImposteriser
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
@RunWith(JUnitParamsRunner.class)
public class LuceneIndexCommandsJUnitTest {

  private InternalCache mockCache;

  @Before
  public void before() throws Exception {
    this.mockCache = mock(InternalCache.class, "InternalCache");
    when(this.mockCache.getSecurityService()).thenReturn(mock(SecurityService.class));
  }

  @Test
  public void testListIndexWithoutStats() throws Exception {
    final String serverName = "mockServer";
    final AbstractExecution mockFunctionExecutor =
        mock(AbstractExecution.class, "Function Executor");
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");

    String[] searchableFields = {"field1", "field2", "field3"};
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new StandardAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    fieldAnalyzers.put("field3", null);
    final LuceneIndexDetails indexDetails1 = createIndexDetails("memberFive", "/Employees",
        searchableFields, fieldAnalyzers, true, serverName);
    final LuceneIndexDetails indexDetails2 = createIndexDetails("memberSix", "/Employees",
        searchableFields, fieldAnalyzers, false, serverName);
    final LuceneIndexDetails indexDetails3 = createIndexDetails("memberTen", "/Employees",
        searchableFields, fieldAnalyzers, true, serverName);

    final List<Set<LuceneIndexDetails>> results = new ArrayList<>();

    results.add(CollectionUtils.asSet(indexDetails2, indexDetails1, indexDetails3));

    when(mockFunctionExecutor.execute(isA(LuceneListIndexFunction.class)))
        .thenReturn(mockResultCollector);
    when(mockResultCollector.getResult()).thenReturn(results);

    final LuceneIndexCommands commands = createIndexCommands(this.mockCache, mockFunctionExecutor);

    CommandResult result = (CommandResult) commands.listIndex(false);
    TabularResultData data = (TabularResultData) result.getResultData();
    assertEquals(Arrays.asList("memberFive", "memberSix", "memberTen"),
        data.retrieveAllValues("Index Name"));
    assertEquals(Arrays.asList("/Employees", "/Employees", "/Employees"),
        data.retrieveAllValues("Region Path"));
    assertEquals(Arrays.asList("[field1, field2, field3]", "[field1, field2, field3]",
        "[field1, field2, field3]"), data.retrieveAllValues("Indexed Fields"));
    assertEquals(
        Arrays.asList("{field1=StandardAnalyzer, field2=KeywordAnalyzer}",
            "{field1=StandardAnalyzer, field2=KeywordAnalyzer}",
            "{field1=StandardAnalyzer, field2=KeywordAnalyzer}"),
        data.retrieveAllValues("Field Analyzer"));
    assertEquals(Arrays.asList("Initialized", "Defined", "Initialized"),
        data.retrieveAllValues("Status"));
  }

  @Test
  public void testListIndexWithStats() throws Exception {
    final String serverName = "mockServer";
    final AbstractExecution mockFunctionExecutor =
        mock(AbstractExecution.class, "Function Executor");
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");
    final LuceneIndexStats mockIndexStats1 = getMockIndexStats(1, 10, 5, 1);
    final LuceneIndexStats mockIndexStats2 = getMockIndexStats(2, 20, 10, 2);
    final LuceneIndexStats mockIndexStats3 = getMockIndexStats(3, 30, 15, 3);
    String[] searchableFields = {"field1", "field2", "field3"};
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new StandardAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    fieldAnalyzers.put("field3", null);
    final LuceneIndexDetails indexDetails1 = createIndexDetails("memberFive", "/Employees",
        searchableFields, fieldAnalyzers, mockIndexStats1, true, serverName);
    final LuceneIndexDetails indexDetails2 = createIndexDetails("memberSix", "/Employees",
        searchableFields, fieldAnalyzers, mockIndexStats2, true, serverName);
    final LuceneIndexDetails indexDetails3 = createIndexDetails("memberTen", "/Employees",
        searchableFields, fieldAnalyzers, mockIndexStats3, true, serverName);

    final List<Set<LuceneIndexDetails>> results = new ArrayList<>();

    results.add(CollectionUtils.asSet(indexDetails2, indexDetails1, indexDetails3));

    when(mockFunctionExecutor.execute(isA(LuceneListIndexFunction.class)))
        .thenReturn(mockResultCollector);
    when(mockResultCollector.getResult()).thenReturn(results);

    final LuceneIndexCommands commands = createIndexCommands(this.mockCache, mockFunctionExecutor);

    CommandResult result = (CommandResult) commands.listIndex(true);
    TabularResultData data = (TabularResultData) result.getResultData();
    assertEquals(Arrays.asList("memberFive", "memberSix", "memberTen"),
        data.retrieveAllValues("Index Name"));
    assertEquals(Arrays.asList("/Employees", "/Employees", "/Employees"),
        data.retrieveAllValues("Region Path"));
    assertEquals(Arrays.asList("[field1, field2, field3]", "[field1, field2, field3]",
        "[field1, field2, field3]"), data.retrieveAllValues("Indexed Fields"));
    assertEquals(
        Arrays.asList("{field1=StandardAnalyzer, field2=KeywordAnalyzer}",
            "{field1=StandardAnalyzer, field2=KeywordAnalyzer}",
            "{field1=StandardAnalyzer, field2=KeywordAnalyzer}"),
        data.retrieveAllValues("Field Analyzer"));
    assertEquals(Arrays.asList("1", "2", "3"), data.retrieveAllValues("Query Executions"));
    assertEquals(Arrays.asList("10", "20", "30"), data.retrieveAllValues("Commits"));
    assertEquals(Arrays.asList("5", "10", "15"), data.retrieveAllValues("Updates"));
    assertEquals(Arrays.asList("1", "2", "3"), data.retrieveAllValues("Documents"));
  }

  @Test
  public void testCreateIndex() throws Exception {
    final ResultCollector mockResultCollector = mock(ResultCollector.class);
    final LuceneIndexCommands commands = spy(createIndexCommands(this.mockCache, null));

    final List<CliFunctionResult> cliFunctionResults = new ArrayList<>();
    cliFunctionResults.add(new CliFunctionResult("member1", true, "Index Created"));
    cliFunctionResults.add(new CliFunctionResult("member2", false, "Index creation failed"));
    cliFunctionResults.add(new CliFunctionResult("member3", true, "Index Created"));

    doReturn(mockResultCollector).when(commands).executeFunctionOnAllMembers(
        isA(LuceneCreateIndexFunction.class), any(LuceneIndexInfo.class));
    doReturn(cliFunctionResults).when(mockResultCollector).getResult();

    String indexName = "index";
    String regionPath = "regionPath";
    String[] searchableFields = {"field1", "field2", "field3"};
    String[] fieldAnalyzers = {StandardAnalyzer.class.getCanonicalName(),
        KeywordAnalyzer.class.getCanonicalName(), StandardAnalyzer.class.getCanonicalName()};

    CommandResult result = (CommandResult) commands.createIndex(indexName, regionPath,
        searchableFields, fieldAnalyzers);
    assertEquals(Status.OK, result.getStatus());
    TabularResultData data = (TabularResultData) result.getResultData();
    assertEquals(Arrays.asList("member1", "member2", "member3"), data.retrieveAllValues("Member"));
    assertEquals(Arrays.asList("Successfully created lucene index", "Failed: Index creation failed",
        "Successfully created lucene index"), data.retrieveAllValues("Status"));
  }

  @Test
  public void testDescribeIndex() throws Exception {
    final String serverName = "mockServer";
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");
    final LuceneIndexCommands commands = spy(createIndexCommands(this.mockCache, null));

    String[] searchableFields = {"field1", "field2", "field3"};
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new StandardAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    fieldAnalyzers.put("field3", null);
    final LuceneIndexStats mockIndexStats = getMockIndexStats(1, 10, 5, 1);
    final List<LuceneIndexDetails> indexDetails = new ArrayList<>();
    indexDetails.add(createIndexDetails("memberFive", "/Employees", searchableFields,
        fieldAnalyzers, mockIndexStats, true, serverName));

    doReturn(mockResultCollector).when(commands).executeFunctionOnRegion(
        isA(LuceneDescribeIndexFunction.class), any(LuceneIndexInfo.class), eq(true));
    doReturn(indexDetails).when(mockResultCollector).getResult();

    CommandResult result = (CommandResult) commands.describeIndex("memberFive", "/Employees");

    TabularResultData data = (TabularResultData) result.getResultData();
    assertEquals(Collections.singletonList("memberFive"), data.retrieveAllValues("Index Name"));
    assertEquals(Collections.singletonList("/Employees"), data.retrieveAllValues("Region Path"));
    assertEquals(Collections.singletonList("[field1, field2, field3]"),
        data.retrieveAllValues("Indexed Fields"));
    assertEquals(Collections.singletonList("{field1=StandardAnalyzer, field2=KeywordAnalyzer}"),
        data.retrieveAllValues("Field Analyzer"));
    assertEquals(Collections.singletonList("Initialized"), data.retrieveAllValues("Status"));
    assertEquals(Collections.singletonList("1"), data.retrieveAllValues("Query Executions"));
    assertEquals(Collections.singletonList("10"), data.retrieveAllValues("Commits"));
    assertEquals(Collections.singletonList("5"), data.retrieveAllValues("Updates"));
    assertEquals(Collections.singletonList("1"), data.retrieveAllValues("Documents"));
  }

  @Test
  public void testSearchIndex() throws Exception {
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");
    final LuceneIndexCommands commands = spy(createIndexCommands(this.mockCache, null));

    final List<Set<LuceneSearchResults>> queryResultsList = new ArrayList<>();
    HashSet<LuceneSearchResults> queryResults = new HashSet<>();
    queryResults.add(createQueryResults("A", "Result1", Float.valueOf("1.3")));
    queryResults.add(createQueryResults("B", "Result1", Float.valueOf("1.2")));
    queryResults.add(createQueryResults("C", "Result1", Float.valueOf("1.1")));
    queryResultsList.add(queryResults);
    doReturn(mockResultCollector).when(commands).executeSearch(isA(LuceneQueryInfo.class));
    doReturn(queryResultsList).when(mockResultCollector).getResult();

    CommandResult result =
        (CommandResult) commands.searchIndex("index", "region", "Result1", "field1", -1, false);

    TabularResultData data = (TabularResultData) result.getResultData();

    assertEquals(Arrays.asList("C", "B", "A"), data.retrieveAllValues("key"));
    assertEquals(Arrays.asList("Result1", "Result1", "Result1"), data.retrieveAllValues("value"));
    assertEquals(Arrays.asList("1.1", "1.2", "1.3"), data.retrieveAllValues("score"));
  }

  @Ignore
  public void testSearchIndexWithPaging() throws Exception {
    final Gfsh mockGfsh = mock(Gfsh.class);
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");
    final LuceneIndexCommands commands = spy(createIndexCommands(this.mockCache, null));
    ArgumentCaptor<String> resultCaptor = ArgumentCaptor.forClass(String.class);

    LuceneSearchResults result1 = createQueryResults("A", "Result1", Float.valueOf("1.7"));
    LuceneSearchResults result2 = createQueryResults("B", "Result1", Float.valueOf("1.6"));
    LuceneSearchResults result3 = createQueryResults("C", "Result1", Float.valueOf("1.5"));
    LuceneSearchResults result4 = createQueryResults("D", "Result1", Float.valueOf("1.4"));
    LuceneSearchResults result5 = createQueryResults("E", "Result1", Float.valueOf("1.3"));
    LuceneSearchResults result6 = createQueryResults("F", "Result1", Float.valueOf("1.2"));
    LuceneSearchResults result7 = createQueryResults("G", "Result1", Float.valueOf("1.1"));
    final List<Set<LuceneSearchResults>> queryResultsList =
        getSearchResults(result1, result2, result3, result4, result5, result6, result7);

    doReturn(mockResultCollector).when(commands).executeSearch(any(LuceneQueryInfo.class));
    doReturn(queryResultsList).when(mockResultCollector).getResult();
    doReturn(mockGfsh).when(commands).initGfsh();
    when(mockGfsh.interact(anyString())).thenReturn("n").thenReturn("n").thenReturn("n")
        .thenReturn("n").thenReturn("p").thenReturn("p").thenReturn("p").thenReturn("p")
        .thenReturn("p").thenReturn("n").thenReturn("q");

    LuceneSearchResults[] expectedResults =
        new LuceneSearchResults[] {result7, result6, result5, result4, result3, result2, result1};
    String expectedPage1 = getPage(expectedResults, new int[] {0, 1});
    String expectedPage2 = getPage(expectedResults, new int[] {2, 3});
    String expectedPage3 = getPage(expectedResults, new int[] {4, 5});
    String expectedPage4 = getPage(expectedResults, new int[] {6});

    commands.searchIndex("index", "region", "Result1", "field1", -1, false);
    verify(mockGfsh, times(20)).printAsInfo(resultCaptor.capture());
    List<String> actualPageResults = resultCaptor.getAllValues();

    assertEquals(expectedPage1, actualPageResults.get(0));
    assertEquals("\t\tPage 1 of 4", actualPageResults.get(1));

    assertEquals(expectedPage2, actualPageResults.get(2));
    assertEquals("\t\tPage 2 of 4", actualPageResults.get(3));

    assertEquals(expectedPage3, actualPageResults.get(4));
    assertEquals("\t\tPage 3 of 4", actualPageResults.get(5));

    assertEquals(expectedPage4, actualPageResults.get(6));
    assertEquals("\t\tPage 4 of 4", actualPageResults.get(7));

    assertEquals("No more results to display.", actualPageResults.get(8));

    assertEquals(expectedPage4, actualPageResults.get(9));
    assertEquals("\t\tPage 4 of 4", actualPageResults.get(10));

    assertEquals(expectedPage3, actualPageResults.get(11));
    assertEquals("\t\tPage 3 of 4", actualPageResults.get(12));

    assertEquals(expectedPage2, actualPageResults.get(13));
    assertEquals("\t\tPage 2 of 4", actualPageResults.get(14));

    assertEquals(expectedPage1, actualPageResults.get(15));
    assertEquals("\t\tPage 1 of 4", actualPageResults.get(16));

    assertEquals("At the top of the search results.", actualPageResults.get(17));

    assertEquals(expectedPage1, actualPageResults.get(18));
    assertEquals("\t\tPage 1 of 4", actualPageResults.get(19));
  }

  @Test
  public void testSearchIndexWithKeysOnly() throws Exception {
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");
    final LuceneIndexCommands commands = spy(createIndexCommands(this.mockCache, null));

    final List<Set<LuceneSearchResults>> queryResultsList = new ArrayList<>();
    HashSet<LuceneSearchResults> queryResults = new HashSet<>();
    queryResults.add(createQueryResults("A", "Result1", Float.valueOf("1.3")));
    queryResults.add(createQueryResults("B", "Result1", Float.valueOf("1.2")));
    queryResults.add(createQueryResults("C", "Result1", Float.valueOf("1.1")));
    queryResultsList.add(queryResults);
    doReturn(mockResultCollector).when(commands).executeSearch(isA(LuceneQueryInfo.class));
    doReturn(queryResultsList).when(mockResultCollector).getResult();

    CommandResult result =
        (CommandResult) commands.searchIndex("index", "region", "Result1", "field1", -1, true);

    TabularResultData data = (TabularResultData) result.getResultData();

    assertEquals(Arrays.asList("C", "B", "A"), data.retrieveAllValues("key"));
  }

  @Test
  public void testSearchIndexWhenSearchResultsHaveSameScore() throws Exception {
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");
    final LuceneIndexCommands commands = spy(createIndexCommands(this.mockCache, null));

    final List<Set<LuceneSearchResults>> queryResultsList = new ArrayList<>();
    HashSet<LuceneSearchResults> queryResults = new HashSet<>();
    queryResults.add(createQueryResults("A", "Result1", 1));
    queryResults.add(createQueryResults("B", "Result1", 1));
    queryResults.add(createQueryResults("C", "Result1", 1));
    queryResults.add(createQueryResults("D", "Result1", 1));
    queryResults.add(createQueryResults("E", "Result1", 1));
    queryResults.add(createQueryResults("F", "Result1", 1));
    queryResults.add(createQueryResults("G", "Result1", 1));
    queryResults.add(createQueryResults("H", "Result1", 1));
    queryResults.add(createQueryResults("I", "Result1", 1));
    queryResults.add(createQueryResults("J", "Result1", 1));
    queryResults.add(createQueryResults("K", "Result1", 1));
    queryResults.add(createQueryResults("L", "Result1", 1));
    queryResults.add(createQueryResults("M", "Result1", 1));
    queryResults.add(createQueryResults("N", "Result1", 1));
    queryResults.add(createQueryResults("P", "Result1", 1));
    queryResults.add(createQueryResults("Q", "Result1", 1));
    queryResults.add(createQueryResults("R", "Result1", 1));
    queryResults.add(createQueryResults("S", "Result1", 1));
    queryResults.add(createQueryResults("T", "Result1", 1));
    queryResultsList.add(queryResults);

    doReturn(mockResultCollector).when(commands).executeSearch(isA(LuceneQueryInfo.class));
    doReturn(queryResultsList).when(mockResultCollector).getResult();

    CommandResult result =
        (CommandResult) commands.searchIndex("index", "region", "Result1", "field1", -1, true);

    TabularResultData data = (TabularResultData) result.getResultData();

    assertEquals(queryResults.size(), data.retrieveAllValues("key").size());
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroySingleIndexNoRegionMembers(boolean expectedToSucceed) throws Exception {
    LuceneIndexCommands commands = createTestLuceneIndexCommandsForDestroyIndex();
    String indexName = "index";
    String regionPath = "regionPath";

    final ResultCollector mockResultCollector = mock(ResultCollector.class);
    final List<CliFunctionResult> cliFunctionResults = new ArrayList<>();
    String expectedStatus;
    if (expectedToSucceed) {
      expectedStatus = CliStrings.format(
          LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEX_0_FROM_REGION_1,
          new Object[] {indexName, regionPath});
      cliFunctionResults.add(new CliFunctionResult("member0"));
    } else {
      Exception e = new IllegalStateException("failed");
      expectedStatus = e.getMessage();
      cliFunctionResults.add(new CliFunctionResult("member0", e, e.getMessage()));
    }

    doReturn(mockResultCollector).when(commands).executeFunction(
        isA(LuceneDestroyIndexFunction.class), any(LuceneDestroyIndexInfo.class), any());
    doReturn(cliFunctionResults).when(mockResultCollector).getResult();

    doReturn(Collections.emptySet()).when(commands).getNormalMembers(any());
    doReturn(Collections.emptySet()).when(commands).getRegionMembers(any(), any());

    CommandResult result = (CommandResult) commands.destroyIndex(indexName, regionPath);
    verifyDestroyIndexCommandResult(result, cliFunctionResults, expectedStatus);
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroySingleIndexWithRegionMembers(boolean expectedToSucceed) throws Exception {
    LuceneIndexCommands commands = createTestLuceneIndexCommandsForDestroyIndex();
    String indexName = "index";
    String regionPath = "regionPath";

    Set<DistributedMember> members = new HashSet<>();
    DistributedMember mockMember = mock(DistributedMember.class);
    when(mockMember.getId()).thenReturn("member0");
    members.add(mockMember);

    final ResultCollector mockResultCollector = mock(ResultCollector.class);
    final List<CliFunctionResult> cliFunctionResults = new ArrayList<>();
    String expectedStatus;
    if (expectedToSucceed) {
      expectedStatus = CliStrings.format(
          LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEX_0_FROM_REGION_1,
          new Object[] {indexName, regionPath});
      cliFunctionResults.add(new CliFunctionResult(mockMember.getId()));
    } else {
      Exception e = new IllegalStateException("failed");
      expectedStatus = e.getMessage();
      cliFunctionResults.add(new CliFunctionResult("member0", e, e.getMessage()));
    }

    doReturn(mockResultCollector).when(commands).executeFunction(
        isA(LuceneDestroyIndexFunction.class), any(LuceneDestroyIndexInfo.class), any());
    doReturn(cliFunctionResults).when(mockResultCollector).getResult();

    doReturn(members).when(commands).getNormalMembers(any());
    doReturn(members).when(commands).getRegionMembers(any(), any());

    CommandResult result = (CommandResult) commands.destroyIndex(indexName, regionPath);
    verifyDestroyIndexCommandResult(result, cliFunctionResults, expectedStatus);
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroyAllIndexesNoRegionMembers(boolean expectedToSucceed) throws Exception {
    LuceneIndexCommands commands = createTestLuceneIndexCommandsForDestroyIndex();
    String indexName = null;
    String regionPath = "regionPath";

    final ResultCollector mockResultCollector = mock(ResultCollector.class);
    final List<CliFunctionResult> cliFunctionResults = new ArrayList<>();
    String expectedStatus;
    if (expectedToSucceed) {
      expectedStatus = CliStrings.format(
          LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEXES_FROM_REGION_0,
          new Object[] {regionPath});
      cliFunctionResults.add(new CliFunctionResult("member0"));
    } else {
      Exception e = new IllegalStateException("failed");
      expectedStatus = e.getMessage();
      cliFunctionResults.add(new CliFunctionResult("member0", e, e.getMessage()));
    }

    doReturn(mockResultCollector).when(commands).executeFunction(
        isA(LuceneDestroyIndexFunction.class), any(LuceneDestroyIndexInfo.class), any());
    doReturn(cliFunctionResults).when(mockResultCollector).getResult();

    doReturn(Collections.emptySet()).when(commands).getNormalMembers(any());
    doReturn(Collections.emptySet()).when(commands).getRegionMembers(any(), any());

    CommandResult result = (CommandResult) commands.destroyIndex(indexName, regionPath);
    verifyDestroyIndexCommandResult(result, cliFunctionResults, expectedStatus);
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroyAllIndexesWithRegionMembers(boolean expectedToSucceed) throws Exception {
    LuceneIndexCommands commands = createTestLuceneIndexCommandsForDestroyIndex();
    String indexName = null;
    String regionPath = "regionPath";

    Set<DistributedMember> members = new HashSet<>();
    DistributedMember mockMember = mock(DistributedMember.class);
    when(mockMember.getId()).thenReturn("member0");
    members.add(mockMember);

    final ResultCollector mockResultCollector = mock(ResultCollector.class);
    final List<CliFunctionResult> cliFunctionResults = new ArrayList<>();
    String expectedStatus;
    if (expectedToSucceed) {
      expectedStatus = CliStrings.format(
          LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEXES_FROM_REGION_0,
          new Object[] {regionPath});
      cliFunctionResults.add(new CliFunctionResult(mockMember.getId()));
    } else {
      Exception e = new IllegalStateException("failed");
      expectedStatus = e.getMessage();
      cliFunctionResults.add(new CliFunctionResult("member0", e, e.getMessage()));
    }

    doReturn(mockResultCollector).when(commands).executeFunction(
        isA(LuceneDestroyIndexFunction.class), any(LuceneDestroyIndexInfo.class), any());
    doReturn(cliFunctionResults).when(mockResultCollector).getResult();

    doReturn(Collections.emptySet()).when(commands).getNormalMembers(any());
    doReturn(Collections.emptySet()).when(commands).getRegionMembers(any(), any());

    CommandResult result = (CommandResult) commands.destroyIndex(indexName, regionPath);
    verifyDestroyIndexCommandResult(result, cliFunctionResults, expectedStatus);
  }

  private LuceneIndexCommands createTestLuceneIndexCommandsForDestroyIndex() {
    final ResultCollector mockResultCollector = mock(ResultCollector.class);
    final LuceneIndexCommands commands = spy(createIndexCommands(this.mockCache, null));

    final List<CliFunctionResult> cliFunctionResults = new ArrayList<>();
    cliFunctionResults.add(new CliFunctionResult("member", true, "Index Destroyed"));

    doReturn(mockResultCollector).when(commands).executeFunctionOnRegion(
        isA(LuceneDestroyIndexFunction.class), any(LuceneIndexInfo.class), eq(false));
    doReturn(cliFunctionResults).when(mockResultCollector).getResult();
    return commands;
  }

  private void verifyDestroyIndexCommandResult(CommandResult result,
      List<CliFunctionResult> cliFunctionResults, String expectedStatus) {
    assertEquals(Status.OK, result.getStatus());
    TabularResultData data = (TabularResultData) result.getResultData();
    List<String> members = data.retrieveAllValues("Member");
    assertEquals(cliFunctionResults.size(), members.size());
    // Verify each member
    for (int i = 0; i < members.size(); i++) {
      assertEquals("member" + i, members.get(i));
    }
    // Verify each status
    List<String> status = data.retrieveAllValues("Status");
    for (String statu : status) {
      assertEquals(expectedStatus, statu);
    }
  }

  private String getPage(final LuceneSearchResults[] expectedResults, int[] indexList) {
    final TabularResultData data = ResultBuilder.createTabularResultData();
    for (int i : indexList) {
      data.accumulate("key", expectedResults[i].getKey());
      data.accumulate("value", expectedResults[i].getValue());
      data.accumulate("score", expectedResults[i].getScore());
    }
    CommandResult commandResult = (CommandResult) ResultBuilder.buildResult(data);
    StringBuilder buffer = new StringBuilder();
    while (commandResult.hasNextLine()) {
      buffer.append(commandResult.nextLine());
    }
    return buffer.toString();
  }

  private List<Set<LuceneSearchResults>> getSearchResults(LuceneSearchResults... results) {
    final List<Set<LuceneSearchResults>> queryResultsList = new ArrayList<>();
    HashSet<LuceneSearchResults> queryResults = new HashSet<>();
    Collections.addAll(queryResults, results);
    queryResultsList.add(queryResults);
    return queryResultsList;
  }

  private LuceneIndexStats getMockIndexStats(int queries, int commits, int updates, int docs) {
    LuceneIndexStats mockIndexStats = mock(LuceneIndexStats.class);
    when(mockIndexStats.getQueryExecutions()).thenReturn(queries);
    when(mockIndexStats.getCommits()).thenReturn(commits);
    when(mockIndexStats.getUpdates()).thenReturn(updates);
    when(mockIndexStats.getDocuments()).thenReturn(docs);
    return mockIndexStats;
  }

  private LuceneIndexCommands createIndexCommands(final InternalCache cache,
      final Execution functionExecutor) {
    return new LuceneTestIndexCommands(cache, functionExecutor);
  }

  private LuceneIndexDetails createIndexDetails(final String indexName, final String regionPath,
      final String[] searchableFields, final Map<String, Analyzer> fieldAnalyzers,
      LuceneIndexStats indexStats, boolean status, final String serverName) {
    return new LuceneIndexDetails(indexName, regionPath, searchableFields, fieldAnalyzers,
        indexStats, status, serverName);
  }

  private LuceneIndexDetails createIndexDetails(final String indexName, final String regionPath,
      final String[] searchableFields, final Map<String, Analyzer> fieldAnalyzers, boolean status,
      final String serverName) {
    return new LuceneIndexDetails(indexName, regionPath, searchableFields, fieldAnalyzers, null,
        status, serverName);
  }

  private LuceneSearchResults createQueryResults(final String key, final String value,
      final float score) {
    return new LuceneSearchResults(key, value, score);
  }

  private static class LuceneTestIndexCommands extends LuceneIndexCommands {

    private final InternalCache cache;
    private final Execution functionExecutor;

    protected LuceneTestIndexCommands(final InternalCache cache, final Execution functionExecutor) {
      assert cache != null : "The InternalCache cannot be null!";
      this.cache = cache;
      this.functionExecutor = functionExecutor;
    }

    @Override
    public InternalCache getCache() {
      return this.cache;
    }

    @Override
    public Set<DistributedMember> getMembers(final InternalCache cache) {
      assertSame(getCache(), cache);
      return Collections.emptySet();
    }

    @Override
    public Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
      Assert.assertNotNull(members);
      return this.functionExecutor;
    }
  }

}
