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

import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;
import static org.apache.geode.management.internal.cli.result.ResultData.TYPE_TABULAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneCreateIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneDescribeIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneDestroyIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneListIndexFunction;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.cache.lucene.internal.repository.serializer.PrimitiveSerializer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.Version;
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
import org.apache.geode.test.junit.categories.LuceneTest;

/**
 * The LuceneIndexCommandsJUnitTest class is a test suite of test cases testing the contract and
 * functionality of the LuceneIndexCommands class.
 *
 * @see LuceneIndexCommands
 * @see LuceneIndexDetails
 * @see LuceneListIndexFunction
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(LuceneTest.class)
@RunWith(JUnitParamsRunner.class)
public class LuceneIndexCommandsJUnitTest {

  private InternalCache mockCache;

  @Before
  public void before() {
    this.mockCache = mock(InternalCache.class, "InternalCache");
    when(this.mockCache.getSecurityService()).thenReturn(mock(SecurityService.class));
  }

  @Test
  public void testListIndexWithoutStats() {
    final String serverName = "mockServer";
    final AbstractExecution mockFunctionExecutor =
        mock(AbstractExecution.class, "Function Executor");
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");

    String[] searchableFields = {"field1", "field2", "field3"};
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new StandardAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    fieldAnalyzers.put("field3", null);
    LuceneSerializer serializer = new HeterogeneousLuceneSerializer();
    final LuceneIndexDetails indexDetails1 = createIndexDetails("memberFive", "/Employees",
        searchableFields, fieldAnalyzers, LuceneIndexStatus.INITIALIZED, serverName, serializer);
    final LuceneIndexDetails indexDetails2 =
        createIndexDetails("memberSix", "/Employees", searchableFields, fieldAnalyzers,
            LuceneIndexStatus.NOT_INITIALIZED, serverName, serializer);
    final LuceneIndexDetails indexDetails3 = createIndexDetails("memberTen", "/Employees",
        searchableFields, fieldAnalyzers, LuceneIndexStatus.INITIALIZED, serverName, serializer);

    final List<Set<LuceneIndexDetails>> results = new ArrayList<>();

    results.add(CollectionUtils.asSet(indexDetails2, indexDetails1, indexDetails3));

    when(mockFunctionExecutor.execute(any(LuceneListIndexFunction.class)))
        .thenReturn(mockResultCollector);
    when(mockResultCollector.getResult()).thenReturn(results);

    final LuceneIndexCommands commands = createIndexCommands(this.mockCache, mockFunctionExecutor);

    CommandResult result = (CommandResult) commands.listIndex(false);
    TabularResultData data = (TabularResultData) result.getResultData();
    assertThat(data.retrieveAllValues("Index Name"))
        .isEqualTo(Arrays.asList("memberFive", "memberSix", "memberTen"));
    assertThat(data.retrieveAllValues("Region Path"))
        .isEqualTo(Arrays.asList("/Employees", "/Employees", "/Employees"));
    assertThat(data.retrieveAllValues("Indexed Fields")).isEqualTo(Arrays.asList(
        "[field1, field2, field3]", "[field1, field2, field3]", "[field1, field2, field3]"));
    assertThat(data.retrieveAllValues("Field Analyzer"))
        .isEqualTo(Arrays.asList("{field1=StandardAnalyzer, field2=KeywordAnalyzer}",
            "{field1=StandardAnalyzer, field2=KeywordAnalyzer}",
            "{field1=StandardAnalyzer, field2=KeywordAnalyzer}"));
    assertThat(data.retrieveAllValues("Status"))
        .isEqualTo(Arrays.asList("INITIALIZED", "NOT_INITIALIZED", "INITIALIZED"));
  }

  @Test
  public void testListIndexWithStats() {
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
    LuceneSerializer serializer = new HeterogeneousLuceneSerializer();
    final LuceneIndexDetails indexDetails1 =
        createIndexDetails("memberFive", "/Employees", searchableFields, fieldAnalyzers,
            mockIndexStats1, LuceneIndexStatus.INITIALIZED, serverName, serializer);
    final LuceneIndexDetails indexDetails2 =
        createIndexDetails("memberSix", "/Employees", searchableFields, fieldAnalyzers,
            mockIndexStats2, LuceneIndexStatus.INITIALIZED, serverName, serializer);
    final LuceneIndexDetails indexDetails3 =
        createIndexDetails("memberTen", "/Employees", searchableFields, fieldAnalyzers,
            mockIndexStats3, LuceneIndexStatus.INITIALIZED, serverName, serializer);

    final List<Set<LuceneIndexDetails>> results = new ArrayList<>();

    results.add(CollectionUtils.asSet(indexDetails2, indexDetails1, indexDetails3));

    when(mockFunctionExecutor.execute(any(LuceneListIndexFunction.class)))
        .thenReturn(mockResultCollector);
    when(mockResultCollector.getResult()).thenReturn(results);

    final LuceneIndexCommands commands = createIndexCommands(this.mockCache, mockFunctionExecutor);

    CommandResult result = (CommandResult) commands.listIndex(true);
    TabularResultData data = (TabularResultData) result.getResultData();
    assertThat(data.retrieveAllValues("Index Name"))
        .isEqualTo(Arrays.asList("memberFive", "memberSix", "memberTen"));
    assertThat(data.retrieveAllValues("Region Path"))
        .isEqualTo(Arrays.asList("/Employees", "/Employees", "/Employees"));
    assertThat(data.retrieveAllValues("Indexed Fields")).isEqualTo(Arrays.asList(
        "[field1, field2, field3]", "[field1, field2, field3]", "[field1, field2, field3]"));
    assertThat(data.retrieveAllValues("Field Analyzer"))
        .isEqualTo(Arrays.asList("{field1=StandardAnalyzer, field2=KeywordAnalyzer}",
            "{field1=StandardAnalyzer, field2=KeywordAnalyzer}",
            "{field1=StandardAnalyzer, field2=KeywordAnalyzer}"));
    assertThat(data.retrieveAllValues("Query Executions")).isEqualTo(Arrays.asList("1", "2", "3"));
    assertThat(data.retrieveAllValues("Commits")).isEqualTo(Arrays.asList("10", "20", "30"));
    assertThat(data.retrieveAllValues("Updates")).isEqualTo(Arrays.asList("5", "10", "15"));
    assertThat(data.retrieveAllValues("Documents")).isEqualTo(Arrays.asList("1", "2", "3"));
    assertThat(data.retrieveAllValues("Serializer"))
        .isEqualTo(Arrays.asList(HeterogeneousLuceneSerializer.class.getSimpleName(),
            HeterogeneousLuceneSerializer.class.getSimpleName(),
            HeterogeneousLuceneSerializer.class.getSimpleName()));
  }

  @Test
  public void testCreateIndex() throws Exception {
    final ResultCollector mockResultCollector = mock(ResultCollector.class);
    final LuceneIndexCommands commands = spy(createIndexCommands(this.mockCache, null));

    final List<CliFunctionResult> cliFunctionResults = new ArrayList<>();
    cliFunctionResults
        .add(new CliFunctionResult("member1", CliFunctionResult.StatusState.OK, "Index Created"));
    cliFunctionResults.add(new CliFunctionResult("member2", CliFunctionResult.StatusState.ERROR,
        "Index creation failed"));
    cliFunctionResults
        .add(new CliFunctionResult("member3", CliFunctionResult.StatusState.OK, "Index Created"));

    doReturn(mockResultCollector).when(commands).executeFunctionOnAllMembers(
        any(LuceneCreateIndexFunction.class), any(LuceneIndexInfo.class));
    doReturn(cliFunctionResults).when(mockResultCollector).getResult();

    String indexName = "index";
    String regionPath = "regionPath";
    String[] searchableFields = {"field1", "field2", "field3"};
    String[] fieldAnalyzers = {StandardAnalyzer.class.getCanonicalName(),
        KeywordAnalyzer.class.getCanonicalName(), StandardAnalyzer.class.getCanonicalName()};

    String serializer = PrimitiveSerializer.class.getCanonicalName();

    CommandResult result = (CommandResult) commands.createIndex(indexName, regionPath,
        searchableFields, fieldAnalyzers, serializer);
    assertThat(result.getStatus()).isEqualTo(Status.OK);
    TabularResultData data = (TabularResultData) result.getResultData();
    assertThat(data.retrieveAllValues("Member"))
        .isEqualTo(Arrays.asList("member1", "member2", "member3"));
    assertThat(data.retrieveAllValues("Status"))
        .isEqualTo(Arrays.asList("Successfully created lucene index",
            "Failed: Index creation failed", "Successfully created lucene index"));
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
    LuceneSerializer serializer = new HeterogeneousLuceneSerializer();
    indexDetails.add(createIndexDetails("memberFive", "/Employees", searchableFields,
        fieldAnalyzers, mockIndexStats, LuceneIndexStatus.INITIALIZED, serverName, serializer));

    doReturn(mockResultCollector).when(commands).executeFunctionOnRegion(
        any(LuceneDescribeIndexFunction.class), any(LuceneIndexInfo.class), eq(true));
    doReturn(indexDetails).when(mockResultCollector).getResult();

    CommandResult result = (CommandResult) commands.describeIndex("memberFive", "/Employees");

    TabularResultData data = (TabularResultData) result.getResultData();
    assertThat(data.retrieveAllValues("Index Name"))
        .isEqualTo(Collections.singletonList("memberFive"));
    assertThat(data.retrieveAllValues("Region Path"))
        .isEqualTo(Collections.singletonList("/Employees"));
    assertThat(data.retrieveAllValues("Indexed Fields"))
        .isEqualTo(Collections.singletonList("[field1, field2, field3]"));
    assertThat(data.retrieveAllValues("Field Analyzer"))
        .isEqualTo(Collections.singletonList("{field1=StandardAnalyzer, field2=KeywordAnalyzer}"));
    assertThat(data.retrieveAllValues("Status"))
        .isEqualTo(Collections.singletonList("INITIALIZED"));
    assertThat(data.retrieveAllValues("Query Executions"))
        .isEqualTo(Collections.singletonList("1"));
    assertThat(data.retrieveAllValues("Commits")).isEqualTo(Collections.singletonList("10"));
    assertThat(data.retrieveAllValues("Updates")).isEqualTo(Collections.singletonList("5"));
    assertThat(data.retrieveAllValues("Documents")).isEqualTo(Collections.singletonList("1"));
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
    doReturn(mockResultCollector).when(commands).executeSearch(any(LuceneQueryInfo.class));
    doReturn(queryResultsList).when(mockResultCollector).getResult();

    CommandResult result =
        (CommandResult) commands.searchIndex("index", "region", "Result1", "field1", -1, false);
    TabularResultData data = (TabularResultData) result.getResultData();
    assertThat(data.retrieveAllValues("key")).isEqualTo(Arrays.asList("A", "B", "C"));
    assertThat(data.retrieveAllValues("value"))
        .isEqualTo(Arrays.asList("Result1", "Result1", "Result1"));
    assertThat(data.retrieveAllValues("score")).isEqualTo(Arrays.asList("1.3", "1.2", "1.1"));
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

    assertThat(actualPageResults.get(0)).isEqualTo(expectedPage1);
    assertThat(actualPageResults.get(1)).isEqualTo("\t\tPage 1 of 4");

    assertThat(actualPageResults.get(2)).isEqualTo(expectedPage2);
    assertThat(actualPageResults.get(3)).isEqualTo("\t\tPage 2 of 4");

    assertThat(actualPageResults.get(4)).isEqualTo(expectedPage3);
    assertThat(actualPageResults.get(5)).isEqualTo("\t\tPage 3 of 4");

    assertThat(actualPageResults.get(6)).isEqualTo(expectedPage4);
    assertThat(actualPageResults.get(7)).isEqualTo("\t\tPage 4 of 4");

    assertThat(actualPageResults.get(8)).isEqualTo("No more results to display.");

    assertThat(actualPageResults.get(9)).isEqualTo(expectedPage4);
    assertThat(actualPageResults.get(10)).isEqualTo("\t\tPage 4 of 4");

    assertThat(actualPageResults.get(11)).isEqualTo(expectedPage3);
    assertThat(actualPageResults.get(12)).isEqualTo("\t\tPage 3 of 4");

    assertThat(actualPageResults.get(13)).isEqualTo(expectedPage2);
    assertThat(actualPageResults.get(14)).isEqualTo("\t\tPage 2 of 4");

    assertThat(actualPageResults.get(15)).isEqualTo(expectedPage1);
    assertThat(actualPageResults.get(16)).isEqualTo("\t\tPage 1 of 4");

    assertThat(actualPageResults.get(17)).isEqualTo("At the top of the search results.");

    assertThat(actualPageResults.get(18)).isEqualTo(expectedPage1);
    assertThat(actualPageResults.get(19)).isEqualTo("\t\tPage 1 of 4");
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
    doReturn(mockResultCollector).when(commands).executeSearch(any(LuceneQueryInfo.class));
    doReturn(queryResultsList).when(mockResultCollector).getResult();

    CommandResult result =
        (CommandResult) commands.searchIndex("index", "region", "Result1", "field1", -1, true);
    TabularResultData data = (TabularResultData) result.getResultData();
    assertThat(data.retrieveAllValues("key")).isEqualTo(Arrays.asList("A", "B", "C"));
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

    doReturn(mockResultCollector).when(commands).executeSearch(any(LuceneQueryInfo.class));
    doReturn(queryResultsList).when(mockResultCollector).getResult();

    CommandResult result =
        (CommandResult) commands.searchIndex("index", "region", "Result1", "field1", -1, true);
    TabularResultData data = (TabularResultData) result.getResultData();
    assertThat(data.retrieveAllValues("key").size()).isEqualTo(queryResults.size());
  }

  @Test
  public void testDestroySingleIndexNoRegionMembers() {
    LuceneIndexCommands commands = createTestLuceneIndexCommandsForDestroyIndex();
    final List<CliFunctionResult> cliFunctionResults = new ArrayList<>();
    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__COULD_NOT_FIND__MEMBERS_GREATER_THAN_VERSION_0,
        new Object[] {Version.GEODE_170}) + LINE_SEPARATOR;
    cliFunctionResults.add(new CliFunctionResult("member0", CliFunctionResult.StatusState.OK));
    doReturn(Collections.emptySet()).when(commands).getNormalMembersWithSameOrNewerVersion(any());
    CommandResult result = (CommandResult) commands.destroyIndex("index", "regionPath");
    verifyDestroyIndexCommandResult(result, cliFunctionResults, expectedStatus);
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroySingleIndexWithRegionMembers(boolean expectedToSucceed) {
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
          indexName, regionPath);
      cliFunctionResults
          .add(new CliFunctionResult(mockMember.getId(), CliFunctionResult.StatusState.OK));
    } else {
      Exception e = new IllegalStateException("failed");
      expectedStatus = e.getMessage();
      cliFunctionResults.add(new CliFunctionResult("member0", e, e.getMessage()));
    }

    doReturn(mockResultCollector).when(commands).executeFunction(
        any(LuceneDestroyIndexFunction.class), any(LuceneDestroyIndexInfo.class), anySet());
    doReturn(cliFunctionResults).when(mockResultCollector).getResult();

    doReturn(members).when(commands).getNormalMembersWithSameOrNewerVersion(any());

    CommandResult result = (CommandResult) commands.destroyIndex(indexName, regionPath);
    verifyDestroyIndexCommandResult(result, cliFunctionResults, expectedStatus);
  }

  @Test
  public void testDestroyAllIndexesNoRegionMembers() {
    LuceneIndexCommands commands = createTestLuceneIndexCommandsForDestroyIndex();
    doReturn(Collections.emptySet()).when(commands).getNormalMembersWithSameOrNewerVersion(any());
    final List<CliFunctionResult> cliFunctionResults = new ArrayList<>();
    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__COULD_NOT_FIND__MEMBERS_GREATER_THAN_VERSION_0,
        new Object[] {Version.GEODE_170}) + LINE_SEPARATOR;
    cliFunctionResults.add(new CliFunctionResult("member0", CliFunctionResult.StatusState.OK));
    CommandResult result = (CommandResult) commands.destroyIndex(null, "regionPath");
    verifyDestroyIndexCommandResult(result, cliFunctionResults, expectedStatus);
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroyAllIndexesWithRegionMembers(boolean expectedToSucceed) {
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
      cliFunctionResults
          .add(new CliFunctionResult(mockMember.getId(), CliFunctionResult.StatusState.OK));
    } else {
      Exception e = new IllegalStateException("failed");
      expectedStatus = e.getMessage();
      cliFunctionResults.add(new CliFunctionResult("member0", e, e.getMessage()));
    }

    doReturn(mockResultCollector).when(commands).executeFunction(
        any(LuceneDestroyIndexFunction.class), any(LuceneDestroyIndexInfo.class), anySet());
    doReturn(cliFunctionResults).when(mockResultCollector).getResult();

    doReturn(members).when(commands).getNormalMembersWithSameOrNewerVersion(any());

    CommandResult result = (CommandResult) commands.destroyIndex(indexName, regionPath);
    verifyDestroyIndexCommandResult(result, cliFunctionResults, expectedStatus);
  }

  private LuceneIndexCommands createTestLuceneIndexCommandsForDestroyIndex() {
    final ResultCollector mockResultCollector = mock(ResultCollector.class);
    final LuceneIndexCommands commands = spy(createIndexCommands(this.mockCache, null));

    final List<CliFunctionResult> cliFunctionResults = new ArrayList<>();
    cliFunctionResults
        .add(new CliFunctionResult("member", CliFunctionResult.StatusState.OK, "Index Destroyed"));

    doReturn(mockResultCollector).when(commands).executeFunctionOnRegion(
        any(LuceneDestroyIndexFunction.class), any(LuceneIndexInfo.class), eq(false));
    doReturn(cliFunctionResults).when(mockResultCollector).getResult();
    return commands;
  }

  private void verifyDestroyIndexCommandResult(CommandResult result,
      List<CliFunctionResult> cliFunctionResults, String expectedStatus) {
    assertThat(result.getStatus()).isEqualTo(Status.OK);
    if (result.getType().equals(TYPE_TABULAR)) {
      TabularResultData data = (TabularResultData) result.getResultData();
      List<String> members = data.retrieveAllValues("Member");
      assertThat(cliFunctionResults.size()).isEqualTo(members.size());
      // Verify each member
      for (int i = 0; i < members.size(); i++) {
        assertThat(members.get(i)).isEqualTo("member" + i);
      }
      // Verify each status
      List<String> status = data.retrieveAllValues("Status");
      for (String statu : status) {
        assertThat(statu).isEqualTo(expectedStatus);
      }
    } else {
      // Info result. Verify next lines are equal.
      assertThat(result.nextLine()).isEqualTo(expectedStatus);
    }
  }

  private String getPage(final LuceneSearchResults[] expectedResults, int[] indexList) {
    final TabularResultData data = ResultBuilder.createTabularResultData();
    for (int i : indexList) {
      data.accumulate("key", expectedResults[i].getKey());
      data.accumulate("value", expectedResults[i].getValue());
      data.accumulate("score", expectedResults[i].getScore());
    }
    CommandResult commandResult = ResultBuilder.buildResult(data);
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
      LuceneIndexStats indexStats, LuceneIndexStatus status, final String serverName,
      LuceneSerializer serializer) {
    return new LuceneIndexDetails(indexName, regionPath, searchableFields, fieldAnalyzers,
        indexStats, status, serverName, serializer);
  }

  private LuceneIndexDetails createIndexDetails(final String indexName, final String regionPath,
      final String[] searchableFields, final Map<String, Analyzer> fieldAnalyzers,
      LuceneIndexStatus status, final String serverName, LuceneSerializer serializer) {
    return new LuceneIndexDetails(indexName, regionPath, searchableFields, fieldAnalyzers, null,
        status, serverName, serializer);
  }

  private LuceneSearchResults createQueryResults(final String key, final String value,
      final float score) {
    return new LuceneSearchResults(key, value, score);
  }

  private static class LuceneTestIndexCommands extends LuceneIndexCommands {

    private final Execution functionExecutor;

    LuceneTestIndexCommands(final InternalCache cache, final Execution functionExecutor) {
      assert cache != null : "The InternalCache cannot be null!";
      setCache(cache);
      this.functionExecutor = functionExecutor;
    }

    @Override
    public Set<DistributedMember> getAllMembers() {
      return Collections.emptySet();
    }

    @Override
    public Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
      assertThat(members).isNotNull();
      return this.functionExecutor;
    }
  }

}
