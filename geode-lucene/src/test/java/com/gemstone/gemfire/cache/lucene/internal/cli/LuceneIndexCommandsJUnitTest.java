/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.lucene.internal.cli;
import static org.junit.Assert.*;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexStats;
import com.gemstone.gemfire.cache.lucene.internal.cli.functions.LuceneCreateIndexFunction;
import com.gemstone.gemfire.cache.lucene.internal.cli.functions.LuceneDescribeIndexFunction;
import com.gemstone.gemfire.cache.lucene.internal.cli.functions.LuceneListIndexFunction;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.util.CollectionUtils;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The LuceneIndexCommandsJUnitTest class is a test suite of test cases testing the contract and functionality of the
 * LuceneIndexCommands class.
 * </p>
 * @see LuceneIndexCommands
 * @see LuceneIndexDetails
 * @see com.gemstone.gemfire.cache.lucene.internal.cli.functions.LuceneListIndexFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.jmock.lib.legacy.ClassImposteriser
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class LuceneIndexCommandsJUnitTest {

  @Test
  public void testListIndexWithoutStats() {

    final Cache mockCache = mock(Cache.class, "Cache");
    final AbstractExecution mockFunctionExecutor = mock(AbstractExecution.class, "Function Executor");
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");

    String[] searchableFields={"field1","field2","field3"};
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new StandardAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    fieldAnalyzers.put("field3", null);
    final LuceneIndexDetails indexDetails1 = createIndexDetails("memberFive", "/Employees", searchableFields, fieldAnalyzers, true);
    final LuceneIndexDetails indexDetails2 = createIndexDetails("memberSix", "/Employees", searchableFields, fieldAnalyzers, false);
    final LuceneIndexDetails indexDetails3 = createIndexDetails("memberTen", "/Employees", searchableFields, fieldAnalyzers, true);

    final List<Set<LuceneIndexDetails>> results = new ArrayList<>();

    results.add(CollectionUtils.asSet(indexDetails2, indexDetails1,indexDetails3));


    when(mockFunctionExecutor.execute(isA(LuceneListIndexFunction.class)))
      .thenReturn(mockResultCollector);
    when(mockResultCollector.getResult())
      .thenReturn(results);

    final LuceneIndexCommands commands = createIndexCommands(mockCache, mockFunctionExecutor);

    CommandResult result = (CommandResult) commands.listIndex(false);
    TabularResultData data = (TabularResultData) result.getResultData();
    assertEquals(Arrays.asList("memberFive", "memberSix", "memberTen"), data.retrieveAllValues("Index Name"));
    assertEquals(Arrays.asList("/Employees", "/Employees", "/Employees"), data.retrieveAllValues("Region Path"));
    assertEquals(Arrays.asList("[field1, field2, field3]", "[field1, field2, field3]", "[field1, field2, field3]"), data.retrieveAllValues("Indexed Fields"));
    assertEquals(Arrays.asList("{field1=StandardAnalyzer, field2=KeywordAnalyzer}", "{field1=StandardAnalyzer, field2=KeywordAnalyzer}", "{field1=StandardAnalyzer, field2=KeywordAnalyzer}"), data.retrieveAllValues("Field Analyzer"));
    assertEquals(Arrays.asList("Initialized", "Defined", "Initialized"), data.retrieveAllValues("Status"));
  }

  @Test
  public void testListIndexWithStats() {

    final Cache mockCache = mock(Cache.class, "Cache");
    final AbstractExecution mockFunctionExecutor = mock(AbstractExecution.class, "Function Executor");
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");
    final LuceneIndexStats mockIndexStats1=getMockIndexStats(1,10,5,1);
    final LuceneIndexStats mockIndexStats2=getMockIndexStats(2,20,10,2);
    final LuceneIndexStats mockIndexStats3=getMockIndexStats(3,30,15,3);
    String[] searchableFields={"field1","field2","field3"};
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new StandardAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    fieldAnalyzers.put("field3", null);
    final LuceneIndexDetails indexDetails1 = createIndexDetails("memberFive", "/Employees", searchableFields, fieldAnalyzers,mockIndexStats1, true);
    final LuceneIndexDetails indexDetails2 = createIndexDetails("memberSix", "/Employees", searchableFields, fieldAnalyzers,mockIndexStats2, true);
    final LuceneIndexDetails indexDetails3 = createIndexDetails("memberTen", "/Employees", searchableFields, fieldAnalyzers,mockIndexStats3, true);

    final List<Set<LuceneIndexDetails>> results = new ArrayList<>();

    results.add(CollectionUtils.asSet(indexDetails2, indexDetails1,indexDetails3));

    when(mockFunctionExecutor.execute(isA(LuceneListIndexFunction.class)))
      .thenReturn(mockResultCollector);
    when(mockResultCollector.getResult())
      .thenReturn(results);

    final LuceneIndexCommands commands = createIndexCommands(mockCache, mockFunctionExecutor);

    CommandResult result = (CommandResult) commands.listIndex(true);
    TabularResultData data = (TabularResultData) result.getResultData();
    assertEquals(Arrays.asList("memberFive", "memberSix", "memberTen"), data.retrieveAllValues("Index Name"));
    assertEquals(Arrays.asList("/Employees", "/Employees", "/Employees"), data.retrieveAllValues("Region Path"));
    assertEquals(Arrays.asList("[field1, field2, field3]", "[field1, field2, field3]", "[field1, field2, field3]"), data.retrieveAllValues("Indexed Fields"));
    assertEquals(Arrays.asList("{field1=StandardAnalyzer, field2=KeywordAnalyzer}", "{field1=StandardAnalyzer, field2=KeywordAnalyzer}", "{field1=StandardAnalyzer, field2=KeywordAnalyzer}"), data.retrieveAllValues("Field Analyzer"));
    assertEquals(Arrays.asList("1","2","3"), data.retrieveAllValues("Query Executions"));
    assertEquals(Arrays.asList("10","20","30"), data.retrieveAllValues("Commits"));
    assertEquals(Arrays.asList("5","10","15"), data.retrieveAllValues("Updates"));
    assertEquals(Arrays.asList("1","2","3"), data.retrieveAllValues("Documents"));
  }

  @Test
  public void testCreateIndex() throws Exception {
    final Cache mockCache=mock(Cache.class);
    final ResultCollector mockResultCollector = mock(ResultCollector.class);
    final LuceneIndexCommands commands=spy(createIndexCommands(mockCache,null));

    final List<CliFunctionResult> cliFunctionResults=new ArrayList<>();
    cliFunctionResults.add(new CliFunctionResult("member1",true,"Index Created"));
    cliFunctionResults.add(new CliFunctionResult("member2",false,"Index creation failed"));
    cliFunctionResults.add(new CliFunctionResult("member3",true,"Index Created"));

    doReturn(mockResultCollector).when(commands).executeFunctionOnGroups(isA(LuceneCreateIndexFunction.class),any(),any(LuceneIndexInfo.class));
    doReturn(cliFunctionResults).when(mockResultCollector).getResult();

    String indexName ="index";
    String regionPath="regionPath";
    String[] searchableFields={"field1","field2","field3"};
    String[] fieldAnalyzers = { StandardAnalyzer.class.getCanonicalName(), KeywordAnalyzer.class.getCanonicalName(), StandardAnalyzer.class.getCanonicalName()};

    CommandResult result=(CommandResult) commands.createIndex(indexName,regionPath,searchableFields,fieldAnalyzers,null);
    assertEquals(Status.OK,result.getStatus());
    TabularResultData data = (TabularResultData) result.getResultData();
    assertEquals(Arrays.asList("member1","member2","member3"), data.retrieveAllValues("Member"));
    assertEquals(Arrays.asList("Successfully created lucene index","Failed: Index creation failed","Successfully created lucene index"), data.retrieveAllValues("Status"));

  }

  @Test
  public void testDescribeIndex() throws Exception {

    final Cache mockCache = mock(Cache.class, "Cache");
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");
    final LuceneIndexCommands commands=spy(createIndexCommands(mockCache,null));

    String[] searchableFields={"field1","field2","field3"};
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new StandardAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    fieldAnalyzers.put("field3", null);
    final LuceneIndexStats mockIndexStats=getMockIndexStats(1,10,5,1);
    final List<LuceneIndexDetails> indexDetails = new ArrayList<>();
    indexDetails.add(createIndexDetails("memberFive", "/Employees", searchableFields, fieldAnalyzers,mockIndexStats, true));

    doReturn(mockResultCollector).when(commands).executeFunctionOnGroups(isA(LuceneDescribeIndexFunction.class),any(),any(LuceneIndexInfo.class));
    doReturn(indexDetails).when(mockResultCollector).getResult();

    CommandResult result = (CommandResult) commands.describeIndex("memberFive","/Employees");

    TabularResultData data = (TabularResultData) result.getResultData();
    assertEquals(Collections.singletonList("memberFive"), data.retrieveAllValues("Index Name"));
    assertEquals(Collections.singletonList("/Employees"), data.retrieveAllValues("Region Path"));
    assertEquals(Collections.singletonList("[field1, field2, field3]"), data.retrieveAllValues("Indexed Fields"));
    assertEquals(Collections.singletonList("{field1=StandardAnalyzer, field2=KeywordAnalyzer}"), data.retrieveAllValues("Field Analyzer"));
    assertEquals(Collections.singletonList("Initialized"), data.retrieveAllValues("Status"));
    assertEquals(Collections.singletonList("1"), data.retrieveAllValues("Query Executions"));
    assertEquals(Collections.singletonList("10"), data.retrieveAllValues("Commits"));
    assertEquals(Collections.singletonList("5"), data.retrieveAllValues("Updates"));
    assertEquals(Collections.singletonList("1"), data.retrieveAllValues("Documents"));
  }

  @Test
  public void testSearchIndex() throws Exception {

    final Cache mockCache = mock(Cache.class, "Cache");
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");
    final LuceneIndexCommands commands=spy(createIndexCommands(mockCache,null));

    final List<Set<LuceneSearchResults>> queryResultsList = new ArrayList<>();
    HashSet<LuceneSearchResults> queryResults = new HashSet<>();
    queryResults.add(createQueryResults("A","Result1",Float.valueOf("1.3")));
    queryResults.add(createQueryResults("B","Result1",Float.valueOf("1.2")));
    queryResults.add(createQueryResults("C","Result1",Float.valueOf("1.1")));
    queryResultsList.add(queryResults);
    doReturn(mockResultCollector).when(commands).executeSearch(isA(LuceneQueryInfo.class));
    doReturn(queryResultsList).when(mockResultCollector).getResult();

    CommandResult result = (CommandResult) commands.searchIndex("index","region","Result1","field1",-1,-1, false);

    TabularResultData data = (TabularResultData) result.getResultData();

    assertEquals(Arrays.asList("C","B","A"), data.retrieveAllValues("key"));
    assertEquals(Arrays.asList("Result1","Result1","Result1"), data.retrieveAllValues("value"));
    assertEquals(Arrays.asList("1.1","1.2","1.3"), data.retrieveAllValues("score"));
  }

  @Test
  public void testSearchIndexWithPaging() throws Exception {
    final Cache mockCache = mock(Cache.class, "Cache");
    final Gfsh mockGfsh = mock(Gfsh.class);
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");
    final LuceneIndexCommands commands=spy(createIndexCommands(mockCache,null));
    ArgumentCaptor<String> resultCaptor  = ArgumentCaptor.forClass(String.class);

    LuceneSearchResults result1=createQueryResults("A","Result1",Float.valueOf("1.7"));
    LuceneSearchResults result2=createQueryResults("B","Result1",Float.valueOf("1.6"));
    LuceneSearchResults result3=createQueryResults("C","Result1",Float.valueOf("1.5"));
    LuceneSearchResults result4=createQueryResults("D","Result1",Float.valueOf("1.4"));
    LuceneSearchResults result5=createQueryResults("E","Result1",Float.valueOf("1.3"));
    LuceneSearchResults result6=createQueryResults("F","Result1",Float.valueOf("1.2"));
    LuceneSearchResults result7=createQueryResults("G","Result1",Float.valueOf("1.1"));
    final List<Set<LuceneSearchResults>> queryResultsList = getSearchResults(result1, result2, result3, result4, result5, result6, result7);

    doReturn(mockResultCollector).when(commands).executeSearch(any(LuceneQueryInfo.class));
    doReturn(queryResultsList).when(mockResultCollector).getResult();
    doReturn(mockGfsh).when(commands).initGfsh();
    when(mockGfsh.interact(anyString())).thenReturn("n").thenReturn("n").thenReturn("n").thenReturn("n")
      .thenReturn("p").thenReturn("p").thenReturn("p").thenReturn("p").thenReturn("p").thenReturn("n").thenReturn("q");

    LuceneSearchResults[] expectedResults = new LuceneSearchResults[] {result7,result6,result5,result4,result3,result2,result1};
    String expectedPage1 = getPage(expectedResults, new int[] {0,1});
    String expectedPage2 = getPage(expectedResults, new int[] {2,3});
    String expectedPage3 = getPage(expectedResults, new int[] {4,5});
    String expectedPage4 = getPage(expectedResults, new int[] {6});

    commands.searchIndex("index","region","Result1","field1",-1,2,false);
    verify(mockGfsh, times(20)).printAsInfo(resultCaptor.capture());
    List<String> actualPageResults=resultCaptor.getAllValues();

    assertEquals(expectedPage1,actualPageResults.get(0));
    assertEquals("\t\tPage 1 of 4",actualPageResults.get(1));

    assertEquals(expectedPage2,actualPageResults.get(2));
    assertEquals("\t\tPage 2 of 4",actualPageResults.get(3));

    assertEquals(expectedPage3,actualPageResults.get(4));
    assertEquals("\t\tPage 3 of 4",actualPageResults.get(5));

    assertEquals(expectedPage4,actualPageResults.get(6));
    assertEquals("\t\tPage 4 of 4",actualPageResults.get(7));

    assertEquals("No more results to display.", actualPageResults.get(8));

    assertEquals(expectedPage4,actualPageResults.get(9));
    assertEquals("\t\tPage 4 of 4",actualPageResults.get(10));

    assertEquals(expectedPage3,actualPageResults.get(11));
    assertEquals("\t\tPage 3 of 4",actualPageResults.get(12));

    assertEquals(expectedPage2,actualPageResults.get(13));
    assertEquals("\t\tPage 2 of 4",actualPageResults.get(14));

    assertEquals(expectedPage1,actualPageResults.get(15));
    assertEquals("\t\tPage 1 of 4",actualPageResults.get(16));

    assertEquals("At the top of the search results.", actualPageResults.get(17));

    assertEquals(expectedPage1,actualPageResults.get(18));
    assertEquals("\t\tPage 1 of 4",actualPageResults.get(19));

  }

  @Test
  public void testSearchIndexWithKeysOnly() throws Exception {

    final Cache mockCache = mock(Cache.class, "Cache");
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");
    final LuceneIndexCommands commands=spy(createIndexCommands(mockCache,null));

    final List<Set<LuceneSearchResults>> queryResultsList = new ArrayList<>();
    HashSet<LuceneSearchResults> queryResults = new HashSet<>();
    queryResults.add(createQueryResults("A","Result1",Float.valueOf("1.3")));
    queryResults.add(createQueryResults("B","Result1",Float.valueOf("1.2")));
    queryResults.add(createQueryResults("C","Result1",Float.valueOf("1.1")));
    queryResultsList.add(queryResults);
    doReturn(mockResultCollector).when(commands).executeSearch(isA(LuceneQueryInfo.class));
    doReturn(queryResultsList).when(mockResultCollector).getResult();

    CommandResult result = (CommandResult) commands.searchIndex("index","region","Result1","field1",-1,-1, true);

    TabularResultData data = (TabularResultData) result.getResultData();

    assertEquals(Arrays.asList("C","B","A"), data.retrieveAllValues("key"));
  }

  @Test
  public void testSearchIndexWhenSearchResultsHaveSameScore() throws Exception {

    final Cache mockCache = mock(Cache.class, "Cache");
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");
    final LuceneIndexCommands commands=spy(createIndexCommands(mockCache,null));

    final List<Set<LuceneSearchResults>> queryResultsList = new ArrayList<>();
    HashSet<LuceneSearchResults> queryResults = new HashSet<>();
    queryResults.add(createQueryResults("A","Result1",1));
    queryResults.add(createQueryResults("B","Result1",1));
    queryResults.add(createQueryResults("C","Result1",1));
    queryResults.add(createQueryResults("D","Result1",1));
    queryResults.add(createQueryResults("E","Result1",1));
    queryResults.add(createQueryResults("F","Result1",1));
    queryResults.add(createQueryResults("G","Result1",1));
    queryResults.add(createQueryResults("H","Result1",1));
    queryResults.add(createQueryResults("I","Result1",1));
    queryResults.add(createQueryResults("J","Result1",1));
    queryResults.add(createQueryResults("K","Result1",1));
    queryResults.add(createQueryResults("L","Result1",1));
    queryResults.add(createQueryResults("M","Result1",1));
    queryResults.add(createQueryResults("N","Result1",1));
    queryResults.add(createQueryResults("P","Result1",1));
    queryResults.add(createQueryResults("Q","Result1",1));
    queryResults.add(createQueryResults("R","Result1",1));
    queryResults.add(createQueryResults("S","Result1",1));
    queryResults.add(createQueryResults("T","Result1",1));
    queryResultsList.add(queryResults);

    doReturn(mockResultCollector).when(commands).executeSearch(isA(LuceneQueryInfo.class));
    doReturn(queryResultsList).when(mockResultCollector).getResult();

    CommandResult result = (CommandResult) commands.searchIndex("index","region","Result1","field1",-1,-1, true);

    TabularResultData data = (TabularResultData) result.getResultData();

    assertEquals(queryResults.size(), data.retrieveAllValues("key").size());
  }

  private String getPage(final LuceneSearchResults[] expectedResults, int[] indexList) {
    final TabularResultData data = ResultBuilder.createTabularResultData();
    for (int i:indexList) {
      data.accumulate("key", expectedResults[i].getKey());
      data.accumulate("value", expectedResults[i].getValue());
      data.accumulate("score", expectedResults[i].getScore());
    }
    CommandResult commandResult = (CommandResult) ResultBuilder.buildResult(data);
    StringBuffer buffer = new StringBuffer();
    while (commandResult.hasNextLine())
      buffer.append(commandResult.nextLine());
    return buffer.toString();
  }

  private List<Set<LuceneSearchResults>> getSearchResults(LuceneSearchResults ... results)
  {
    final List<Set<LuceneSearchResults>> queryResultsList = new ArrayList<>();
    HashSet<LuceneSearchResults> queryResults = new HashSet<>();
    for(LuceneSearchResults result : results)
      queryResults.add(result);
    queryResultsList.add(queryResults);
    return queryResultsList;
  }


  private LuceneIndexStats getMockIndexStats(int queries, int commits, int updates, int docs) {
    LuceneIndexStats mockIndexStats=mock(LuceneIndexStats.class);
    when(mockIndexStats.getQueryExecutions())
      .thenReturn(queries);
    when(mockIndexStats.getCommits())
      .thenReturn(commits);
    when(mockIndexStats.getUpdates())
      .thenReturn(updates);
    when(mockIndexStats.getDocuments())
      .thenReturn(docs);
    return mockIndexStats;
  }

  private LuceneIndexCommands createIndexCommands(final Cache cache, final Execution functionExecutor) {
    return new LuceneTestIndexCommands(cache, functionExecutor);
  }

  private LuceneIndexDetails createIndexDetails(final String indexName, final String regionPath, final String[] searchableFields, final Map<String, Analyzer> fieldAnalyzers, LuceneIndexStats indexStats, boolean status) {
    return new LuceneIndexDetails(indexName, regionPath, searchableFields, fieldAnalyzers,indexStats, status);
  }

  private LuceneIndexDetails createIndexDetails(final String indexName, final String regionPath, final String[] searchableFields, final Map<String, Analyzer> fieldAnalyzers, boolean status) {
    return new LuceneIndexDetails(indexName, regionPath, searchableFields, fieldAnalyzers, null, status);
  }

  private LuceneSearchResults createQueryResults(final String key, final String value, final float score) {
    return new LuceneSearchResults(key,value,score);
  }

  private static class LuceneTestIndexCommands extends LuceneIndexCommands {

    private final Cache cache;
    private final Execution functionExecutor;

    protected LuceneTestIndexCommands(final Cache cache, final Execution functionExecutor) {
      assert cache != null : "The Cache cannot be null!";
      this.cache = cache;
      this.functionExecutor = functionExecutor;
    }

    @Override
    protected Cache getCache() {
      return this.cache;
    }

    @Override
    protected Set<DistributedMember> getMembers(final Cache cache) {
      assertSame(getCache(), cache);
      return Collections.emptySet();
    }

    @Override
    protected Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
      Assert.assertNotNull(members);
      return functionExecutor;
    }
  }

}
