/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.distributed;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.DEFAULT_FIELD;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneQueryFactory;
import org.apache.geode.cache.lucene.LuceneQueryProvider;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.StringQueryProvider;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexResultCollector;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalRegionFunctionContext;
import org.apache.geode.test.junit.categories.UnitTest;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.Query;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@Category(UnitTest.class)
public class LuceneFunctionJUnitTest {

  String regionPath = "/region";
  String indexName = "index";
  final EntryScore<String> r1_1 = new EntryScore<String>("key-1-1", .5f);
  final EntryScore<String> r1_2 = new EntryScore<String>("key-1-2", .4f);
  final EntryScore<String> r1_3 = new EntryScore<String>("key-1-3", .3f);
  final EntryScore<String> r2_1 = new EntryScore<String>("key-2-1", .45f);
  final EntryScore<String> r2_2 = new EntryScore<String>("key-2-2", .35f);

  InternalRegionFunctionContext mockContext;
  ResultSender<TopEntriesCollector> mockResultSender;
  Region<Object, Object> mockRegion;

  RepositoryManager mockRepoManager;
  IndexRepository mockRepository1;
  IndexRepository mockRepository2;
  IndexResultCollector mockCollector;
  InternalLuceneService mockService;
  LuceneIndexImpl mockIndex;

  ArrayList<IndexRepository> repos;
  LuceneFunctionContext<IndexResultCollector> searchArgs;
  LuceneQueryProvider queryProvider;
  Query query;

  private InternalCache mockCache;

  @Test
  public void testRepoQueryAndMerge() throws Exception {
    when(mockContext.getDataSet()).thenReturn(mockRegion);
    when(mockContext.getArguments()).thenReturn(searchArgs);
    when(mockContext.<TopEntriesCollector>getResultSender()).thenReturn(mockResultSender);
    when(mockRepoManager.getRepositories(eq(mockContext))).thenReturn(repos);
    doAnswer(invocation -> {
      IndexResultCollector collector = invocation.getArgumentAt(2, IndexResultCollector.class);
      collector.collect(r1_1.getKey(), r1_1.getScore());
      collector.collect(r1_2.getKey(), r1_2.getScore());
      collector.collect(r1_3.getKey(), r1_3.getScore());
      return null;
    }).when(mockRepository1).query(eq(query), eq(LuceneQueryFactory.DEFAULT_LIMIT),
        any(IndexResultCollector.class));

    doAnswer(invocation -> {
      IndexResultCollector collector = invocation.getArgumentAt(2, IndexResultCollector.class);
      collector.collect(r2_1.getKey(), r2_1.getScore());
      collector.collect(r2_2.getKey(), r2_2.getScore());
      return null;
    }).when(mockRepository2).query(eq(query), eq(LuceneQueryFactory.DEFAULT_LIMIT),
        any(IndexResultCollector.class));

    LuceneFunction function = new LuceneFunction();

    function.execute(mockContext);

    ArgumentCaptor<TopEntriesCollector> resultCaptor =
        ArgumentCaptor.forClass(TopEntriesCollector.class);
    verify(mockResultSender).lastResult(resultCaptor.capture());
    TopEntriesCollector result = resultCaptor.getValue();


    List<EntryScore> hits = result.getEntries().getHits();
    assertEquals(5, hits.size());
    TopEntriesJUnitTest.verifyResultOrder(result.getEntries().getHits(), r1_1, r2_1, r1_2, r2_2,
        r1_3);
  }

  @Test
  public void testResultLimitClause() throws Exception {

    searchArgs =
        new LuceneFunctionContext<IndexResultCollector>(queryProvider, "indexName", null, 3);
    when(mockContext.getDataSet()).thenReturn(mockRegion);
    when(mockContext.getArguments()).thenReturn(searchArgs);
    when(mockContext.<TopEntriesCollector>getResultSender()).thenReturn(mockResultSender);
    when(mockRepoManager.getRepositories(eq(mockContext))).thenReturn(repos);

    doAnswer(invocation -> {
      IndexResultCollector collector = invocation.getArgumentAt(2, IndexResultCollector.class);
      collector.collect(r1_1.getKey(), r1_1.getScore());
      collector.collect(r1_2.getKey(), r1_2.getScore());
      collector.collect(r1_3.getKey(), r1_3.getScore());
      return null;
    }).when(mockRepository1).query(eq(query), eq(3), any(IndexResultCollector.class));

    doAnswer(invocation -> {
      IndexResultCollector collector = invocation.getArgumentAt(2, IndexResultCollector.class);
      collector.collect(r2_1.getKey(), r2_1.getScore());
      collector.collect(r2_2.getKey(), r2_2.getScore());
      return null;
    }).when(mockRepository2).query(eq(query), eq(3), any(IndexResultCollector.class));


    LuceneFunction function = new LuceneFunction();

    function.execute(mockContext);

    ArgumentCaptor<TopEntriesCollector> resultCaptor =
        ArgumentCaptor.forClass(TopEntriesCollector.class);
    verify(mockResultSender).lastResult(resultCaptor.capture());
    TopEntriesCollector result = resultCaptor.getValue();

    List<EntryScore> hits = result.getEntries().getHits();
    assertEquals(3, hits.size());
    TopEntriesJUnitTest.verifyResultOrder(result.getEntries().getHits(), r1_1, r2_1, r1_2);
  }

  @Test
  public void injectCustomCollectorManager() throws Exception {
    final CollectorManager mockManager = mock(CollectorManager.class);
    searchArgs =
        new LuceneFunctionContext<IndexResultCollector>(queryProvider, "indexName", mockManager);
    when(mockContext.getDataSet()).thenReturn(mockRegion);
    when(mockContext.getArguments()).thenReturn(searchArgs);
    when(mockContext.<TopEntriesCollector>getResultSender()).thenReturn(mockResultSender);
    repos.remove(0);
    when(mockRepoManager.getRepositories(eq(mockContext))).thenReturn(repos);
    when(mockManager.newCollector(eq("repo2"))).thenReturn(mockCollector);
    when(mockManager.reduce(any(Collection.class))).thenAnswer(invocation -> {
      Collection<IndexResultCollector> collectors = invocation.getArgumentAt(0, Collection.class);
      assertEquals(1, collectors.size());
      assertEquals(mockCollector, collectors.iterator().next());
      return new TopEntriesCollector(null);

    });

    doAnswer(invocation -> {
      IndexResultCollector collector = invocation.getArgumentAt(2, IndexResultCollector.class);
      collector.collect(r2_1.getKey(), r2_1.getScore());
      return null;
    }).when(mockRepository2).query(eq(query), eq(LuceneQueryFactory.DEFAULT_LIMIT),
        any(IndexResultCollector.class));


    LuceneFunction function = new LuceneFunction();

    function.execute(mockContext);

    verify(mockCollector).collect(eq("key-2-1"), eq(.45f));
    verify(mockResultSender).lastResult(any(TopEntriesCollector.class));
  }

  @Test(expected = FunctionException.class)
  public void testIndexRepoQueryFails() throws Exception {
    when(mockContext.getDataSet()).thenReturn(mockRegion);
    when(mockContext.getArguments()).thenReturn(searchArgs);
    when(mockContext.<TopEntriesCollector>getResultSender()).thenReturn(mockResultSender);
    when(mockRepoManager.getRepositories(eq(mockContext))).thenReturn(repos);
    doThrow(IOException.class).when(mockRepository1).query(eq(query),
        eq(LuceneQueryFactory.DEFAULT_LIMIT), any(IndexResultCollector.class));

    LuceneFunction function = new LuceneFunction();

    function.execute(mockContext);
  }

  @Test(expected = FunctionException.class)
  public void testBucketNotFound() throws Exception {
    when(mockContext.getDataSet()).thenReturn(mockRegion);
    when(mockContext.getArguments()).thenReturn(searchArgs);
    when(mockContext.<TopEntriesCollector>getResultSender()).thenReturn(mockResultSender);
    when(mockRepoManager.getRepositories(eq(mockContext)))
        .thenThrow(new BucketNotFoundException(""));
    LuceneFunction function = new LuceneFunction();

    function.execute(mockContext);

    verify(mockResultSender).sendException(any(BucketNotFoundException.class));
  }

  @Test(expected = FunctionException.class)
  public void testReduceError() throws Exception {
    final CollectorManager mockManager = mock(CollectorManager.class);
    searchArgs =
        new LuceneFunctionContext<IndexResultCollector>(queryProvider, "indexName", mockManager);

    when(mockContext.getDataSet()).thenReturn(mockRegion);
    when(mockContext.getArguments()).thenReturn(searchArgs);
    when(mockContext.<TopEntriesCollector>getResultSender()).thenReturn(mockResultSender);
    repos.remove(1);
    when(mockRepoManager.getRepositories(eq(mockContext))).thenReturn(repos);
    when(mockManager.newCollector(eq("repo1"))).thenReturn(mockCollector);
    when(mockManager.reduce(any(Collection.class))).thenThrow(IOException.class);

    LuceneFunction function = new LuceneFunction();

    function.execute(mockContext);
  }

  @Test(expected = FunctionException.class)
  public void queryProviderErrorIsHandled() throws Exception {
    queryProvider = mock(LuceneQueryProvider.class);
    searchArgs = new LuceneFunctionContext<IndexResultCollector>(queryProvider, "indexName");
    when(mockContext.getDataSet()).thenReturn(mockRegion);
    when(mockContext.getArguments()).thenReturn(searchArgs);
    when(mockContext.<TopEntriesCollector>getResultSender()).thenReturn(mockResultSender);
    when(queryProvider.getQuery(eq(mockIndex))).thenThrow(LuceneQueryException.class);
    LuceneFunction function = new LuceneFunction();

    function.execute(mockContext);
  }

  @Test
  public void testQueryFunctionId() {
    String id = new LuceneFunction().getId();
    assertEquals(LuceneFunction.class.getName(), id);
  }

  @Before
  public void createMocksAndCommonObjects() throws Exception {
    mockContext = mock(InternalRegionFunctionContext.class);
    mockResultSender = mock(ResultSender.class);
    mockRegion = mock(Region.class);

    mockRepoManager = mock(RepositoryManager.class);
    mockRepository1 = mock(IndexRepository.class, "repo1");
    mockRepository2 = mock(IndexRepository.class, "repo2");
    mockCollector = mock(IndexResultCollector.class);

    repos = new ArrayList<IndexRepository>();
    repos.add(mockRepository1);
    repos.add(mockRepository2);

    mockIndex = mock(LuceneIndexImpl.class);
    mockService = mock(InternalLuceneService.class);
    mockCache = mock(InternalCache.class);
    Analyzer analyzer = new StandardAnalyzer();
    Mockito.doReturn(analyzer).when(mockIndex).getAnalyzer();
    queryProvider = new StringQueryProvider("gemfire:lucene", DEFAULT_FIELD);

    searchArgs = new LuceneFunctionContext<IndexResultCollector>(queryProvider, "indexName");

    when(mockRegion.getCache()).thenReturn(mockCache);
    when(mockRegion.getFullPath()).thenReturn(regionPath);
    when(mockCache.getService(any())).thenReturn(mockService);
    when(mockService.getIndex(eq("indexName"), eq(regionPath))).thenReturn(mockIndex);
    when(mockIndex.getRepositoryManager()).thenReturn(mockRepoManager);
    when(mockIndex.getFieldNames()).thenReturn(new String[] {"gemfire"});

    query = queryProvider.getQuery(mockIndex);
  }
}
