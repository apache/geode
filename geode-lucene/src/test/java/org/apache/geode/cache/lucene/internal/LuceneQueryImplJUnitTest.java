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

package org.apache.geode.cache.lucene.internal;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneQueryProvider;
import org.apache.geode.cache.lucene.LuceneResultStruct;
import org.apache.geode.cache.lucene.PageableLuceneQueryResults;
import org.apache.geode.cache.lucene.internal.distributed.EntryScore;
import org.apache.geode.cache.lucene.internal.distributed.LuceneFunctionContext;
import org.apache.geode.cache.lucene.internal.distributed.LuceneQueryFunction;
import org.apache.geode.cache.lucene.internal.distributed.TopEntries;
import org.apache.geode.cache.lucene.internal.distributed.TopEntriesCollector;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneQueryImplJUnitTest {
  private static int LIMIT = 123;
  private LuceneQueryImpl<Object, Object> query;
  private Execution execution;
  private LuceneQueryProvider provider;
  private ResultCollector<TopEntriesCollector, TopEntries> collector;
  private Region region;
  private PageableLuceneQueryResults<Object, Object> results;
  private Cache cache;
  private CacheTransactionManager cacheTransactionManager;


  @Before
  public void createMocks() {
    region = mock(Region.class);
    execution = mock(Execution.class);
    collector = mock(ResultCollector.class);
    provider = mock(LuceneQueryProvider.class);
    cache = mock(Cache.class);
    cacheTransactionManager = mock(CacheTransactionManager.class);
    when(region.getCache()).thenReturn(cache);
    when(region.getCache().getCacheTransactionManager()).thenReturn(cacheTransactionManager);
    when(region.getCache().getCacheTransactionManager().exists()).thenReturn(false);
    when(execution.setArguments(any())).thenReturn(execution);
    when(execution.withCollector(any())).thenReturn(execution);
    when(execution.execute(anyString())).thenReturn((ResultCollector) collector);
    results = mock(PageableLuceneQueryResults.class);

    query = new LuceneQueryImpl<Object, Object>("index", region, provider, LIMIT, 20) {

      @Override
      protected Execution onRegion() {
        return execution;
      }

      @Override
      protected PageableLuceneQueryResults<Object, Object> newPageableResults(final int pageSize,
          final TopEntries<Object> entries) {
        return results;
      }
    };
  }

  private void addValueToResults() {
    TopEntries entries = new TopEntries();
    entries.addHit(new EntryScore("hi", 5));
    when(collector.getResult()).thenReturn(entries);

    when(results.getMaxScore()).thenReturn(5f);
    when(results.size()).thenReturn(1);
    List<LuceneResultStruct<Object, Object>> page =
        Collections.singletonList(new LuceneResultStructImpl<>("hi", "value", 5f));
    when(results.next()).thenReturn(page);
    when(results.hasNext()).thenReturn(true);
  }

  @Test
  public void shouldReturnKeysFromFindKeys() throws LuceneQueryException {
    addValueToResults();
    Collection<Object> results = query.findKeys();
    assertEquals(Collections.singletonList("hi"), results);
  }

  @Test
  public void shouldReturnEmptyListFromFindKeysWithNoResults() throws LuceneQueryException {
    TopEntries entries = new TopEntries();
    when(collector.getResult()).thenReturn(entries);
    Collection<Object> results = query.findKeys();
    assertEquals(Collections.emptyList(), results);
  }

  @Test
  public void shouldReturnValuesFromFindValues() throws LuceneQueryException {
    addValueToResults();
    Collection<Object> results = query.findValues();
    assertEquals(Collections.singletonList("value"), results);
  }

  @Test
  public void shouldReturnEmptyListFromFindValuesWithNoResults() throws LuceneQueryException {
    TopEntries entries = new TopEntries();
    when(collector.getResult()).thenReturn(entries);
    Collection<Object> results = query.findValues();
    assertEquals(Collections.emptyList(), results);
  }

  @Test
  public void shouldReturnLuceneResultStructFromFindResults() throws LuceneQueryException {
    addValueToResults();
    List<LuceneResultStruct<String, String>> result = new ArrayList<>();
    result.add(new LuceneResultStructImpl("hi", "value", 5));
    assertEquals(result, query.findResults());
  }

  @Test
  public void shouldInvokeLuceneFunctionWithCorrectArguments() throws Exception {
    addValueToResults();
    PageableLuceneQueryResults<Object, Object> results = query.findPages();

    verify(execution).execute(eq(LuceneQueryFunction.ID));
    ArgumentCaptor<LuceneFunctionContext> captor =
        ArgumentCaptor.forClass(LuceneFunctionContext.class);
    verify(execution).setArguments(captor.capture());
    LuceneFunctionContext context = captor.getValue();
    assertEquals(LIMIT, context.getLimit());
    assertEquals(provider, context.getQueryProvider());
    assertEquals("index", context.getIndexName());

    assertEquals(5, results.getMaxScore(), 0.01);
    assertEquals(1, results.size());
    final List<LuceneResultStruct<Object, Object>> page = results.next();
    assertEquals(1, page.size());
    LuceneResultStruct element = page.iterator().next();
    assertEquals("hi", element.getKey());
    assertEquals("value", element.getValue());
    assertEquals(5, element.getScore(), 0.01);
  }

}
