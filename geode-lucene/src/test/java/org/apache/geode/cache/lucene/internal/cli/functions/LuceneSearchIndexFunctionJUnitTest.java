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
package org.apache.geode.cache.lucene.internal.cli.functions;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneQueryFactory;
import org.apache.geode.cache.lucene.LuceneResultStruct;
import org.apache.geode.cache.lucene.PageableLuceneQueryResults;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.LuceneResultStructImpl;
import org.apache.geode.cache.lucene.internal.cli.LuceneQueryInfo;
import org.apache.geode.cache.lucene.internal.cli.LuceneSearchResults;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})

public class LuceneSearchIndexFunctionJUnitTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testExecute() throws LuceneQueryException {
    FunctionContext context = mock(FunctionContext.class);
    ResultSender resultSender = mock(ResultSender.class);
    GemFireCacheImpl cache = Fakes.cache();

    LuceneQueryInfo queryInfo =
        createMockQueryInfo("index", "region", "field1:region1", "field1", 1);
    InternalLuceneService service = getMockLuceneService("A", "Value", "1.2");
    Region mockRegion = mock(Region.class);

    LuceneSearchIndexFunction function = new LuceneSearchIndexFunction();

    doReturn(queryInfo).when(context).getArguments();
    doReturn(resultSender).when(context).getResultSender();
    doReturn(cache).when(context).getCache();

    when(cache.getService(eq(InternalLuceneService.class))).thenReturn(service);
    when(cache.getRegion(queryInfo.getRegionPath())).thenReturn(mockRegion);

    function.execute(context);
    ArgumentCaptor<Set> resultCaptor = ArgumentCaptor.forClass(Set.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    Set<LuceneSearchResults> result = resultCaptor.getValue();

    assertEquals(1, result.size());
    for (LuceneSearchResults searchResult : result) {
      assertEquals("A", searchResult.getKey());
      assertEquals("Value", searchResult.getValue());
      assertEquals(1.2, searchResult.getScore(), .1);
    }
  }

  private InternalLuceneService getMockLuceneService(String resultKey, String resultValue,
      String resultScore) throws LuceneQueryException {
    InternalLuceneService service = mock(InternalLuceneService.class);
    LuceneQueryFactory mockQueryFactory = spy(LuceneQueryFactory.class);
    LuceneQuery mockQuery = mock(LuceneQuery.class);
    PageableLuceneQueryResults pageableLuceneQueryResults = mock(PageableLuceneQueryResults.class);
    LuceneResultStruct<String, String> resultStruct =
        new LuceneResultStructImpl(resultKey, resultValue, Float.parseFloat(resultScore));
    List<LuceneResultStruct<String, String>> queryResults = new ArrayList<>();
    queryResults.add(resultStruct);

    doReturn(mock(LuceneIndex.class)).when(service).getIndex(anyString(), anyString());
    doReturn(mockQueryFactory).when(service).createLuceneQueryFactory();
    doReturn(mockQueryFactory).when(mockQueryFactory).setLimit(anyInt());
    doReturn(mockQuery).when(mockQueryFactory).create(any(), any(), any(), any());
    when(mockQuery.findPages()).thenReturn(pageableLuceneQueryResults);
    when(pageableLuceneQueryResults.hasNext()).thenReturn(true).thenReturn(false);
    when(pageableLuceneQueryResults.next()).thenReturn(queryResults);

    return service;
  }

  private LuceneQueryInfo createMockQueryInfo(final String index, final String region,
      final String query, final String field, final int limit) {
    LuceneQueryInfo queryInfo = mock(LuceneQueryInfo.class);
    when(queryInfo.getIndexName()).thenReturn(index);
    when(queryInfo.getRegionPath()).thenReturn(region);
    when(queryInfo.getQueryString()).thenReturn(query);
    when(queryInfo.getDefaultField()).thenReturn(field);
    when(queryInfo.getLimit()).thenReturn(limit);
    return queryInfo;
  }

}
