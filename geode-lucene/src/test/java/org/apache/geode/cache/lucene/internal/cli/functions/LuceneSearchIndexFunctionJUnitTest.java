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
package com.gemstone.gemfire.cache.lucene.internal.cli.functions;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryException;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.LuceneResultStruct;
import com.gemstone.gemfire.cache.lucene.PageableLuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.internal.InternalLuceneService;
import com.gemstone.gemfire.cache.lucene.internal.LuceneResultStructImpl;
import com.gemstone.gemfire.cache.lucene.internal.cli.LuceneQueryInfo;
import com.gemstone.gemfire.cache.lucene.internal.cli.LuceneSearchResults;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.fake.Fakes;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

@Category(UnitTest.class)

public class LuceneSearchIndexFunctionJUnitTest {
  
  @Test
  @SuppressWarnings("unchecked")
  public void testExecute() throws LuceneQueryException {
    FunctionContext context = mock(FunctionContext.class);
    ResultSender resultSender = mock(ResultSender.class);
    GemFireCacheImpl cache = Fakes.cache();

    LuceneQueryInfo queryInfo = createMockQueryInfo("index","region","field1:region1","field1",1);
    InternalLuceneService service = getMockLuceneService("A","Value","1.2");
    Region mockRegion=mock(Region.class);

    LuceneSearchIndexFunction function = spy(LuceneSearchIndexFunction.class);

    doReturn(queryInfo).when(context).getArguments();
    doReturn(resultSender).when(context).getResultSender();
    doReturn(cache).when(function).getCache();

    when(cache.getService(eq(InternalLuceneService.class))).thenReturn(service);
    when(cache.getRegion(queryInfo.getRegionPath())).thenReturn(mockRegion);

    function.execute(context);
    ArgumentCaptor<Set> resultCaptor  = ArgumentCaptor.forClass(Set.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    Set<LuceneSearchResults> result = resultCaptor.getValue();

    assertEquals(1,result.size());
    for (LuceneSearchResults searchResult: result) {
      assertEquals("A",searchResult.getKey());
      assertEquals("Value",searchResult.getValue());
      assertEquals(1.2,searchResult.getScore(),.1);
    }
  }
  private InternalLuceneService getMockLuceneService(String resultKey, String resultValue, String resultScore) throws LuceneQueryException{
    InternalLuceneService service=mock(InternalLuceneService.class);
    LuceneQueryFactory mockQueryFactory = spy(LuceneQueryFactory.class);
    LuceneQuery mockQuery=mock(LuceneQuery.class);
    PageableLuceneQueryResults pageableLuceneQueryResults = mock(PageableLuceneQueryResults.class);
    LuceneResultStruct<String,String> resultStruct = new LuceneResultStructImpl(resultKey,resultValue,Float.valueOf(resultScore));
    List<LuceneResultStruct<String,String>> queryResults= new ArrayList<>();
    queryResults.add(resultStruct);

    doReturn(mock(LuceneIndex.class)).when(service).getIndex(anyString(),anyString());
    doReturn(mockQueryFactory).when(service).createLuceneQueryFactory();
    doReturn(mockQueryFactory).when(mockQueryFactory).setResultLimit(anyInt());
    doReturn(mockQuery).when(mockQueryFactory).create(any(),any(),any(),any());
    when(mockQuery.findPages()).thenReturn(pageableLuceneQueryResults);
    when(pageableLuceneQueryResults.hasNext()).thenReturn(true).thenReturn(false);
    when(pageableLuceneQueryResults.next()).thenReturn(queryResults);

    return service;
  }

  private LuceneQueryInfo createMockQueryInfo(final String index, final String region, final String query, final String field, final int limit) {
    LuceneQueryInfo queryInfo = mock(LuceneQueryInfo.class);
    when(queryInfo.getIndexName()).thenReturn(index);
    when(queryInfo.getRegionPath()).thenReturn(region);
    when(queryInfo.getQueryString()).thenReturn(query);
    when(queryInfo.getDefaultField()).thenReturn(field);
    when(queryInfo.getLimit()).thenReturn(limit);
    return queryInfo;
  }

}
