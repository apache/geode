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
package org.apache.geode.cache.lucene.internal.cli.functions;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexDetails;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@Category(UnitTest.class)

public class LuceneDescribeIndexFunctionJUnitTest {
  
  @Test
  @SuppressWarnings("unchecked")
  public void testExecute() throws Throwable {
    GemFireCacheImpl cache = Fakes.cache();
    LuceneServiceImpl service = mock(LuceneServiceImpl.class);
    when(cache.getService(InternalLuceneService.class)).thenReturn(service);
    FunctionContext context = mock(FunctionContext.class);
    ResultSender resultSender = mock(ResultSender.class);
    LuceneIndexInfo indexInfo=getMockLuceneInfo("index1");
    LuceneIndexImpl index1 = getMockLuceneIndex("index1");
    LuceneDescribeIndexFunction function = spy(LuceneDescribeIndexFunction.class);

    doReturn(indexInfo).when(context).getArguments();
    doReturn(resultSender).when(context).getResultSender();
    doReturn(cache).when(function).getCache();
    when(service.getIndex(indexInfo.getIndexName(),indexInfo.getRegionPath())).thenReturn(index1);

    function.execute(context);
    ArgumentCaptor<LuceneIndexDetails> resultCaptor  = ArgumentCaptor.forClass(LuceneIndexDetails.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    LuceneIndexDetails result = resultCaptor.getValue();
    LuceneIndexDetails expected=new LuceneIndexDetails(index1);

    assertEquals(expected.getIndexName(),result.getIndexName());
    assertEquals(expected.getRegionPath(),result.getRegionPath());
    assertEquals(expected.getIndexStats(),result.getIndexStats());
    assertEquals(expected.getFieldAnalyzersString(),result.getFieldAnalyzersString());
    assertEquals(expected.getSearchableFieldNamesString(),result.getSearchableFieldNamesString());
  }

  private LuceneIndexInfo getMockLuceneInfo(final String index1) {
    LuceneIndexInfo mockInfo=mock(LuceneIndexInfo.class);
    doReturn(index1).when(mockInfo).getIndexName();
    doReturn("/region").when(mockInfo).getRegionPath();
    return mockInfo;
  }

  private LuceneIndexImpl getMockLuceneIndex(final String indexName)
  {
    String[] searchableFields={"field1","field2"};
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new StandardAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());

    LuceneIndexImpl index = mock(LuceneIndexImpl.class);
    when(index.getName()).thenReturn(indexName);
    when(index.getRegionPath()).thenReturn("/region");
    when(index.getFieldNames()).thenReturn(searchableFields);
    when(index.getFieldAnalyzers()).thenReturn(fieldAnalyzers);
    return index;
  }

}
