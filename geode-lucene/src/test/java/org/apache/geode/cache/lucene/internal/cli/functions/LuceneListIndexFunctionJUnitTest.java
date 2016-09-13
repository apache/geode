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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.internal.InternalLuceneService;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexImpl;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.cache.lucene.internal.cli.LuceneIndexDetails;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.fake.Fakes;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)

public class LuceneListIndexFunctionJUnitTest {
  
  @Test
  @SuppressWarnings("unchecked")
  public void testExecute() throws Throwable {
    GemFireCacheImpl cache = Fakes.cache();
    LuceneServiceImpl service = mock(LuceneServiceImpl.class);
    when(cache.getService(InternalLuceneService.class)).thenReturn(service);
    
    FunctionContext context = mock(FunctionContext.class);
    ResultSender resultSender = mock(ResultSender.class);
    when(context.getResultSender()).thenReturn(resultSender);

    LuceneIndexImpl index1 = getMockLuceneIndex("index1");
    LuceneIndexImpl index2 = getMockLuceneIndex("index2");

    TreeSet expectedResult = new TreeSet();
    expectedResult.add(new LuceneIndexDetails(index1));
    expectedResult.add(new LuceneIndexDetails(index2));

    ArrayList<LuceneIndex> allIndexes = new ArrayList();
    allIndexes.add(index1);
    allIndexes.add(index2);
    when(service.getAllIndexes()).thenReturn(allIndexes);
    
    LuceneListIndexFunction function = new LuceneListIndexFunction();
    function = spy(function);
    Mockito.doReturn(cache).when(function).getCache();
    function.execute(context);
    
    ArgumentCaptor<Set> resultCaptor  = ArgumentCaptor.forClass(Set.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    Set<String> result = resultCaptor.getValue();

    assertEquals(2, result.size());
    assertEquals(expectedResult, result);
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
