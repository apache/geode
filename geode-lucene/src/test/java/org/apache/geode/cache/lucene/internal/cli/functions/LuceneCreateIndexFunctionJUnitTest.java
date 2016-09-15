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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)

public class LuceneCreateIndexFunctionJUnitTest {
  
  private InternalLuceneService service;
  private GemFireCacheImpl cache;
  String member;
  FunctionContext context;
  ResultSender resultSender;
  CliFunctionResult expectedResult;
  
  @Before
  public void prepare() {
    cache = Fakes.cache();
    DistributedSystem ds = Fakes.distributedSystem();
    member = ds.getDistributedMember().getId();
    service = mock(InternalLuceneService.class);
    when(cache.getService(InternalLuceneService.class)).thenReturn(service);
    doNothing().when(service).createIndex(anyString(), anyString(), anyMap());
    
    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    when(context.getResultSender()).thenReturn(resultSender);

    XmlEntity xmlEntity = null;
    expectedResult = new CliFunctionResult(member, xmlEntity);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteWithAnalyzer() throws Throwable {
    List<String> analyzerNames = new ArrayList<>();
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());
    analyzerNames.add(KeywordAnalyzer.class.getCanonicalName());
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());
    String [] analyzers = new String[3];
    analyzerNames.toArray(analyzers);
    LuceneIndexInfo indexInfo = new LuceneIndexInfo("index1", "/region1", 
        new String[] {"field1", "field2", "field3"}, analyzers);
    when(context.getArguments()).thenReturn(indexInfo);

    LuceneCreateIndexFunction function = new LuceneCreateIndexFunction();
    function = spy(function);
    doReturn(cache).when(function).getCache();
    function.execute(context);
    
    ArgumentCaptor<Map> analyzersCaptor = ArgumentCaptor.forClass(Map.class);
    verify(service).createIndex(eq("index1"), eq("/region1"), analyzersCaptor.capture());
    Map<String, Analyzer> analyzerPerField = analyzersCaptor.getValue();
    assertEquals(3, analyzerPerField.size());
    assertTrue(analyzerPerField.get("field1") instanceof StandardAnalyzer);
    assertTrue(analyzerPerField.get("field2") instanceof KeywordAnalyzer);
    assertTrue(analyzerPerField.get("field3") instanceof StandardAnalyzer);
    
    ArgumentCaptor<Set> resultCaptor  = ArgumentCaptor.forClass(Set.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = (CliFunctionResult)resultCaptor.getValue();

    assertEquals(expectedResult, result);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteWithoutAnalyzer() throws Throwable {
    String fields[] = new String[] {"field1", "field2", "field3"};
    LuceneIndexInfo indexInfo = new LuceneIndexInfo("index1", "/region1", fields, null);
    when(context.getArguments()).thenReturn(indexInfo);

    LuceneCreateIndexFunction function = new LuceneCreateIndexFunction();
    function = spy(function);
    doReturn(cache).when(function).getCache();
    function.execute(context);
    
    verify(service).createIndex(eq("index1"), eq("/region1"), eq("field1"), eq("field2"), eq("field3"));

    ArgumentCaptor<Set> resultCaptor  = ArgumentCaptor.forClass(Set.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = (CliFunctionResult)resultCaptor.getValue();

    assertEquals(expectedResult, result);
  }
}
