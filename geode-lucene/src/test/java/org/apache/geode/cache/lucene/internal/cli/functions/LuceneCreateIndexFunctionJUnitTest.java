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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.LuceneIndexFactoryImpl;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.cache.lucene.internal.repository.serializer.PrimitiveSerializer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})

public class LuceneCreateIndexFunctionJUnitTest {

  private InternalLuceneService service;
  private GemFireCacheImpl cache;
  String member;
  FunctionContext context;
  ResultSender resultSender;
  CliFunctionResult expectedResult;
  private LuceneIndexFactoryImpl factory;

  @Before
  public void prepare() {
    cache = Fakes.cache();
    DistributedSystem ds = Fakes.distributedSystem();
    member = ds.getDistributedMember().getId();
    service = mock(InternalLuceneService.class);
    when(cache.getService(InternalLuceneService.class)).thenReturn(service);
    factory = mock(LuceneIndexFactoryImpl.class);
    when(service.createIndexFactory()).thenReturn(factory);

    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);

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
    String[] analyzers = new String[3];
    analyzerNames.toArray(analyzers);
    LuceneIndexInfo indexInfo = new LuceneIndexInfo("index1", SEPARATOR + "region1",
        new String[] {"field1", "field2", "field3"}, analyzers, null);
    when(context.getArguments()).thenReturn(indexInfo);

    LuceneCreateIndexFunction function = new LuceneCreateIndexFunction();
    function.execute(context);

    ArgumentCaptor<Map> analyzersCaptor = ArgumentCaptor.forClass(Map.class);
    verify(service).createIndexFactory();
    verify(factory).addField(eq("field1"), isA(StandardAnalyzer.class));
    verify(factory).addField(eq("field2"), isA(KeywordAnalyzer.class));
    verify(factory).addField(eq("field3"), isA(StandardAnalyzer.class));
    verify(factory).create(eq("index1"), eq(SEPARATOR + "region1"), eq(false));

    ArgumentCaptor<Set> resultCaptor = ArgumentCaptor.forClass(Set.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = (CliFunctionResult) resultCaptor.getValue();

    assertEquals(expectedResult, result);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteWithoutAnalyzer() throws Throwable {
    String[] fields = new String[] {"field1", "field2", "field3"};
    LuceneIndexInfo indexInfo =
        new LuceneIndexInfo("index1", SEPARATOR + "region1", fields, null, null);
    when(context.getArguments()).thenReturn(indexInfo);

    LuceneCreateIndexFunction function = new LuceneCreateIndexFunction();
    function.execute(context);

    verify(factory).addField(eq("field1"));
    verify(factory).addField(eq("field2"));
    verify(factory).addField(eq("field3"));
    verify(factory).create(eq("index1"), eq(SEPARATOR + "region1"), eq(false));

    ArgumentCaptor<Set> resultCaptor = ArgumentCaptor.forClass(Set.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = (CliFunctionResult) resultCaptor.getValue();

    assertEquals(expectedResult, result);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteWithSerializer() throws Throwable {
    String[] fields = new String[] {"field1", "field2", "field3"};
    LuceneIndexInfo indexInfo = new LuceneIndexInfo("index1", SEPARATOR + "region1", fields, null,
        PrimitiveSerializer.class.getCanonicalName());
    when(context.getArguments()).thenReturn(indexInfo);

    LuceneCreateIndexFunction function = new LuceneCreateIndexFunction();
    function.execute(context);

    verify(factory).addField(eq("field1"));
    verify(factory).addField(eq("field2"));
    verify(factory).addField(eq("field3"));
    verify(factory).setLuceneSerializer(isA(PrimitiveSerializer.class));
    verify(factory).create(eq("index1"), eq(SEPARATOR + "region1"), eq(false));

    ArgumentCaptor<Set> resultCaptor = ArgumentCaptor.forClass(Set.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = (CliFunctionResult) resultCaptor.getValue();

    assertEquals(expectedResult, result);
  }
}
