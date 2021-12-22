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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.cli.LuceneDestroyIndexInfo;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneDestroyIndexFunctionJUnitTest {

  private LuceneServiceImpl service;
  private GemFireCacheImpl cache;
  private String member;
  private FunctionContext context;
  private ResultSender resultSender;

  @Before
  public void prepare() {
    cache = Fakes.cache();
    member = Fakes.distributedSystem().getDistributedMember().getId();
    service = mock(LuceneServiceImpl.class);
    when(cache.getService(InternalLuceneService.class)).thenReturn(service);
    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
  }

  @Test
  public void testDestroyIndex() throws Throwable {
    String indexName = "index1";
    String regionPath = SEPARATOR + "region1";
    LuceneDestroyIndexInfo indexInfo = new LuceneDestroyIndexInfo(indexName, regionPath, false);
    when(context.getArguments()).thenReturn(indexInfo);
    LuceneDestroyIndexFunction function = new LuceneDestroyIndexFunction();
    function = spy(function);
    function.execute(context);
    verify(service).destroyIndex(eq(indexName), eq(regionPath));
    verify(function).getXmlEntity(eq(indexName), eq(regionPath));
    verify(service, never()).destroyDefinedIndex(eq(indexName), eq(regionPath));
    verify(service, never()).destroyIndexes(eq(regionPath));
    verifyFunctionResult(true);
  }

  @Test
  public void testDestroyIndexFailure() throws Throwable {
    String indexName = "index1";
    String regionPath = SEPARATOR + "region1";
    LuceneDestroyIndexInfo indexInfo = new LuceneDestroyIndexInfo(indexName, regionPath, false);
    when(context.getArguments()).thenReturn(indexInfo);
    LuceneDestroyIndexFunction function = new LuceneDestroyIndexFunction();
    doThrow(new IllegalStateException()).when(service).destroyIndex(eq(indexName),
        eq(regionPath));
    function.execute(context);
    verifyFunctionResult(false);
  }

  @Test
  public void testDestroyDefinedIndex() throws Throwable {
    String indexName = "index1";
    String regionPath = SEPARATOR + "region1";
    LuceneDestroyIndexInfo indexInfo = new LuceneDestroyIndexInfo(indexName, regionPath, true);
    when(context.getArguments()).thenReturn(indexInfo);
    LuceneDestroyIndexFunction function = new LuceneDestroyIndexFunction();
    function = spy(function);
    function.execute(context);
    verify(service).destroyDefinedIndex(eq(indexName), eq(regionPath));
    verify(service, never()).destroyIndex(eq(indexName), eq(regionPath));
    verify(service, never()).destroyIndexes(eq(regionPath));
    verify(function, never()).getXmlEntity(eq(indexName), eq(regionPath));
    verifyFunctionResult(true);
  }

  @Test
  public void testDestroyDefinedIndexFailure() throws Throwable {
    String indexName = "index1";
    String regionPath = SEPARATOR + "region1";
    LuceneDestroyIndexInfo indexInfo = new LuceneDestroyIndexInfo(indexName, regionPath, true);
    when(context.getArguments()).thenReturn(indexInfo);
    LuceneDestroyIndexFunction function = new LuceneDestroyIndexFunction();
    doThrow(new IllegalStateException()).when(service).destroyDefinedIndex(eq(indexName),
        eq(regionPath));
    function.execute(context);
    verifyFunctionResult(false);
  }

  @Test
  public void testDestroyIndexes() throws Throwable {
    String regionPath = SEPARATOR + "region1";
    LuceneDestroyIndexInfo indexInfo = new LuceneDestroyIndexInfo(null, regionPath, false);
    when(context.getArguments()).thenReturn(indexInfo);
    LuceneDestroyIndexFunction function = new LuceneDestroyIndexFunction();
    function = spy(function);
    function.execute(context);
    verify(service).destroyIndexes(eq(regionPath));
    verify(service).destroyDefinedIndexes(eq(regionPath));
    verify(function).getXmlEntity(eq(null), eq(regionPath));
    verify(service, never()).destroyIndex(any(), eq(regionPath));
    verifyFunctionResult(true);
  }

  @Test
  public void testDestroyIndexesFailure() throws Throwable {
    String regionPath = SEPARATOR + "region1";
    LuceneDestroyIndexInfo indexInfo = new LuceneDestroyIndexInfo(null, regionPath, false);
    when(context.getArguments()).thenReturn(indexInfo);
    LuceneDestroyIndexFunction function = new LuceneDestroyIndexFunction();
    doThrow(new IllegalStateException()).when(service).destroyIndexes(eq(regionPath));
    doThrow(new IllegalStateException()).when(service).destroyDefinedIndexes(eq(regionPath));
    function.execute(context);
    verifyFunctionResult(false);
  }

  @Test
  public void testDestroyDefinedIndexes() throws Throwable {
    String regionPath = SEPARATOR + "region1";
    LuceneDestroyIndexInfo indexInfo = new LuceneDestroyIndexInfo(null, regionPath, true);
    when(context.getArguments()).thenReturn(indexInfo);
    LuceneDestroyIndexFunction function = new LuceneDestroyIndexFunction();
    function = spy(function);
    function.execute(context);
    verify(service).destroyDefinedIndexes(eq(regionPath));
    verify(service, never()).destroyIndexes(eq(regionPath));
    verify(service, never()).destroyIndex(any(), eq(regionPath));
    verify(function, never()).getXmlEntity(eq("index1"), eq(regionPath));
    verifyFunctionResult(true);
  }

  @Test
  public void testDestroyDefinedIndexesFailure() throws Throwable {
    String regionPath = SEPARATOR + "region1";
    LuceneDestroyIndexInfo indexInfo = new LuceneDestroyIndexInfo(null, regionPath, true);
    when(context.getArguments()).thenReturn(indexInfo);
    LuceneDestroyIndexFunction function = new LuceneDestroyIndexFunction();
    doThrow(new IllegalStateException()).when(service).destroyDefinedIndexes(eq(regionPath));
    function.execute(context);
    verifyFunctionResult(false);
  }

  @Test
  public void getXmlEntity() throws Exception {
    LuceneDestroyIndexFunction function = new LuceneDestroyIndexFunction();
    XmlEntity entity1 = function.getXmlEntity("index", SEPARATOR + "region");
    XmlEntity entity2 = function.getXmlEntity("index", "region");
    assertThat(entity1).isEqualTo(entity2);
    assertThat(entity1.getSearchString()).isEqualTo(entity2.getSearchString());
  }

  private void verifyFunctionResult(boolean result) {
    ArgumentCaptor<CliFunctionResult> resultCaptor =
        ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult functionResult = resultCaptor.getValue();
    assertEquals(member, functionResult.getMemberIdOrName());
    assertEquals(result, functionResult.isSuccessful());
  }
}
