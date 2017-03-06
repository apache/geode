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

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@Category(UnitTest.class)
public class LuceneDestroyIndexFunctionJUnitTest {

  private InternalLuceneService service;
  private GemFireCacheImpl cache;
  private String member;
  private FunctionContext context;
  private ResultSender resultSender;

  @Before
  public void prepare() {
    this.cache = Fakes.cache();
    this.member = Fakes.distributedSystem().getDistributedMember().getId();
    this.service = mock(InternalLuceneService.class);
    when(this.cache.getService(InternalLuceneService.class)).thenReturn(this.service);
    this.context = mock(FunctionContext.class);
    this.resultSender = mock(ResultSender.class);
    when(this.context.getResultSender()).thenReturn(this.resultSender);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteWithRegionAndIndex() throws Throwable {
    LuceneIndexInfo indexInfo = new LuceneIndexInfo("index1", "/region1");
    when(this.context.getArguments()).thenReturn(indexInfo);
    LuceneDestroyIndexFunction function = new LuceneDestroyIndexFunction();
    function = spy(function);
    doReturn(this.cache).when(function).getCache();
    doReturn(mock(XmlEntity.class)).when(function).getXmlEntity(any());
    function.execute(this.context);
    verify(this.service).destroyIndex(eq("index1"), eq("/region1"));
    verify(this.service, never()).destroyIndexes(eq("/region1"));
    ArgumentCaptor<CliFunctionResult> resultCaptor =
        ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertEquals(this.member, result.getMemberIdOrName());
    assertEquals(true, result.isSuccessful());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteWithRegion() throws Throwable {
    LuceneIndexInfo indexInfo = new LuceneIndexInfo(null, "/region1");
    when(this.context.getArguments()).thenReturn(indexInfo);
    LuceneDestroyIndexFunction function = new LuceneDestroyIndexFunction();
    function = spy(function);
    doReturn(this.cache).when(function).getCache();
    doReturn(mock(XmlEntity.class)).when(function).getXmlEntity(any());
    function.execute(this.context);
    verify(this.service).destroyIndexes(eq("/region1"));
    verify(this.service, never()).destroyIndex(any(), eq("/region1"));
    ArgumentCaptor<CliFunctionResult> resultCaptor =
        ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertEquals(this.member, result.getMemberIdOrName());
    assertEquals(true, result.isSuccessful());
  }
}
