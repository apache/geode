/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.lucene.LuceneQueryException;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;
import com.gemstone.gemfire.cache.lucene.PageableLuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.LuceneResultStruct;
import com.gemstone.gemfire.cache.lucene.internal.distributed.EntryScore;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunction;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunctionContext;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntries;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntriesCollector;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneQueryImplJUnitTest {
  private static int LIMIT = 123;
  private LuceneQueryImpl<Object, Object> query;
  private Execution execution;
  private LuceneQueryProvider provider;
  private ResultCollector<TopEntriesCollector, TopEntries> collector;
  private Region region;

  @Before
  public void createMocks() {
    region = mock(Region.class);
    execution = mock(Execution.class);
    collector = mock(ResultCollector.class);
    provider = mock(LuceneQueryProvider.class);

    when(execution.withArgs(any())).thenReturn(execution);
    when(execution.withCollector(any())).thenReturn(execution);
    when(execution.execute(anyString())).thenReturn((ResultCollector) collector);

    query = new LuceneQueryImpl<Object, Object>("index", region, provider, null, LIMIT, 20) {
      @Override protected Execution onRegion() {
        return execution;
      }
    };
  }

  private void addValueToResults() {
    TopEntries entries = new TopEntries();
    entries.addHit(new EntryScore("hi", 5));
    when(collector.getResult()).thenReturn(entries);

    Map<String, String> getAllResult = new HashMap<String, String>();
    getAllResult.put("hi", "value");
    when(region.getAll(eq(Collections.singletonList("hi"))))
      .thenReturn(getAllResult);
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
    List<LuceneResultStruct<String,String>> result=new ArrayList<>();
    result.add(new LuceneResultStructImpl("hi","value",5));
    assertEquals(result,query.findResults());
  }

  @Test
  public void shouldInvokeLuceneFunctionWithCorrectArguments() throws Exception {
    addValueToResults();
    PageableLuceneQueryResults<Object, Object> results = query.findPages();

    verify(execution).execute(eq(LuceneFunction.ID));
    ArgumentCaptor<LuceneFunctionContext> captor = ArgumentCaptor.forClass(LuceneFunctionContext.class);
    verify(execution).withArgs(captor.capture());
    LuceneFunctionContext context = captor.getValue();
    assertEquals(LIMIT, context.getLimit());
    assertEquals(provider, context.getQueryProvider());
    assertEquals("index", context.getIndexName());

    assertEquals(5, results.getMaxScore(), 0.01);
    final List<LuceneResultStruct<Object, Object>> page = results.next();
    assertEquals(1, page.size());
    LuceneResultStruct element = page.iterator().next();
    assertEquals("hi", element.getKey());
    assertEquals("value", element.getValue());
    assertEquals(5, element.getScore(), 0.01);
  }
}
