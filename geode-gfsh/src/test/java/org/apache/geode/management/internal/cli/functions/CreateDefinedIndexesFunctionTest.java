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

package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexCreationException;
import org.apache.geode.cache.query.MultiIndexCreationException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.cache.query.internal.index.CompactMapRangeIndex;
import org.apache.geode.cache.query.internal.index.PrimaryKeyIndex;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.fake.Fakes;

public class CreateDefinedIndexesFunctionTest {
  private Cache cache;
  @SuppressWarnings("rawtypes")
  private Region region1;
  @SuppressWarnings("rawtypes")
  private Region region2;
  private FunctionContext<Set<RegionConfig.Index>> context;
  private QueryService queryService;
  private TestResultSender resultSender;
  private Set<RegionConfig.Index> indexDefinitions;
  private CreateDefinedIndexesFunction function;

  @Before
  @SuppressWarnings("deprecation")
  public void setUp() throws Exception {
    cache = Fakes.cache();
    resultSender = new TestResultSender();
    queryService = spy(InternalQueryService.class);
    region1 = Fakes.region("Region1", cache);
    region2 = Fakes.region("Region2", cache);
    function = spy(CreateDefinedIndexesFunction.class);
    doReturn(queryService).when(cache).getQueryService();

    indexDefinitions = new HashSet<>();
    indexDefinitions.add(new RegionConfig.Index() {
      {
        setName("index1");
        setExpression("value1");
        setFromClause("/Region1");
        setType(org.apache.geode.cache.query.IndexType.HASH.getName());
      }
    });
    indexDefinitions.add(new RegionConfig.Index() {
      {
        setName("index2");
        setExpression("value2");
        setFromClause("/Region2");
        setType(org.apache.geode.cache.query.IndexType.FUNCTIONAL.getName());
      }
    });
    indexDefinitions.add(new RegionConfig.Index() {
      {
        setName("index3");
        setExpression("value3");
        setFromClause("/Region1");
        setType(org.apache.geode.cache.query.IndexType.PRIMARY_KEY.getName());
      }
    });
  }

  @Test
  @SuppressWarnings("deprecation")
  public void noIndexDefinitionsAsFunctionArgument() throws Exception {
    context = createFunctionContext(Collections.emptySet());

    function.execute(context);
    List<?> results = resultSender.getResults();

    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(2);
    Object firstResult = results.get(0);
    assertThat(firstResult).isInstanceOf(CliFunctionResult.class);
    assertThat(((CliFunctionResult) firstResult).isSuccessful()).isTrue();
    assertThat(((CliFunctionResult) firstResult).getMessage()).isNotEmpty();
    assertThat(((CliFunctionResult) firstResult).getMessage()).contains("No indexes defined");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void noIndexPreviouslyDefinedInQueryService() throws Exception {
    when(queryService.createDefinedIndexes()).thenReturn(Collections.emptyList());
    context = createFunctionContext(indexDefinitions);

    function.execute(context);
    List<?> results = resultSender.getResults();

    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(2);
    Object firstResult = results.get(0);
    assertThat(firstResult).isInstanceOf(CliFunctionResult.class);
    assertThat(((CliFunctionResult) firstResult).isSuccessful()).isTrue();
    assertThat(((CliFunctionResult) firstResult).getMessage()).isNotEmpty();
    assertThat(((CliFunctionResult) firstResult).getMessage()).contains("No indexes defined");
  }

  @Test
  public void multiIndexCreationExceptionThrowByQueryService() throws Exception {
    HashMap<String, Exception> exceptions = new HashMap<>();
    exceptions.put("index1", new IndexCreationException("Mock Failure."));
    exceptions.put("index3", new IndexCreationException("Another Mock Failure."));
    when(queryService.createDefinedIndexes())
        .thenThrow(new MultiIndexCreationException(exceptions));
    context = createFunctionContext(indexDefinitions);

    function.execute(context);
    List<?> results = resultSender.getResults();

    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(4);

    CliFunctionResult result1 = (CliFunctionResult) results.get(0);
    assertThat(result1.isSuccessful()).isTrue();
    assertThat(result1.getStatusMessage()).isEqualTo("Created index index2");

    CliFunctionResult result2 = (CliFunctionResult) results.get(1);
    assertThat(result2.isSuccessful()).isFalse();
    assertThat(result2.getStatusMessage())
        .isEqualTo("Failed to create index index1: Mock Failure.");

    CliFunctionResult result3 = (CliFunctionResult) results.get(2);
    assertThat(result3.isSuccessful()).isFalse();
    assertThat(result3.getStatusMessage())
        .isEqualTo("Failed to create index index3: Another Mock Failure.");
  }

  @Test
  public void unexpectedExceptionThrowByQueryService() throws Exception {
    when(queryService.createDefinedIndexes()).thenThrow(new RuntimeException("Mock Exception"));
    context = createFunctionContext(indexDefinitions);

    function.execute(context);
    List<?> results = resultSender.getResults();

    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(2);
    CliFunctionResult firstResult = (CliFunctionResult) results.get(0);

    assertThat(firstResult.isSuccessful()).isFalse();
    assertThat(firstResult.getStatusMessage()).isEqualTo("Mock Exception");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void creationSuccess() throws Exception {
    @SuppressWarnings("deprecation")
    Index index1 = mock(org.apache.geode.cache.query.internal.index.HashIndex.class);
    when(index1.getName()).thenReturn("index1");
    when(index1.getRegion()).thenReturn(region1);

    Index index2 = mock(CompactMapRangeIndex.class);
    when(index2.getName()).thenReturn("index2");
    when(index2.getRegion()).thenReturn(region2);

    Index index3 = mock(PrimaryKeyIndex.class);
    when(index3.getName()).thenReturn("index3");
    when(index3.getRegion()).thenReturn(region1);

    when(queryService.createDefinedIndexes()).thenReturn(Arrays.asList(index1, index2, index3));
    context = createFunctionContext(indexDefinitions);

    function.execute(context);
    List<?> results = resultSender.getResults();

    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(4);

    Object firstIndex = results.get(0);
    assertThat(firstIndex).isNotNull();
    assertThat(firstIndex).isInstanceOf(CliFunctionResult.class);
    assertThat(((CliFunctionResult) firstIndex).isSuccessful());
  }

  @SuppressWarnings("unchecked")
  private FunctionContext<Set<RegionConfig.Index>> createFunctionContext(
      Set<RegionConfig.Index> indexDefinitions) {
    return new FunctionContextImpl(cache, CreateDefinedIndexesFunction.class.getName(),
        indexDefinitions, resultSender);
  }

  private static class TestResultSender implements ResultSender<Object> {

    private final List<Object> results = new LinkedList<>();

    private Exception t;

    protected List<Object> getResults() throws Exception {
      if (t != null) {
        throw t;
      }
      return Collections.unmodifiableList(results);
    }

    @Override
    public void lastResult(final Object lastResult) {
      results.add(lastResult);
    }

    @Override
    public void sendResult(final Object oneResult) {
      results.add(oneResult);
    }

    @Override
    public void sendException(final Throwable t) {
      this.t = (Exception) t;
    }
  }
}
