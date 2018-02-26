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
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexCreationException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.MultiIndexCreationException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.cache.query.internal.index.CompactMapRangeIndex;
import org.apache.geode.cache.query.internal.index.HashIndex;
import org.apache.geode.cache.query.internal.index.PrimaryKeyIndex;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.management.internal.cli.domain.IndexInfo;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class CreateDefinedIndexesFunctionTest {
  private Cache cache;
  private Region region1;
  private Region region2;
  private FunctionContext context;
  private QueryService queryService;
  private TestResultSender resultSender;
  private Set<IndexInfo> indexDefinitions;
  private CreateDefinedIndexesFunction function;

  @Before
  public void setUp() throws Exception {
    cache = Fakes.cache();
    resultSender = new TestResultSender();
    queryService = spy(InternalQueryService.class);
    region1 = Fakes.region("Region1", cache);
    region2 = Fakes.region("Region2", cache);
    function = spy(CreateDefinedIndexesFunction.class);
    doReturn(queryService).when(cache).getQueryService();

    indexDefinitions = new HashSet<>();
    indexDefinitions.add(new IndexInfo("index1", "value1", "/Region1", IndexType.HASH));
    indexDefinitions.add(new IndexInfo("index2", "value2", "/Region2", IndexType.FUNCTIONAL));
    indexDefinitions.add(new IndexInfo("index3", "value3", "/Region1", IndexType.PRIMARY_KEY));
  }

  @Test
  public void noIndexDefinitionsAsFunctionArgument() throws Exception {
    context = new FunctionContextImpl(cache, CreateDefinedIndexesFunction.class.getName(),
        Collections.emptySet(), resultSender, cache.getDistributedSystem());

    function.execute(context);
    List<?> results = resultSender.getResults();

    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(1);
    Object firstResult = results.get(0);
    assertThat(firstResult).isInstanceOf(CliFunctionResult.class);
    assertThat(((CliFunctionResult) firstResult).isSuccessful()).isTrue();
    assertThat(((CliFunctionResult) firstResult).getMessage()).isNotEmpty();
    assertThat(((CliFunctionResult) firstResult).getMessage()).contains("No indexes defined");
  }

  @Test
  public void noIndexPreviouslyDefinedInQueryService() throws Exception {
    when(queryService.createDefinedIndexes()).thenReturn(Collections.emptyList());
    context = new FunctionContextImpl(cache, CreateDefinedIndexesFunction.class.getName(),
        indexDefinitions, resultSender, cache.getDistributedSystem());

    function.execute(context);
    List<?> results = resultSender.getResults();

    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(1);
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
    context = new FunctionContextImpl(cache, CreateDefinedIndexesFunction.class.getName(),
        indexDefinitions, resultSender, cache.getDistributedSystem());

    function.execute(context);
    List<?> results = resultSender.getResults();

    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(1);
    Object firstResult = results.get(0);
    assertThat(firstResult).isNotNull();
    assertThat(firstResult).isInstanceOf(CliFunctionResult.class);
    assertThat(((CliFunctionResult) firstResult).isSuccessful()).isFalse();
    assertThat(((CliFunctionResult) firstResult).getSerializables().length).isEqualTo(1);
    assertThat(((CliFunctionResult) firstResult).getSerializables()[0]).isNotNull();
    assertThat(((CliFunctionResult) firstResult).getSerializables()[0].toString()).contains(
        "Index creation failed for indexes", "index1", "Mock Failure", "index3",
        "Another Mock Failure");
  }

  @Test
  public void unexpectedExceptionThrowByQueryService() throws Exception {
    when(queryService.createDefinedIndexes()).thenThrow(new RuntimeException("Mock Exception"));
    context = new FunctionContextImpl(cache, CreateDefinedIndexesFunction.class.getName(),
        indexDefinitions, resultSender, cache.getDistributedSystem());

    function.execute(context);
    List<?> results = resultSender.getResults();

    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(1);
    Object firstResult = results.get(0);
    assertThat(firstResult).isNotNull();
    assertThat(firstResult).isInstanceOf(CliFunctionResult.class);
    assertThat(((CliFunctionResult) firstResult).isSuccessful()).isFalse();
    assertThat(((CliFunctionResult) firstResult).getSerializables().length).isEqualTo(1);
    assertThat(((CliFunctionResult) firstResult).getSerializables()[0]).isNotNull();
    assertThat(((CliFunctionResult) firstResult).getSerializables()[0].toString())
        .contains("RuntimeException", "Mock Exception");
  }

  @Test
  public void creationSuccess() throws Exception {
    Index index1 = mock(HashIndex.class);
    when(index1.getName()).thenReturn("index1");
    when(index1.getRegion()).thenReturn(region1);

    Index index2 = mock(CompactMapRangeIndex.class);
    when(index2.getName()).thenReturn("index2");
    when(index2.getRegion()).thenReturn(region2);

    Index index3 = mock(PrimaryKeyIndex.class);
    when(index3.getName()).thenReturn("index3");
    when(index3.getRegion()).thenReturn(region1);

    doReturn(mock(XmlEntity.class)).when(function).createXmlEntity(any());
    when(queryService.createDefinedIndexes()).thenReturn(Arrays.asList(index1, index2, index3));
    context = new FunctionContextImpl(cache, CreateDefinedIndexesFunction.class.getName(),
        indexDefinitions, resultSender, cache.getDistributedSystem());

    function.execute(context);
    List<?> results = resultSender.getResults();

    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(2);

    Object firstIndex = results.get(0);
    assertThat(firstIndex).isNotNull();
    assertThat(firstIndex).isInstanceOf(CliFunctionResult.class);
    assertThat(((CliFunctionResult) firstIndex).isSuccessful());

    Object secondIndex = results.get(0);
    assertThat(secondIndex).isNotNull();
    assertThat(secondIndex).isInstanceOf(CliFunctionResult.class);
    assertThat(((CliFunctionResult) secondIndex).isSuccessful()).isTrue();
    assertThat(((CliFunctionResult) secondIndex).getXmlEntity()).isNotNull();
  }

  private static class TestResultSender implements ResultSender {

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
