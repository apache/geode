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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.cache.query.internal.LinkedStructSet;
import org.apache.geode.cache.query.internal.ResultsBag;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.TestExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.OQLQueryRequest;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.OQLQueryResponse;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class OqlQueryRequestOperationHandlerJUnitTest
    extends OperationHandlerJUnitTest<OQLQueryRequest, OQLQueryResponse> {

  public static final String SELECT_STAR_QUERY = "select * from /region";
  public static final String STRING_RESULT_1 = "result1";
  public static final String STRING_RESULT_2 = "result2";
  private InternalQueryService queryService;

  @Before
  public void setUp() {
    queryService = mock(InternalQueryService.class);
    when(cacheStub.getQueryService()).thenReturn(queryService);
    operationHandler = new OqlQueryRequestOperationHandler();
  }

  @Test
  public void queryForSingleObject() throws ConnectionStateException, DecodingException,
      InvalidExecutionContextException, EncodingException, NameResolutionException,
      TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {
    Query query = mock(DefaultQuery.class);
    when(queryService.newQuery(eq(SELECT_STAR_QUERY))).thenReturn(query);
    when(query.execute((Object[]) any())).thenReturn(STRING_RESULT_1);
    final OQLQueryRequest request =
        OQLQueryRequest.newBuilder().setQuery(SELECT_STAR_QUERY).build();
    final Result<OQLQueryResponse> result = operationHandler.process(serializationService, request,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertEquals(serializationService.encode(STRING_RESULT_1),
        result.getMessage().getSingleResult());
  }

  @Test
  public void queryForMultipleObjects() throws ConnectionStateException, DecodingException,
      InvalidExecutionContextException, EncodingException, NameResolutionException,
      TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {
    Query query = mock(DefaultQuery.class);
    when(queryService.newQuery(eq(SELECT_STAR_QUERY))).thenReturn(query);
    @SuppressWarnings("unchecked")
    SelectResults<String> results = new ResultsBag();
    results.setElementType(new ObjectTypeImpl());
    results.add(STRING_RESULT_1);
    results.add(STRING_RESULT_2);

    when(query.execute((Object[]) any())).thenReturn(results);
    final OQLQueryRequest request =
        OQLQueryRequest.newBuilder().setQuery(SELECT_STAR_QUERY).build();
    final Result<OQLQueryResponse> result = operationHandler.process(serializationService, request,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertEquals(Arrays.asList(STRING_RESULT_1, STRING_RESULT_2),
        result.getMessage().getListResult().getElementList().stream()
            .map(serializationService::decode).collect(Collectors.toList()));
  }

  @Test
  public void queryForMultipleStructs() throws ConnectionStateException, DecodingException,
      InvalidExecutionContextException, EncodingException, NameResolutionException,
      TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {
    Query query = mock(DefaultQuery.class);
    when(queryService.newQuery(eq(SELECT_STAR_QUERY))).thenReturn(query);


    SelectResults<Struct> results = new LinkedStructSet();
    StructTypeImpl elementType = new StructTypeImpl(new String[] {"field1"});
    results.setElementType(elementType);
    results.add(new StructImpl(elementType, new Object[] {STRING_RESULT_1}));
    results.add(new StructImpl(elementType, new Object[] {STRING_RESULT_2}));

    when(query.execute((Object[]) any())).thenReturn(results);

    final OQLQueryRequest request =
        OQLQueryRequest.newBuilder().setQuery(SELECT_STAR_QUERY).build();

    final Result<OQLQueryResponse> result = operationHandler.process(serializationService, request,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertEquals(
        Arrays.asList(
            BasicTypes.EncodedValueList.newBuilder()
                .addElement(serializationService.encode(STRING_RESULT_1)).build(),
            BasicTypes.EncodedValueList.newBuilder()
                .addElement(serializationService.encode(STRING_RESULT_2)).build()),
        result.getMessage().getTableResult().getRowList());
  }

}
