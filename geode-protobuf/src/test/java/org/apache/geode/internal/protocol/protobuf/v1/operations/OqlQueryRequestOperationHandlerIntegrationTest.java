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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.EncodedValue;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.OQLQueryRequest;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.OQLQueryResponse;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.AuthorizingCacheImpl;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.NoSecurityAuthorizer;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OqlQueryRequestOperationHandlerIntegrationTest {
  private Cache cache;

  @Before
  public void setUp() throws Exception {
    cache = new CacheFactory().set(ConfigurationProperties.LOCATORS, "").create();
    Region region = cache.createRegionFactory(RegionShortcut.LOCAL).create("region");

    IntStream.range(0, 2).forEach(i -> region.put("key" + i, new PortfolioPdx(i)));
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void queryForSingleObject() throws ConnectionStateException, DecodingException,
      InvalidExecutionContextException, EncodingException, NameResolutionException,
      TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {
    checkResults("(select * from /region).size", 2);
  }

  @Test
  public void queryForMultipleWholeObjects() throws ConnectionStateException, DecodingException,
      InvalidExecutionContextException, EncodingException, NameResolutionException,
      TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {
    checkResults("select ID from /region order by ID", 0, 1);
  }

  @Test
  public void queryForMultipleProjectionFields() throws ConnectionStateException, DecodingException,
      InvalidExecutionContextException, EncodingException, NameResolutionException,
      TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {
    checkResults("select ID,status from /region order by ID", new EncodedValue[] {},
        new String[] {"ID", "status"}, new Object[] {0, "active"}, new Object[] {1, "inactive"});
  }

  @Test
  public void queryForSingleStruct() throws ConnectionStateException, DecodingException,
      InvalidExecutionContextException, EncodingException, NameResolutionException,
      TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {
    checkResults("select count(*),min(ID) from /region", new EncodedValue[] {},
        new String[] {"0", "ID"}, new Object[] {2, 0});
  }

  @Test
  public void queryWithBindParameters() throws ConnectionStateException, DecodingException,
      InvalidExecutionContextException, EncodingException, NameResolutionException,
      TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {
    checkResults("select ID,status from /region where ID=$1",
        new EncodedValue[] {new ProtobufSerializationService().encode(0)},
        new String[] {"ID", "status"}, new Object[] {0, "active"});
  }

  private void checkResults(final String query, final Object value)
      throws InvalidExecutionContextException, ConnectionStateException, EncodingException,
      DecodingException {
    ProtobufSerializationService serializer = new ProtobufSerializationService();
    final Result<OQLQueryResponse> results =
        invokeHandler(query, new EncodedValue[] {}, serializer);

    assertEquals(serializer.encode(value), results.getMessage().getSingleResult());
  }

  private void checkResults(final String query, final Object... values)
      throws InvalidExecutionContextException, ConnectionStateException, EncodingException,
      DecodingException {
    ProtobufSerializationService serializer = new ProtobufSerializationService();
    final Result<OQLQueryResponse> results =
        invokeHandler(query, new EncodedValue[] {}, serializer);

    List<EncodedValue> expected =
        Arrays.asList(values).stream().map(serializer::encode).collect(Collectors.toList());
    assertEquals(expected, results.getMessage().getListResult().getElementList());
  }

  private void checkResults(final String query, EncodedValue[] bindParameters, String[] fieldnames,
      final Object[]... rows) throws InvalidExecutionContextException, ConnectionStateException,
      EncodingException, DecodingException {
    ProtobufSerializationService serializer = new ProtobufSerializationService();
    final Result<OQLQueryResponse> results = invokeHandler(query, bindParameters, serializer);

    List<BasicTypes.EncodedValueList> expected = new ArrayList<>();
    for (Object[] row : rows) {
      List<EncodedValue> encodedRow =
          Arrays.asList(row).stream().map(serializer::encode).collect(Collectors.toList());
      expected.add(BasicTypes.EncodedValueList.newBuilder().addAllElement(encodedRow).build());
    }

    assertEquals(expected, results.getMessage().getTableResult().getRowList());
    assertEquals(Arrays.asList(fieldnames),
        results.getMessage().getTableResult().getFieldNameList());
  }

  private Result<OQLQueryResponse> invokeHandler(String query, EncodedValue[] bindParameters,
      ProtobufSerializationService serializer) throws InvalidExecutionContextException,
      ConnectionStateException, EncodingException, DecodingException {
    final MessageExecutionContext context = mock(MessageExecutionContext.class);
    when(context.getAuthorizingCache())
        .thenReturn(new AuthorizingCacheImpl((InternalCache) cache, new NoSecurityAuthorizer()));
    final OQLQueryRequest request = OQLQueryRequest.newBuilder().setQuery(query)
        .addAllBindParameter(Arrays.asList(bindParameters)).build();
    Result<OQLQueryResponse> result =
        new OqlQueryRequestOperationHandler().process(serializer, request, context);

    return result;
  }
}
