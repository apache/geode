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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.internal.protocol.TestExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
@SuppressWarnings("unchecked") // Region lacks generics when we look it up
public class PutIfAbsentRequestOperationHandlerJUnitTest extends OperationHandlerJUnitTest {
  private final String TEST_KEY = "my key";
  private final String TEST_VALUE = "99";
  private final String TEST_REGION = "test region";
  private Region regionMock;
  private PutIfAbsentRequestOperationHandler operationHandler;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    regionMock = mock(Region.class);
    operationHandler = new PutIfAbsentRequestOperationHandler();
    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionMock);
  }

  @Test
  public void newEntrySucceeds() throws Exception {
    when(regionMock.putIfAbsent(TEST_KEY, TEST_VALUE)).thenReturn(null);

    Result<RegionAPI.PutIfAbsentResponse> result1 = operationHandler.process(serializationService,
        generateTestRequest(), TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertNull(serializationService.decode(result1.getMessage().getOldValue()));

    verify(regionMock).putIfAbsent(TEST_KEY, TEST_VALUE);
    verify(regionMock, times(1)).putIfAbsent(any(), any());
  }

  @Test
  public void existingEntryFails() throws Exception {
    when(regionMock.putIfAbsent(TEST_KEY, TEST_VALUE)).thenReturn(1);

    Result<RegionAPI.PutIfAbsentResponse> result1 = operationHandler.process(serializationService,
        generateTestRequest(), TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertNotNull(serializationService.decode(result1.getMessage().getOldValue()));

    verify(regionMock).putIfAbsent(TEST_KEY, TEST_VALUE);
    verify(regionMock, times(1)).putIfAbsent(any(), any());
  }

  @Test
  public void nullValuePassedThrough() throws Exception {
    final RegionAPI.PutIfAbsentRequest request =
        RegionAPI.PutIfAbsentRequest.newBuilder().setRegionName(TEST_REGION)
            .setEntry(ProtobufUtilities.createEntry(serializationService, TEST_KEY, null)).build();

    Result<RegionAPI.PutIfAbsentResponse> response = operationHandler.process(serializationService,
        request, TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertNull(serializationService.decode(response.getMessage().getOldValue()));

    verify(regionMock).putIfAbsent(TEST_KEY, null);
  }

  @Test
  public void nullKeyPassedThrough() throws Exception {
    final RegionAPI.PutIfAbsentRequest request = RegionAPI.PutIfAbsentRequest.newBuilder()
        .setRegionName(TEST_REGION)
        .setEntry(ProtobufUtilities.createEntry(serializationService, null, TEST_VALUE)).build();

    Result<RegionAPI.PutIfAbsentResponse> response = operationHandler.process(serializationService,
        request, TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertNull(serializationService.decode(response.getMessage().getOldValue()));

    verify(regionMock).putIfAbsent(null, TEST_VALUE);
  }

  @Test(expected = DecodingException.class)
  public void unsetEntrythrowsDecodingException() throws Exception {
    Result<RegionAPI.PutIfAbsentResponse> result1 =
        operationHandler.process(serializationService, generateTestRequest(true, false),
            TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertEquals(BasicTypes.ErrorCode.INVALID_REQUEST,
        result1.getErrorMessage().getError().getErrorCode());
  }

  @Test
  public void unsetRegionGetsServerError() throws Exception {
    expectedException.expect(RegionDestroyedException.class);
    Result<RegionAPI.PutIfAbsentResponse> result1 =
        operationHandler.process(serializationService, generateTestRequest(false, true),
            TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));
  }

  @Test
  public void nonexistingRegionReturnsServerError() throws Exception {
    when(cacheStub.getRegion(TEST_REGION)).thenReturn(null);

    expectedException.expect(RegionDestroyedException.class);
    Result<RegionAPI.PutIfAbsentResponse> result1 = operationHandler.process(serializationService,
        generateTestRequest(), TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));
  }

  /**
   * Some regions (DataPolicy.NORMAL, for example) don't support concurrent ops such as putIfAbsent.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void unsupportedOperation() throws Exception {
    when(regionMock.putIfAbsent(any(), any())).thenThrow(new UnsupportedOperationException());

    Result<RegionAPI.PutIfAbsentResponse> result1 = operationHandler.process(serializationService,
        generateTestRequest(), TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));
    assertEquals(BasicTypes.ErrorCode.INVALID_REQUEST,
        result1.getErrorMessage().getError().getErrorCode());
  }

  private RegionAPI.PutIfAbsentRequest generateTestRequest(boolean includeRegion,
      boolean includeEntry) throws EncodingException {
    RegionAPI.PutIfAbsentRequest.Builder builder = RegionAPI.PutIfAbsentRequest.newBuilder();

    if (includeRegion) {
      builder.setRegionName(TEST_REGION);
    }

    if (includeEntry) {
      BasicTypes.EncodedValue testKey = serializationService.encode(TEST_KEY);
      BasicTypes.EncodedValue testValue = serializationService.encode(TEST_VALUE);
      BasicTypes.Entry testEntry = ProtobufUtilities.createEntry(testKey, testValue);
      builder.setEntry(testEntry);
    }

    return builder.build();
  }

  private RegionAPI.PutIfAbsentRequest generateTestRequest() throws EncodingException {
    return generateTestRequest(true, true);
  }
}
