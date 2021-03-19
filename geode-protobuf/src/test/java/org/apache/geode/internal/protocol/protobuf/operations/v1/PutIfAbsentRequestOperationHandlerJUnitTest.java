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
package org.apache.geode.internal.protocol.protobuf.operations.v1;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.internal.protocol.TestExecutionContext;
import org.apache.geode.internal.protocol.protobuf.Result;
import org.apache.geode.internal.protocol.protobuf.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
@SuppressWarnings("unchecked") // Region lacks generics when we look it up
public class PutIfAbsentRequestOperationHandlerJUnitTest extends OperationHandlerJUnitTest {
  private final String TEST_KEY = "my key";
  private final String TEST_VALUE = "99";
  private final String TEST_REGION = "test region";
  private Region regionMock;
  private PutIfAbsentRequestOperationHandler operationHandler;

  @Before
  public void setUp() throws Exception {
    regionMock = Mockito.mock(Region.class);
    operationHandler = new PutIfAbsentRequestOperationHandler();
    Mockito.when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionMock);
  }

  @Test
  public void newEntrySucceeds() throws Exception {
    Mockito.when(regionMock.putIfAbsent(TEST_KEY, TEST_VALUE)).thenReturn(null);

    Result<RegionAPI.PutIfAbsentResponse> result1 = operationHandler.process(serializationService,
        generateTestRequest(), TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    Assert.assertNull(serializationService.decode(result1.getMessage().getOldValue()));

    Mockito.verify(regionMock).putIfAbsent(TEST_KEY, TEST_VALUE);
    Mockito.verify(regionMock, Mockito.times(1)).putIfAbsent(ArgumentMatchers.any(),
        ArgumentMatchers
            .any());
  }

  @Test
  public void existingEntryFails() throws Exception {
    Mockito.when(regionMock.putIfAbsent(TEST_KEY, TEST_VALUE)).thenReturn(1);

    Result<RegionAPI.PutIfAbsentResponse> result1 = operationHandler.process(serializationService,
        generateTestRequest(), TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    Assert.assertNotNull(serializationService.decode(result1.getMessage().getOldValue()));

    Mockito.verify(regionMock).putIfAbsent(TEST_KEY, TEST_VALUE);
    Mockito.verify(regionMock, Mockito.times(1)).putIfAbsent(ArgumentMatchers.any(),
        ArgumentMatchers
            .any());
  }

  @Test
  public void nullValuePassedThrough() throws Exception {
    final RegionAPI.PutIfAbsentRequest request =
        RegionAPI.PutIfAbsentRequest.newBuilder().setRegionName(TEST_REGION)
            .setEntry(ProtobufUtilities.createEntry(serializationService, TEST_KEY, null)).build();

    Result<RegionAPI.PutIfAbsentResponse> response = operationHandler.process(serializationService,
        request, TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    Assert.assertNull(serializationService.decode(response.getMessage().getOldValue()));

    Mockito.verify(regionMock).putIfAbsent(TEST_KEY, null);
  }

  @Test
  public void nullKeyPassedThrough() throws Exception {
    final RegionAPI.PutIfAbsentRequest request = RegionAPI.PutIfAbsentRequest.newBuilder()
        .setRegionName(TEST_REGION)
        .setEntry(ProtobufUtilities.createEntry(serializationService, null, TEST_VALUE)).build();

    Result<RegionAPI.PutIfAbsentResponse> response = operationHandler.process(serializationService,
        request, TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    Assert.assertNull(serializationService.decode(response.getMessage().getOldValue()));

    Mockito.verify(regionMock).putIfAbsent(null, TEST_VALUE);
  }

  @Test(expected = DecodingException.class)
  public void unsetEntrythrowsDecodingException() throws Exception {
    Result<RegionAPI.PutIfAbsentResponse> result1 =
        operationHandler.process(serializationService, generateTestRequest(true, false),
            TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    Assert.assertEquals(BasicTypes.ErrorCode.INVALID_REQUEST,
        result1.getErrorMessage().getError().getErrorCode());
  }

  @Test
  public void unsetRegionGetsServerError() {
    Assertions.assertThatThrownBy(
        () -> operationHandler.process(serializationService, generateTestRequest(false, true),
            TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub)))
        .isInstanceOf(RegionDestroyedException.class);
  }

  @Test
  public void nonexistingRegionReturnsServerError() throws Exception {
    Mockito.when(cacheStub.getRegion(TEST_REGION)).thenReturn(null);

    Assertions.assertThatThrownBy(() -> operationHandler.process(serializationService,
        generateTestRequest(), TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub)))
        .isInstanceOf(RegionDestroyedException.class);
  }

  /**
   * Some regions (DataPolicy.NORMAL, for example) don't support concurrent ops such as putIfAbsent.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void unsupportedOperation() throws Exception {
    Mockito.when(regionMock.putIfAbsent(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenThrow(new UnsupportedOperationException());

    Result<RegionAPI.PutIfAbsentResponse> result1 = operationHandler.process(serializationService,
        generateTestRequest(), TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));
    Assert.assertEquals(BasicTypes.ErrorCode.INVALID_REQUEST,
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
