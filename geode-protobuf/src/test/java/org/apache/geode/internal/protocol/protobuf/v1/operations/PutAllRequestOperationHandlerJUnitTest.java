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

import static org.apache.geode.internal.protocol.TestExecutionContext.getNoAuthCacheExecutionContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.protocol.TestExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class PutAllRequestOperationHandlerJUnitTest extends OperationHandlerJUnitTest {
  private final String TEST_KEY1 = "my key1";
  private final String TEST_KEY2 = "my key2";
  private final String TEST_KEY3 = "my key3";
  private final String TEST_INVALID_KEY = "invalid key";
  private final String TEST_VALUE1 = "my value1";
  private final String TEST_VALUE2 = "my value2";
  private final String TEST_VALUE3 = "my value3";
  private final Integer TEST_INVALID_VALUE = 732;
  private final String TEST_REGION = "test region";
  private final String EXCEPTION_TEXT = "Simulating put failure";
  private Region regionMock;

  @Before
  public void setUp() throws Exception {
    regionMock = mock(Region.class);
    when(regionMock.put(TEST_INVALID_KEY, TEST_INVALID_VALUE))
        .thenThrow(new ClassCastException(EXCEPTION_TEXT));

    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionMock);

    operationHandler = new PutAllRequestOperationHandler();
  }

  @Test
  public void processReturnsErrorUnableToDecodeRequest() throws Exception {
    Exception exception = new DecodingException("error finding codec for type");
    ProtobufSerializationService serializationServiceStub =
        mock(ProtobufSerializationService.class);
    when(serializationServiceStub.decode(any())).thenThrow(exception);

    BasicTypes.EncodedValue encodedObject = BasicTypes.EncodedValue.newBuilder()
        .setJsonObjectResult("{\"someKey\":\"someValue\"}").build();

    Set<BasicTypes.Entry> entries = new HashSet<>();
    entries.add(ProtobufUtilities.createEntry(encodedObject, encodedObject));

    RegionAPI.PutAllRequest putAllRequest =
        ProtobufRequestUtilities.createPutAllRequest(TEST_REGION, entries).getPutAllRequest();

    Result response = operationHandler.process(serializationServiceStub, putAllRequest,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue("response was " + response, response instanceof Success);

    RegionAPI.PutAllResponse message = (RegionAPI.PutAllResponse) response.getMessage();
    assertEquals(1, message.getFailedKeysCount());

    BasicTypes.KeyedError error = message.getFailedKeys(0);
    assertEquals(BasicTypes.ErrorCode.INVALID_REQUEST, error.getError().getErrorCode());
    assertTrue(error.getError().getMessage().contains("Encoding not supported"));
  }



  @Test
  public void processInsertsMultipleValidEntriesInCache() throws Exception {
    Result result = operationHandler.process(serializationService, generateTestRequest(false, true),
        getNoAuthCacheExecutionContext(cacheStub));

    Assert.assertTrue(result instanceof Success);

    verify(regionMock).put(TEST_KEY1, TEST_VALUE1);
    verify(regionMock).put(TEST_KEY2, TEST_VALUE2);
    verify(regionMock).put(TEST_KEY3, TEST_VALUE3);
  }

  @Test
  public void processWithInvalidEntrySucceedsAndReturnsFailedKey() throws Exception {
    Result result = operationHandler.process(serializationService, generateTestRequest(true, true),
        getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Success);
    verify(regionMock).put(TEST_KEY1, TEST_VALUE1);
    verify(regionMock).put(TEST_KEY2, TEST_VALUE2);
    verify(regionMock).put(TEST_KEY3, TEST_VALUE3);

    RegionAPI.PutAllResponse putAllResponse = (RegionAPI.PutAllResponse) result.getMessage();
    assertEquals(1, putAllResponse.getFailedKeysCount());
    BasicTypes.KeyedError error = putAllResponse.getFailedKeys(0);
    assertEquals(TEST_INVALID_KEY, serializationService.decode(error.getKey()));
  }

  @Test
  public void processWithNoEntriesPasses() throws Exception {
    Result result = operationHandler.process(serializationService,
        generateTestRequest(false, false), getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Success);

    verify(regionMock, times(0)).put(any(), any());
  }

  private RegionAPI.PutAllRequest generateTestRequest(boolean addInvalidKey, boolean addValidKeys)
      throws EncodingException {
    Set<BasicTypes.Entry> entries = new HashSet<>();
    if (addInvalidKey) {
      entries.add(ProtobufUtilities.createEntry(serializationService.encode(TEST_INVALID_KEY),
          serializationService.encode(TEST_INVALID_VALUE)));
    }
    if (addValidKeys) {
      entries.add(ProtobufUtilities.createEntry(serializationService.encode(TEST_KEY1),
          serializationService.encode(TEST_VALUE1)));
      entries.add(ProtobufUtilities.createEntry(serializationService.encode(TEST_KEY2),
          serializationService.encode(TEST_VALUE2)));
      entries.add(ProtobufUtilities.createEntry(serializationService.encode(TEST_KEY3),
          serializationService.encode(TEST_VALUE3)));
    }
    return ProtobufRequestUtilities.createPutAllRequest(TEST_REGION, entries).getPutAllRequest();
  }
}
