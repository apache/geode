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
package org.apache.geode.protocol.protobuf.operations;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.tier.sockets.MessageExecutionContext;
import org.apache.geode.internal.cache.tier.sockets.InvalidExecutionContextException;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.Result;
import org.apache.geode.protocol.protobuf.Success;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    super.setUp();
    regionMock = mock(Region.class);
    when(regionMock.put(TEST_INVALID_KEY, TEST_INVALID_VALUE))
        .thenThrow(new ClassCastException(EXCEPTION_TEXT));

    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionMock);
  }

  @Test
  public void processInsertsMultipleValidEntriesInCache()
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException,
      CodecAlreadyRegisteredForTypeException, InvalidExecutionContextException {
    PutAllRequestOperationHandler operationHandler = new PutAllRequestOperationHandler();

    Result<RegionAPI.PutAllResponse> result = operationHandler.process(serializationServiceStub,
        generateTestRequest(false, true), new MessageExecutionContext(cacheStub));

    Assert.assertTrue(result instanceof Success);

    verify(regionMock).put(TEST_KEY1, TEST_VALUE1);
    verify(regionMock).put(TEST_KEY2, TEST_VALUE2);
    verify(regionMock).put(TEST_KEY3, TEST_VALUE3);
  }

  @Test
  public void processWithInvalidEntrySucceedsAndReturnsFailedKey() throws Exception {
    PutAllRequestOperationHandler operationHandler = new PutAllRequestOperationHandler();

    Result<RegionAPI.PutAllResponse> result = operationHandler.process(serializationServiceStub,
        generateTestRequest(true, true), new MessageExecutionContext(cacheStub));

    assertTrue(result instanceof Success);
    verify(regionMock).put(TEST_KEY1, TEST_VALUE1);
    verify(regionMock).put(TEST_KEY2, TEST_VALUE2);
    verify(regionMock).put(TEST_KEY3, TEST_VALUE3);

    RegionAPI.PutAllResponse putAllResponse = result.getMessage();
    assertEquals(1, putAllResponse.getFailedKeysCount());
    BasicTypes.KeyedErrorResponse error = putAllResponse.getFailedKeys(0);
    assertEquals(TEST_INVALID_KEY,
        ProtobufUtilities.decodeValue(serializationServiceStub, error.getKey()));
  }

  @Test
  public void processWithNoEntriesPasses() throws Exception {
    PutAllRequestOperationHandler operationHandler = new PutAllRequestOperationHandler();

    Result<RegionAPI.PutAllResponse> result = operationHandler.process(serializationServiceStub,
        generateTestRequest(false, false), new MessageExecutionContext(cacheStub));

    assertTrue(result instanceof Success);

    verify(regionMock, times(0)).put(any(), any());
  }

  private RegionAPI.PutAllRequest generateTestRequest(boolean addInvalidKey, boolean addValidKeys)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    Set<BasicTypes.Entry> entries = new HashSet<>();
    if (addInvalidKey) {
      entries.add(ProtobufUtilities.createEntry(
          ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_INVALID_KEY),
          ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_INVALID_VALUE)));
    }
    if (addValidKeys) {
      entries.add(ProtobufUtilities.createEntry(
          ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_KEY1),
          ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_VALUE1)));
      entries.add(ProtobufUtilities.createEntry(
          ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_KEY2),
          ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_VALUE2)));
      entries.add(ProtobufUtilities.createEntry(
          ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_KEY3),
          ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_VALUE3)));
    }
    return ProtobufRequestUtilities.createPutAllRequest(TEST_REGION, entries).getPutAllRequest();
  }
}
