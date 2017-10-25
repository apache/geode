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
package org.apache.geode.internal.protocol.protobuf.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.Failure;
import org.apache.geode.internal.protocol.ProtobufTestExecutionContext;
import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.internal.protocol.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.internal.protocol.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class PutRequestOperationHandlerJUnitTest extends OperationHandlerJUnitTest {
  private final String TEST_KEY = "my key";
  private final String TEST_VALUE = "99";
  private final String TEST_REGION = "test region";
  private Region regionMock;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    regionMock = mock(Region.class);
    when(regionMock.put(TEST_KEY, TEST_VALUE)).thenReturn(1);

    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionMock);
  }

  @Test
  public void test_puttingTheEncodedEntryIntoRegion() throws Exception {
    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();
    Result result = operationHandler.process(serializationServiceStub, generateTestRequest(),
        ProtobufTestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Success);

    verify(regionMock).put(TEST_KEY, TEST_VALUE);
    verify(regionMock, times(1)).put(anyString(), anyString());
  }

  @Test
  public void test_invalidEncodingType() throws Exception {
    String exceptionText = "unsupported type!";
    UnsupportedEncodingTypeException exception =
        new UnsupportedEncodingTypeException(exceptionText);
    when(serializationServiceStub.decode(any(), any())).thenThrow(exception);

    ByteString byteString = ByteString.copyFrom("{\"someKey\":\"someValue\"}", "UTF-8");
    BasicTypes.CustomEncodedValue.Builder customEncodedValueBuilder = BasicTypes.CustomEncodedValue
        .newBuilder().setEncodingType(BasicTypes.EncodingType.JSON).setValue(byteString);
    BasicTypes.EncodedValue encodedKey = BasicTypes.EncodedValue.newBuilder()
        .setCustomEncodedValue(customEncodedValueBuilder).build();

    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();

    BasicTypes.EncodedValue testValue =
        ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_VALUE);
    BasicTypes.Entry testEntry = ProtobufUtilities.createEntry(encodedKey, testValue);
    RegionAPI.PutRequest putRequest =
        ProtobufRequestUtilities.createPutRequest(TEST_REGION, testEntry).getPutRequest();
    Result result = operationHandler.process(serializationServiceStub, putRequest,
        ProtobufTestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Failure);
    ClientProtocol.ErrorResponse errorMessage =
        (ClientProtocol.ErrorResponse) result.getErrorMessage();
    assertEquals(ProtocolErrorCode.VALUE_ENCODING_ERROR.codeValue,
        errorMessage.getError().getErrorCode());
  }

  @Test
  public void test_RegionNotFound() throws Exception {
    when(cacheStub.getRegion(TEST_REGION)).thenReturn(null);
    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();
    Result result = operationHandler.process(serializationServiceStub, generateTestRequest(),
        ProtobufTestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Failure);
    ClientProtocol.ErrorResponse errorMessage =
        (ClientProtocol.ErrorResponse) result.getErrorMessage();
    assertEquals(ProtocolErrorCode.REGION_NOT_FOUND.codeValue,
        errorMessage.getError().getErrorCode());
  }

  @Test
  public void test_RegionThrowsClasscastException() throws Exception {
    when(regionMock.put(any(), any())).thenThrow(ClassCastException.class);

    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();
    Result result = operationHandler.process(serializationServiceStub, generateTestRequest(),
        ProtobufTestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Failure);
    ClientProtocol.ErrorResponse errorMessage =
        (ClientProtocol.ErrorResponse) result.getErrorMessage();
    assertEquals(ProtocolErrorCode.CONSTRAINT_VIOLATION.codeValue,
        errorMessage.getError().getErrorCode());
  }

  private RegionAPI.PutRequest generateTestRequest()
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    BasicTypes.EncodedValue testKey =
        ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_KEY);
    BasicTypes.EncodedValue testValue =
        ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_VALUE);
    BasicTypes.Entry testEntry = ProtobufUtilities.createEntry(testKey, testValue);
    return ProtobufRequestUtilities.createPutRequest(TEST_REGION, testEntry).getPutRequest();
  }
}
