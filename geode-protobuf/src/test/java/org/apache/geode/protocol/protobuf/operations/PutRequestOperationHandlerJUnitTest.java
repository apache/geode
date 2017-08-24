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

import com.google.protobuf.ByteString;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.tier.sockets.MessageExecutionContext;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.Failure;
import org.apache.geode.protocol.protobuf.ProtocolErrorCode;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.Result;
import org.apache.geode.protocol.protobuf.Success;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.security.server.NoOpAuthorizer;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
  public void test_puttingTheEncodedEntryIntoRegion()
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException,
      CodecAlreadyRegisteredForTypeException, InvalidExecutionContextException {
    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();
    Result<RegionAPI.PutResponse> result = operationHandler.process(serializationServiceStub,
        generateTestRequest(), new MessageExecutionContext(cacheStub, new NoOpAuthorizer()));

    assertTrue(result instanceof Success);

    verify(regionMock).put(TEST_KEY, TEST_VALUE);
    verify(regionMock, times(1)).put(anyString(), anyString());
  }

  @Test
  public void test_invalidEncodingType() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException,
      UnsupportedEncodingException, InvalidExecutionContextException {
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
    Result<RegionAPI.PutResponse> result = operationHandler.process(serializationServiceStub,
        putRequest, new MessageExecutionContext(cacheStub, new NoOpAuthorizer()));

    assertTrue(result instanceof Failure);
    assertEquals(ProtocolErrorCode.VALUE_ENCODING_ERROR.codeValue,
        result.getErrorMessage().getError().getErrorCode());
  }

  @Test
  public void test_RegionNotFound()
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException, InvalidExecutionContextException {
    when(cacheStub.getRegion(TEST_REGION)).thenReturn(null);
    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();
    Result<RegionAPI.PutResponse> result = operationHandler.process(serializationServiceStub,
        generateTestRequest(), new MessageExecutionContext(cacheStub, new NoOpAuthorizer()));

    assertTrue(result instanceof Failure);
    assertEquals(ProtocolErrorCode.REGION_NOT_FOUND.codeValue,
        result.getErrorMessage().getError().getErrorCode());
  }

  @Test
  public void test_RegionThrowsClasscastException()
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException, InvalidExecutionContextException {
    when(regionMock.put(any(), any())).thenThrow(ClassCastException.class);

    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();
    Result<RegionAPI.PutResponse> result = operationHandler.process(serializationServiceStub,
        generateTestRequest(), new MessageExecutionContext(cacheStub, new NoOpAuthorizer()));

    assertTrue(result instanceof Failure);
    assertEquals(ProtocolErrorCode.CONSTRAINT_VIOLATION.codeValue,
        result.getErrorMessage().getError().getErrorCode());
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
