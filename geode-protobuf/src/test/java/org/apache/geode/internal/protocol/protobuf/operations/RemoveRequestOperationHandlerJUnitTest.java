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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.Failure;
import org.apache.geode.internal.protocol.ProtobufTestExecutionContext;
import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.internal.protocol.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.internal.protocol.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RemoveRequestOperationHandlerJUnitTest extends OperationHandlerJUnitTest {
  private final String TEST_KEY = "my key";
  private final String TEST_VALUE = "my value";
  private final String TEST_REGION = "test region";
  private final String MISSING_REGION = "missing region";
  private final String MISSING_KEY = "missing key";
  private Region regionStub;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    regionStub = mock(Region.class);
    when(regionStub.remove(TEST_KEY)).thenReturn(TEST_VALUE);
    when(regionStub.containsKey(TEST_KEY)).thenReturn(true);
    when(regionStub.containsKey(MISSING_KEY)).thenReturn(false);

    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionStub);
    when(cacheStub.getRegion(MISSING_REGION)).thenReturn(null);
    operationHandler = new RemoveRequestOperationHandler();
  }

  @Test
  public void processValidKeyRemovesTheEntryAndReturnSuccess() throws Exception {
    RegionAPI.RemoveRequest removeRequest = generateTestRequest(false, false).getRemoveRequest();
    Result result = operationHandler.process(serializationServiceStub, removeRequest,
        ProtobufTestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Success);
    verify(regionStub).remove(TEST_KEY);
  }

  @Test
  public void processReturnsUnsucessfulResponseForInvalidRegion() throws Exception {
    RegionAPI.RemoveRequest removeRequest = generateTestRequest(true, false).getRemoveRequest();
    Result result = operationHandler.process(serializationServiceStub, removeRequest,
        ProtobufTestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Failure);
    ClientProtocol.ErrorResponse errorMessage =
        (ClientProtocol.ErrorResponse) result.getErrorMessage();
    assertEquals(ProtocolErrorCode.REGION_NOT_FOUND.codeValue,
        errorMessage.getError().getErrorCode());
  }

  @Test
  public void processReturnsSuccessWhenKeyIsNotFound() throws Exception {
    RegionAPI.RemoveRequest removeRequest = generateTestRequest(false, true).getRemoveRequest();
    Result result = operationHandler.process(serializationServiceStub, removeRequest,
        ProtobufTestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Success);
  }

  @Test
  public void processReturnsErrorWhenUnableToDecodeRequest() throws Exception {
    CodecNotRegisteredForTypeException exception =
        new CodecNotRegisteredForTypeException("error finding codec for type");
    when(serializationServiceStub.decode(any(), any())).thenThrow(exception);

    ByteString byteString = ByteString.copyFrom("{\"someKey\":\"someValue\"}", "UTF-8");
    BasicTypes.CustomEncodedValue.Builder customEncodedValueBuilder = BasicTypes.CustomEncodedValue
        .newBuilder().setEncodingType(BasicTypes.EncodingType.JSON).setValue(byteString);
    BasicTypes.EncodedValue encodedKey = BasicTypes.EncodedValue.newBuilder()
        .setCustomEncodedValue(customEncodedValueBuilder).build();

    RegionAPI.RemoveRequest removeRequest =
        ProtobufRequestUtilities.createRemoveRequest(TEST_REGION, encodedKey).getRemoveRequest();;
    Result result = operationHandler.process(serializationServiceStub, removeRequest,
        ProtobufTestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Failure);
    ClientProtocol.ErrorResponse errorMessage =
        (ClientProtocol.ErrorResponse) result.getErrorMessage();
    assertEquals(ProtocolErrorCode.VALUE_ENCODING_ERROR.codeValue,
        errorMessage.getError().getErrorCode());
  }

  private ClientProtocol.Request generateTestRequest(boolean missingRegion, boolean missingKey)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    String region = missingRegion ? MISSING_REGION : TEST_REGION;
    String key = missingKey ? MISSING_KEY : TEST_KEY;
    BasicTypes.EncodedValue testKey =
        ProtobufUtilities.createEncodedValue(serializationServiceStub, key);
    return ProtobufRequestUtilities.createRemoveRequest(region, testKey);
  }
}
