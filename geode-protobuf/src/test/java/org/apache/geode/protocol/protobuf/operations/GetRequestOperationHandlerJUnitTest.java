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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.Failure;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.Result;
import org.apache.geode.protocol.protobuf.Success;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.codec.StringCodec;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.UnitTest;
import org.mockito.internal.matchers.Any;

@Category(UnitTest.class)
public class GetRequestOperationHandlerJUnitTest extends OperationHandlerJUnitTest {
  private final String TEST_KEY = "my key";
  private final String TEST_VALUE = "my value";
  private final String TEST_REGION = "test region";
  private final String MISSING_REGION = "missing region";
  private final String MISSING_KEY = "missing key";
  private final String NULLED_KEY = "nulled key";
  private StringCodec stringDecoder;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    Region regionStub = mock(Region.class);
    when(regionStub.get(TEST_KEY)).thenReturn(TEST_VALUE);
    when(regionStub.get(MISSING_KEY)).thenReturn(null);
    when(regionStub.get(NULLED_KEY)).thenReturn(null);
    when(regionStub.containsKey(MISSING_KEY)).thenReturn(false);
    when(regionStub.containsKey(NULLED_KEY)).thenReturn(true);

    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionStub);
    when(cacheStub.getRegion(MISSING_REGION)).thenReturn(null);
    operationHandler = new GetRequestOperationHandler();
    stringDecoder = new StringCodec();
  }

  @Test
  public void processReturnsTheEncodedValueFromTheRegion()
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException {
    RegionAPI.GetRequest getRequest = generateTestRequest(false, false, false);
    Result<RegionAPI.GetResponse> result =
        operationHandler.process(serializationServiceStub, getRequest, cacheStub);

    Assert.assertTrue(result instanceof Success);
    Assert.assertEquals(BasicTypes.EncodedValue.ValueCase.STRINGRESULT,
        result.getMessage().getResult().getValueCase());
    String actualValue = result.getMessage().getResult().getStringResult();
    Assert.assertEquals(TEST_VALUE, actualValue);
  }

  @Test
  public void processReturnsUnsucessfulResponseForInvalidRegion()
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException {
    RegionAPI.GetRequest getRequest = generateTestRequest(true, false, false);
    Result<RegionAPI.GetResponse> response =
        operationHandler.process(serializationServiceStub, getRequest, cacheStub);

    Assert.assertTrue(response instanceof Failure);
    Assert.assertEquals("Region not found", response.getErrorMessage().getMessage());
  }

  @Test
  public void processReturnsKeyNotFoundWhenKeyIsNotFound()
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException {
    RegionAPI.GetRequest getRequest = generateTestRequest(false, true, false);
    Result<RegionAPI.GetResponse> response =
        operationHandler.process(serializationServiceStub, getRequest, cacheStub);

    Assert.assertTrue(response instanceof Success);
  }

  @Test
  public void processReturnsLookupFailureWhenKeyFoundWithNoValue()
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException {
    RegionAPI.GetRequest getRequest = generateTestRequest(false, false, true);
    Result<RegionAPI.GetResponse> response =
        operationHandler.process(serializationServiceStub, getRequest, cacheStub);

    Assert.assertTrue(response instanceof Success);
  }

  @Test
  public void processReturnsErrorWhenUnableToDecodeRequest()
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException, UnsupportedEncodingException {
    CodecNotRegisteredForTypeException exception =
        new CodecNotRegisteredForTypeException("error finding codec for type");
    when(serializationServiceStub.decode(any(), any())).thenThrow(exception);

    ByteString byteString = ByteString.copyFrom("{\"someKey\":\"someValue\"}", "UTF-8");
    BasicTypes.CustomEncodedValue.Builder customEncodedValueBuilder = BasicTypes.CustomEncodedValue
        .newBuilder().setEncodingType(BasicTypes.EncodingType.JSON).setValue(byteString);
    BasicTypes.EncodedValue encodedKey = BasicTypes.EncodedValue.newBuilder()
        .setCustomEncodedValue(customEncodedValueBuilder).build();
    RegionAPI.GetRequest getRequest =
        ProtobufRequestUtilities.createGetRequest(TEST_REGION, encodedKey).getGetRequest();
    Result<RegionAPI.GetResponse> response =
        operationHandler.process(serializationServiceStub, getRequest, cacheStub);

    Assert.assertTrue(response instanceof Failure);
    Assert.assertEquals("Codec error in protobuf deserialization.",
        response.getErrorMessage().getMessage());
  }

  private RegionAPI.GetRequest generateTestRequest(boolean missingRegion, boolean missingKey,
      boolean nulledKey)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    String region = missingRegion ? MISSING_REGION : TEST_REGION;
    String key = missingKey ? MISSING_KEY : (nulledKey ? NULLED_KEY : TEST_KEY);
    BasicTypes.EncodedValue testKey =
        ProtobufUtilities.createEncodedValue(serializationServiceStub, key);
    return ProtobufRequestUtilities.createGetRequest(region, testKey).getGetRequest();
  }
}
