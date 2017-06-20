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
package org.apache.geode.protocol.operations.protobuf;

import com.google.protobuf.ByteString;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.operations.GetRequestOperationHandler;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.codec.StringCodec;
import org.apache.geode.protocol.protobuf.EncodingTypeTranslator;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.SerializationCodecRegistry;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.Charset;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class GetRequestOperationHandlerTest {
  public static final String TEST_KEY = "my key";
  public static final String TEST_VALUE = "my value";
  public static final String TEST_REGION = "test region";
  public Cache cacheStub;
  public SerializationService serializationServiceStub;

  @Before
  public void setUp() throws Exception {
    serializationServiceStub = mock(SerializationService.class);
    when(serializationServiceStub.decode(BasicTypes.EncodingType.STRING,
        TEST_KEY.getBytes(Charset.forName("UTF-8")))).thenReturn(TEST_KEY);
    when(serializationServiceStub.encode(BasicTypes.EncodingType.STRING, TEST_VALUE))
        .thenReturn(TEST_VALUE.getBytes(Charset.forName("UTF-8")));

    Region regionStub = mock(Region.class);
    when(regionStub.get(TEST_KEY)).thenReturn(TEST_VALUE);

    cacheStub = mock(Cache.class);
    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionStub);
  }

  @Test
  public void processReturnsTheEncodedValueFromTheRegion() throws UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException, CodecAlreadyRegisteredForTypeException {
    GetRequestOperationHandler operationHandler = new GetRequestOperationHandler();

    RegionAPI.GetResponse response =
        operationHandler.process(serializationServiceStub, makeGetRequest(), cacheStub);

    Assert.assertEquals(BasicTypes.EncodingType.STRING, response.getResult().getEncodingType());
    String actualValue = getStringCodec().decode(response.getResult().getValue().toByteArray());
    Assert.assertEquals(TEST_VALUE, actualValue);
  }

  private RegionAPI.GetRequest makeGetRequest() throws CodecNotRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecAlreadyRegisteredForTypeException {
    StringCodec stringCodec = getStringCodec();
    RegionAPI.GetRequest.Builder getRequestBuilder = RegionAPI.GetRequest.newBuilder();
    getRequestBuilder.setRegionName(TEST_REGION)
        .setKey(BasicTypes.EncodedValue.newBuilder().setEncodingType(BasicTypes.EncodingType.STRING)
            .setValue(ByteString.copyFrom(stringCodec.encode(TEST_KEY))));

    return getRequestBuilder.build();
  }

  private StringCodec getStringCodec() throws CodecAlreadyRegisteredForTypeException,
      CodecNotRegisteredForTypeException, UnsupportedEncodingTypeException {
    SerializationCodecRegistry serializationCodecRegistry = new SerializationCodecRegistry();
    return (StringCodec) serializationCodecRegistry.getCodecForType(
        EncodingTypeTranslator.getSerializationTypeForEncodingType(BasicTypes.EncodingType.STRING));
  }
}
