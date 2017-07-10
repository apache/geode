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
package org.apache.geode.protocol;

import com.google.protobuf.ByteString;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.protocol.protobuf.*;
import org.apache.geode.serialization.codec.StringCodec;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.SerializationCodecRegistry;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@Category(UnitTest.class)
public class IntegrationJUnitTest {

  public static final String TEST_KEY = "my key";
  public static final String TEST_VALUE = "my value";
  public static final String TEST_REGION = "test region";
  private StringCodec stringCodec;
  private Cache cacheStub;
  private Region regionStub;

  @Before
  public void setup() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    SerializationCodecRegistry serializationCodecRegistry = new SerializationCodecRegistry();
    stringCodec = (StringCodec) serializationCodecRegistry.getCodecForType(
        EncodingTypeTranslator.getSerializationTypeForEncodingType(BasicTypes.EncodingType.STRING));
    regionStub = getRegionStub();
    cacheStub = getCacheStub(regionStub);
  }

  @Test
  public void testGetRequestProcessed() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    ProtobufStreamProcessor streamProcessor = new ProtobufStreamProcessor();
    streamProcessor.processOneMessage(getInputStream(getRequest(stringCodec)), outputStream,
        cacheStub);

    RegionAPI.GetResponse getResponse = getGetResponse(outputStream);

    Assert.assertNotNull(getResponse);
    Assert.assertEquals(BasicTypes.EncodingType.STRING, getResponse.getResult().getEncodingType());
    String actualValue = stringCodec.decode(getResponse.getResult().getValue().toByteArray());
    Assert.assertEquals(TEST_VALUE, actualValue);
    verify(regionStub, times(1)).get(TEST_KEY);
    verify(regionStub, times(1)).get(anyString());
  }

  private Region getRegionStub() {
    regionStub = mock(Region.class);
    when(regionStub.get(TEST_KEY)).thenReturn(TEST_VALUE);
    return regionStub;
  }

  private Cache getCacheStub(Region region) {
    Cache cacheStub = mock(Cache.class);
    when(cacheStub.getRegion(TEST_REGION)).thenReturn(region);
    return cacheStub;
  }

  private RegionAPI.GetResponse getGetResponse(ByteArrayOutputStream outputStream)
      throws IOException {
    ByteArrayInputStream helperInputStream = new ByteArrayInputStream(outputStream.toByteArray());
    ClientProtocol.Message responseMessage =
        ClientProtocol.Message.parseDelimitedFrom(helperInputStream);
    ClientProtocol.Response response = responseMessage.getResponse();
    return response.getGetResponse();
  }

  private ByteArrayInputStream getInputStream(ClientProtocol.Message request) throws IOException {
    ByteArrayOutputStream helperOutputStream = new ByteArrayOutputStream();
    request.writeDelimitedTo(helperOutputStream);
    return new ByteArrayInputStream(helperOutputStream.toByteArray());
  }

  private ClientProtocol.Message getRequest(StringCodec stringCodec) {
    RegionAPI.GetRequest.Builder getRequestBuilder = RegionAPI.GetRequest.newBuilder();
    getRequestBuilder.setRegionName(TEST_REGION)
        .setKey(BasicTypes.EncodedValue.newBuilder().setEncodingType(BasicTypes.EncodingType.STRING)
            .setValue(ByteString.copyFrom(stringCodec.encode(TEST_KEY))));
    ClientProtocol.Request request =
        ClientProtocol.Request.newBuilder().setGetRequest(getRequestBuilder).build();
    ClientProtocol.Message requestMessage = ClientProtocol.Message.newBuilder()
        .setMessageHeader(ClientProtocol.MessageHeader.newBuilder()).setRequest(request).build();

    return requestMessage;
  }
}
