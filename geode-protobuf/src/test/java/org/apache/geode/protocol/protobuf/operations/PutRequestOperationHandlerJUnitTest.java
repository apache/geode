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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.Charset;

import static org.mockito.Mockito.*;

@Category(UnitTest.class)
public class PutRequestOperationHandlerJUnitTest {
  public static final String TEST_KEY = "my key";
  public static final String TEST_VALUE = "99";
  public static final String TEST_REGION = "test region";
  public Cache cacheStub;
  public SerializationService serializationServiceStub;
  private Region regionMock;

  @Before
  public void setUp() throws Exception {
    serializationServiceStub = mock(SerializationService.class);
    when(serializationServiceStub.decode(BasicTypes.EncodingType.STRING,
        TEST_KEY.getBytes(Charset.forName("UTF-8")))).thenReturn(TEST_KEY);
    when(serializationServiceStub.decode(BasicTypes.EncodingType.STRING,
        TEST_VALUE.getBytes(Charset.forName("UTF-8")))).thenReturn(TEST_VALUE);
    when(serializationServiceStub.encode(BasicTypes.EncodingType.STRING, TEST_KEY))
        .thenReturn(TEST_KEY.getBytes(Charset.forName("UTF-8")));
    when(serializationServiceStub.encode(BasicTypes.EncodingType.STRING, TEST_VALUE))
        .thenReturn(TEST_VALUE.getBytes(Charset.forName("UTF-8")));

    regionMock = mock(Region.class);
    when(regionMock.put(TEST_KEY, TEST_VALUE)).thenReturn(1);

    cacheStub = mock(Cache.class);
    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionMock);
  }

  @Test
  public void test_puttingTheEncodedEntryIntoRegion() throws UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException, CodecAlreadyRegisteredForTypeException {
    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();
    ClientProtocol.Response response =
        operationHandler.process(serializationServiceStub, generateTestRequest(), cacheStub);

    Assert.assertEquals(ClientProtocol.Response.ResponseAPICase.PUTRESPONSE,
        response.getResponseAPICase());

    verify(regionMock).put(TEST_KEY, TEST_VALUE);
    verify(regionMock, times(1)).put(anyString(), anyString());
  }

  @Test
  public void test_invalidEncodingType() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    UnsupportedEncodingTypeException exception =
        new UnsupportedEncodingTypeException("unsupported type!");
    when(serializationServiceStub.decode(BasicTypes.EncodingType.STRING,
        TEST_KEY.getBytes(Charset.forName("UTF-8")))).thenThrow(exception);
    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();

    ClientProtocol.Response response =
        operationHandler.process(serializationServiceStub, generateTestRequest(), cacheStub);

    Assert.assertEquals(ClientProtocol.Response.ResponseAPICase.ERRORRESPONSE,
        response.getResponseAPICase());
  }

  @Test
  public void test_codecNotRegistered() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    CodecNotRegisteredForTypeException exception =
        new CodecNotRegisteredForTypeException("error finding codec for type");
    when(serializationServiceStub.decode(BasicTypes.EncodingType.STRING,
        TEST_KEY.getBytes(Charset.forName("UTF-8")))).thenThrow(exception);
    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();

    ClientProtocol.Response response =
        operationHandler.process(serializationServiceStub, generateTestRequest(), cacheStub);

    Assert.assertEquals(ClientProtocol.Response.ResponseAPICase.ERRORRESPONSE,
        response.getResponseAPICase());
  }

  @Test
  public void test_RegionNotFound() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    when(cacheStub.getRegion(TEST_REGION)).thenReturn(null);
    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();
    ClientProtocol.Response response =
        operationHandler.process(serializationServiceStub, generateTestRequest(), cacheStub);

    Assert.assertEquals(ClientProtocol.Response.ResponseAPICase.ERRORRESPONSE,
        response.getResponseAPICase());
  }

  @Test
  public void test_RegionThrowsClasscastException() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    when(regionMock.put(any(), any())).thenThrow(ClassCastException.class);

    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();
    ClientProtocol.Response response =
        operationHandler.process(serializationServiceStub, generateTestRequest(), cacheStub);

    Assert.assertEquals(ClientProtocol.Response.ResponseAPICase.ERRORRESPONSE,
        response.getResponseAPICase());
  }

  private ClientProtocol.Request generateTestRequest()
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    BasicTypes.EncodedValue testKey =
        ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_KEY);
    BasicTypes.EncodedValue testValue =
        ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_VALUE);
    BasicTypes.Entry testEntry = ProtobufUtilities.createEntry(testKey, testValue);
    return ProtobufRequestUtilities.createPutRequest(TEST_REGION, testEntry);
  }
}
