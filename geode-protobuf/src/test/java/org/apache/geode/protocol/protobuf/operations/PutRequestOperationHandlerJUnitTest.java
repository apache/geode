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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;

import org.hamcrest.CoreMatchers;
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
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.dunit.Assert;
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

    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionMock);
  }

  @Test
  public void test_puttingTheEncodedEntryIntoRegion() throws UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException, CodecAlreadyRegisteredForTypeException {
    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();
    Result<RegionAPI.PutResponse> result =
        operationHandler.process(serializationServiceStub, generateTestRequest(), cacheStub);

    Assert.assertTrue(result instanceof Success);

    verify(regionMock).put(TEST_KEY, TEST_VALUE);
    verify(regionMock, times(1)).put(anyString(), anyString());
  }

  @Test
  public void test_invalidEncodingType() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    String exceptionText = "unsupported type!";
    UnsupportedEncodingTypeException exception =
        new UnsupportedEncodingTypeException(exceptionText);
    when(serializationServiceStub.decode(BasicTypes.EncodingType.STRING,
        TEST_KEY.getBytes(Charset.forName("UTF-8")))).thenThrow(exception);
    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();

    Result<RegionAPI.PutResponse> result =
        operationHandler.process(serializationServiceStub, generateTestRequest(), cacheStub);

    Assert.assertTrue(result instanceof Failure);
    org.junit.Assert.assertEquals(exceptionText, result.getErrorMessage().getMessage());
  }

  @Test
  public void test_codecNotRegistered() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    String exceptionMessage = "error finding codec for type";
    CodecNotRegisteredForTypeException exception =
        new CodecNotRegisteredForTypeException(exceptionMessage);
    when(serializationServiceStub.decode(BasicTypes.EncodingType.STRING,
        TEST_KEY.getBytes(Charset.forName("UTF-8")))).thenThrow(exception);
    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();

    Result<RegionAPI.PutResponse> result =
        operationHandler.process(serializationServiceStub, generateTestRequest(), cacheStub);

    Assert.assertTrue(result instanceof Failure);
    org.junit.Assert.assertThat(result.getErrorMessage().getMessage(),
        CoreMatchers.containsString(exceptionMessage));
  }

  @Test
  public void test_RegionNotFound() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    when(cacheStub.getRegion(TEST_REGION)).thenReturn(null);
    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();
    Result<RegionAPI.PutResponse> result =
        operationHandler.process(serializationServiceStub, generateTestRequest(), cacheStub);

    Assert.assertTrue(result instanceof Failure);
    org.junit.Assert.assertEquals("Region passed by client did not exist: test region",
        result.getErrorMessage().getMessage());
  }

  @Test
  public void test_RegionThrowsClasscastException() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    when(regionMock.put(any(), any())).thenThrow(ClassCastException.class);

    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();
    Result<RegionAPI.PutResponse> result =
        operationHandler.process(serializationServiceStub, generateTestRequest(), cacheStub);

    Assert.assertTrue(result instanceof Failure);
    org.junit.Assert.assertThat(result.getErrorMessage().getMessage(),
        CoreMatchers.containsString("invalid key or value type"));
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
