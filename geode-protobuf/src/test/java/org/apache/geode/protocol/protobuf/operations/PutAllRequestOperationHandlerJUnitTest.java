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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.*;
import org.hamcrest.CoreMatchers;
import org.mockito.ArgumentMatcher;

@Category(UnitTest.class)
public class PutAllRequestOperationHandlerJUnitTest {
  private static final String TEST_KEY1 = "my key1";
  private static final String TEST_KEY2 = "my key2";
  private static final String TEST_KEY3 = "my key3";
  private static final String TEST_INVALID_KEY = "invalid key";
  private static final String TEST_VALUE1 = "my value1";
  private static final String TEST_VALUE2 = "my value2";
  private static final String TEST_VALUE3 = "my value3";
  private static final Integer TEST_INVALID_VALUE = 732;
  private static final String TEST_REGION = "test region";
  private static final String EXCEPTION_TEXT = "Simulating put failure";
  private Cache cacheStub;
  private SerializationService serializationServiceStub;
  private Region regionMock;

  class isAMapContainingInvalidKey implements ArgumentMatcher<Map> {
    @Override
    public boolean matches(Map argument) {
      return argument.containsKey(TEST_INVALID_KEY);
    }
  }

  @Before
  public void setUp() throws Exception {
    serializationServiceStub = mock(SerializationService.class);
    addStringStubEncoding(serializationServiceStub, TEST_KEY1);
    addStringStubEncoding(serializationServiceStub, TEST_KEY2);
    addStringStubEncoding(serializationServiceStub, TEST_KEY3);
    addStringStubEncoding(serializationServiceStub, TEST_INVALID_KEY);
    addStringStubEncoding(serializationServiceStub, TEST_VALUE1);
    addStringStubEncoding(serializationServiceStub, TEST_VALUE2);
    addStringStubEncoding(serializationServiceStub, TEST_VALUE3);
    when(serializationServiceStub.encode(BasicTypes.EncodingType.INT, TEST_INVALID_VALUE))
        .thenReturn(ByteBuffer.allocate(Integer.BYTES).putInt(TEST_INVALID_VALUE).array());
    when(serializationServiceStub.decode(BasicTypes.EncodingType.INT,
        ByteBuffer.allocate(Integer.BYTES).putInt(TEST_INVALID_VALUE).array()))
            .thenReturn(TEST_INVALID_VALUE);

    regionMock = mock(Region.class);

    doThrow(new ClassCastException(EXCEPTION_TEXT)).when(regionMock)
        .putAll(argThat(new isAMapContainingInvalidKey()));

    cacheStub = mock(Cache.class);
    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionMock);
  }

  private void addStringStubEncoding(SerializationService stub, String s) throws Exception {
    when(stub.encode(BasicTypes.EncodingType.STRING, s))
        .thenReturn(s.getBytes(Charset.forName("UTF-8")));
    when(stub.decode(BasicTypes.EncodingType.STRING, s.getBytes(Charset.forName("UTF-8"))))
        .thenReturn(s);
  }

  @Test
  public void processInsertsMultipleValidEntriesInCache() throws UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException, CodecAlreadyRegisteredForTypeException {
    PutAllRequestOperationHandler operationHandler = new PutAllRequestOperationHandler();

    ClientProtocol.Response response = operationHandler.process(serializationServiceStub,
        generateTestRequest(false, true), cacheStub);

    Assert.assertEquals(ClientProtocol.Response.ResponseAPICase.PUTALLRESPONSE,
        response.getResponseAPICase());

    HashMap<Object, Object> expectedValues = new HashMap<>();
    expectedValues.put(TEST_KEY1, TEST_VALUE1);
    expectedValues.put(TEST_KEY2, TEST_VALUE2);
    expectedValues.put(TEST_KEY3, TEST_VALUE3);

    verify(regionMock).putAll(expectedValues);
  }

  @Test
  public void processWithInvalidEntryReturnsError() throws Exception {
    PutAllRequestOperationHandler operationHandler = new PutAllRequestOperationHandler();

    ClientProtocol.Response response = operationHandler.process(serializationServiceStub,
        generateTestRequest(true, true), cacheStub);

    Assert.assertEquals(ClientProtocol.Response.ResponseAPICase.ERRORRESPONSE,
        response.getResponseAPICase());
    Assert.assertThat(response.getErrorResponse().getMessage(),
        CoreMatchers.containsString(EXCEPTION_TEXT));
    // can't verify anything about put keys because we make no guarantees.
  }

  @Test
  public void processWithNoEntriesPasses() throws Exception {
    PutAllRequestOperationHandler operationHandler = new PutAllRequestOperationHandler();

    ClientProtocol.Response response = operationHandler.process(serializationServiceStub,
        generateTestRequest(false, false), cacheStub);

    Assert.assertEquals(ClientProtocol.Response.ResponseAPICase.PUTALLRESPONSE,
        response.getResponseAPICase());

    verify(regionMock, times(0)).put(any(), any());
  }

  private ClientProtocol.Request generateTestRequest(boolean addInvalidKey, boolean addValidKeys)
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
    return ProtobufRequestUtilities.createPutAllRequest(TEST_REGION, entries);
  }
}
