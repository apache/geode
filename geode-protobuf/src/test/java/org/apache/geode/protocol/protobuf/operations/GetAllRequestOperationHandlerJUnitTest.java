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
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.codec.StringCodec;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.Charset;
import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class GetAllRequestOperationHandlerJUnitTest {
  private static final String TEST_KEY1 = "my key1";
  private static final String TEST_VALUE1 = "my value1";
  private static final String TEST_KEY2 = "my key2";
  private static final String TEST_VALUE2 = "my value2";
  private static final String TEST_KEY3 = "my key3";
  private static final String TEST_VALUE3 = "my value3";
  private static final String TEST_REGION = "test region";
  private Cache cacheStub;
  private SerializationService serializationServiceStub;
  private GetAllRequestOperationHandler operationHandler;
  private StringCodec stringDecoder;

  @Before
  public void setUp() throws Exception {
    serializationServiceStub = mock(SerializationService.class);
    addStringMockEncoding(serializationServiceStub, TEST_KEY1, true, true);
    addStringMockEncoding(serializationServiceStub, TEST_KEY2, true, true);
    addStringMockEncoding(serializationServiceStub, TEST_KEY3, true, true);
    addStringMockEncoding(serializationServiceStub, TEST_VALUE1, true, false);
    addStringMockEncoding(serializationServiceStub, TEST_VALUE2, true, false);
    addStringMockEncoding(serializationServiceStub, TEST_VALUE3, true, false);

    Region regionStub = mock(Region.class);
    when(regionStub.getAll(new HashSet<Object>() {
      {
        add(TEST_KEY1);
        add(TEST_KEY2);
        add(TEST_KEY3);
      }
    })).thenReturn(new HashMap() {
      {
        put(TEST_KEY1, TEST_VALUE1);
        put(TEST_KEY2, TEST_VALUE2);
        put(TEST_KEY3, TEST_VALUE3);
      }
    });

    cacheStub = mock(Cache.class);
    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionStub);
    operationHandler = new GetAllRequestOperationHandler();
    stringDecoder = new StringCodec();
  }

  private void addStringMockEncoding(SerializationService mock, String s, boolean add_encoding,
      boolean add_decoding) throws Exception {
    if (add_encoding) {
      when(mock.encode(BasicTypes.EncodingType.STRING, s))
          .thenReturn(s.getBytes(Charset.forName("UTF-8")));
    }
    if (add_decoding) {
      when(mock.decode(BasicTypes.EncodingType.STRING, s.getBytes(Charset.forName("UTF-8"))))
          .thenReturn(s);
    }
  }

  @Test
  public void processReturnsExpectedValuesForValidKeys()
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException {
    ClientProtocol.Request getRequest = generateTestRequest(true);
    ClientProtocol.Response response =
        operationHandler.process(serializationServiceStub, getRequest, cacheStub);

    Assert.assertEquals(ClientProtocol.Response.ResponseAPICase.GETALLRESPONSE,
        response.getResponseAPICase());
    RegionAPI.GetAllResponse getAllResponse = response.getGetAllResponse();
    Assert.assertEquals(3, getAllResponse.getEntriesCount());

    List<BasicTypes.Entry> entriesList = getAllResponse.getEntriesList();
    Map<String, String> responseEntries = convertEntryListToMap(entriesList);

    Assert.assertEquals(TEST_VALUE1, responseEntries.get(TEST_KEY1));
    Assert.assertEquals(TEST_VALUE2, responseEntries.get(TEST_KEY2));
    Assert.assertEquals(TEST_VALUE3, responseEntries.get(TEST_KEY3));
  }

  @Test
  public void processReturnsNoEntriesForNoKeysRequested()
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    ClientProtocol.Request getRequest = generateTestRequest(false);
    ClientProtocol.Response response =
        operationHandler.process(serializationServiceStub, getRequest, cacheStub);

    Assert.assertEquals(ClientProtocol.Response.ResponseAPICase.GETALLRESPONSE,
        response.getResponseAPICase());

    RegionAPI.GetAllResponse getAllResponse = response.getGetAllResponse();
    List<BasicTypes.Entry> entriesList = getAllResponse.getEntriesList();
    Map<String, String> responseEntries = convertEntryListToMap(entriesList);
    Assert.assertEquals(0, responseEntries.size());
  }

  private ClientProtocol.Request generateTestRequest(boolean addKeys)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    HashSet<BasicTypes.EncodedValue> testKeys = new HashSet<>();
    if (addKeys) {
      testKeys.add(ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_KEY1));
      testKeys.add(ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_KEY2));
      testKeys.add(ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_KEY3));
    }
    return ProtobufRequestUtilities.createGetAllRequest(TEST_REGION, testKeys);
  }

  private Map<String, String> convertEntryListToMap(List<BasicTypes.Entry> entriesList) {
    Map<String, String> result = new HashMap<>();
    for (BasicTypes.Entry entry : entriesList) {
      BasicTypes.EncodedValue encodedKey = entry.getKey();
      Assert.assertEquals(BasicTypes.EncodingType.STRING, encodedKey.getEncodingType());
      String key = stringDecoder.decode(encodedKey.getValue().toByteArray());
      BasicTypes.EncodedValue encodedValue = entry.getValue();
      Assert.assertEquals(BasicTypes.EncodingType.STRING, encodedValue.getEncodingType());
      String value = stringDecoder.decode(encodedValue.getValue().toByteArray());
      result.put(key, value);
    }
    return result;
  }
}
