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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.Result;
import org.apache.geode.protocol.protobuf.Success;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.codec.StringCodec;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GetAllRequestOperationHandlerJUnitTest extends OperationHandlerJUnitTest {
  private static final String TEST_KEY1 = "my key1";
  private static final String TEST_VALUE1 = "my value1";
  private static final String TEST_KEY2 = "my key2";
  private static final String TEST_VALUE2 = "my value2";
  private static final String TEST_KEY3 = "my key3";
  private static final String TEST_VALUE3 = "my value3";
  private static final String TEST_REGION = "test region";
  private StringCodec stringDecoder;

  @Before
  public void setUp() throws Exception {
    super.setUp();

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

    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionStub);
    operationHandler = new GetAllRequestOperationHandler();
    stringDecoder = new StringCodec();
  }

  @Test
  public void processReturnsExpectedValuesForValidKeys()
      throws CodecAlreadyRegisteredForTypeException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException {
    Result<RegionAPI.GetAllResponse> result =
        operationHandler.process(serializationServiceStub, generateTestRequest(true), cacheStub);

    Assert.assertTrue(result instanceof Success);

    RegionAPI.GetAllResponse response = result.getMessage();

    Assert.assertEquals(3, response.getEntriesCount());

    List<BasicTypes.Entry> entriesList = response.getEntriesList();
    Map<String, String> responseEntries = convertEntryListToMap(entriesList);

    Assert.assertEquals(TEST_VALUE1, responseEntries.get(TEST_KEY1));
    Assert.assertEquals(TEST_VALUE2, responseEntries.get(TEST_KEY2));
    Assert.assertEquals(TEST_VALUE3, responseEntries.get(TEST_KEY3));
  }

  @Test
  public void processReturnsNoEntriesForNoKeysRequested()
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    Result<RegionAPI.GetAllResponse> result =
        operationHandler.process(serializationServiceStub, generateTestRequest(false), cacheStub);

    Assert.assertTrue(result instanceof Success);

    List<BasicTypes.Entry> entriesList = result.getMessage().getEntriesList();
    Map<String, String> responseEntries = convertEntryListToMap(entriesList);
    Assert.assertEquals(0, responseEntries.size());
  }

  private RegionAPI.GetAllRequest generateTestRequest(boolean addKeys)
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
      Assert.assertEquals(BasicTypes.EncodedValue.ValueCase.STRINGRESULT,
          encodedKey.getValueCase());
      String key = encodedKey.getStringResult();
      BasicTypes.EncodedValue encodedValue = entry.getValue();
      Assert.assertEquals(BasicTypes.EncodedValue.ValueCase.STRINGRESULT,
          encodedValue.getValueCase());
      String value = encodedValue.getStringResult();
      result.put(key, value);
    }
    return result;
  }
}
