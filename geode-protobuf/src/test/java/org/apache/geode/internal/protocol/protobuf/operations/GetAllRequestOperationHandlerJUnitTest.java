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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.ProtobufTestExecutionContext;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.internal.protocol.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.internal.protocol.serialization.registry.exception.CodecNotRegisteredForTypeException;
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
  private static final String TEST_INVALID_KEY = "I'm a naughty key!";
  private static final String NO_VALUE_PRESENT_FOR_THIS_KEY = "no value present for this key";
  private Region regionStub;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    regionStub = mock(Region.class);
    when(regionStub.get(TEST_KEY1)).thenReturn(TEST_VALUE1);
    when(regionStub.get(TEST_KEY2)).thenReturn(TEST_VALUE2);
    when(regionStub.get(TEST_KEY3)).thenReturn(TEST_VALUE3);
    when(regionStub.get(NO_VALUE_PRESENT_FOR_THIS_KEY)).thenReturn(null);
    when(regionStub.get(TEST_INVALID_KEY))
        .thenThrow(new CacheLoaderException("Let's pretend that didn't work"));

    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionStub);
    operationHandler = new GetAllRequestOperationHandler();
  }

  @Test
  public void processReturnsExpectedValuesForValidKeys() throws Exception {
    Result result =
        operationHandler.process(serializationServiceStub, generateTestRequest(true, false),
            ProtobufTestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Success);

    RegionAPI.GetAllResponse response = (RegionAPI.GetAllResponse) result.getMessage();

    assertEquals(3, response.getEntriesCount());

    List<BasicTypes.Entry> entriesList = response.getEntriesList();
    Map<String, String> responseEntries = convertEntryListToMap(entriesList);

    assertEquals(TEST_VALUE1, responseEntries.get(TEST_KEY1));
    assertEquals(TEST_VALUE2, responseEntries.get(TEST_KEY2));
    assertEquals(TEST_VALUE3, responseEntries.get(TEST_KEY3));
  }

  @Test
  public void processReturnsNoEntriesForNoKeysRequested() throws UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException, InvalidExecutionContextException {
    Result result =
        operationHandler.process(serializationServiceStub, generateTestRequest(false, false),
            ProtobufTestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Success);

    RegionAPI.GetAllResponse response = (RegionAPI.GetAllResponse) result.getMessage();
    List<BasicTypes.Entry> entriesList = response.getEntriesList();
    Map<String, String> responseEntries = convertEntryListToMap(entriesList);
    assertEquals(0, responseEntries.size());
  }

  @Test
  public void singeNullKey() throws Exception {
    HashSet<BasicTypes.EncodedValue> testKeys = new HashSet<>();
    testKeys.add(ProtobufUtilities.createEncodedValue(serializationServiceStub,
        NO_VALUE_PRESENT_FOR_THIS_KEY));
    RegionAPI.GetAllRequest getAllRequest =
        ProtobufRequestUtilities.createGetAllRequest(TEST_REGION, testKeys);
    Result result = operationHandler.process(serializationServiceStub, getAllRequest,
        ProtobufTestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Success);
    RegionAPI.GetAllResponse message = (RegionAPI.GetAllResponse) result.getMessage();
    assertEquals(1, message.getEntriesCount());
    assertFalse(message.getEntries(0).hasValue());
    assertEquals(NO_VALUE_PRESENT_FOR_THIS_KEY, message.getEntries(0).getKey().getStringResult());

    verify(regionStub, times(1)).get(NO_VALUE_PRESENT_FOR_THIS_KEY);
  }

  @Test
  public void multipleKeysWhereOneThrows() throws UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException, InvalidExecutionContextException {
    Result result =
        operationHandler.process(serializationServiceStub, generateTestRequest(true, true),
            ProtobufTestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Success);

    RegionAPI.GetAllResponse response = (RegionAPI.GetAllResponse) result.getMessage();

    assertEquals(3, response.getEntriesCount());

    List<BasicTypes.Entry> entriesList = response.getEntriesList();
    Map<String, String> responseEntries = convertEntryListToMap(entriesList);

    assertEquals(TEST_VALUE1, responseEntries.get(TEST_KEY1));
    assertEquals(TEST_VALUE2, responseEntries.get(TEST_KEY2));
    assertEquals(TEST_VALUE3, responseEntries.get(TEST_KEY3));

    assertEquals(1, response.getFailuresCount());
    assertEquals(TEST_INVALID_KEY, response.getFailures(0).getKey().getStringResult());
  }

  private RegionAPI.GetAllRequest generateTestRequest(boolean addKeys, boolean useInvalid)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    HashSet<BasicTypes.EncodedValue> testKeys = new HashSet<>();
    if (addKeys) {
      testKeys.add(ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_KEY1));
      testKeys.add(ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_KEY2));
      testKeys.add(ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_KEY3));
      if (useInvalid) {
        testKeys
            .add(ProtobufUtilities.createEncodedValue(serializationServiceStub, TEST_INVALID_KEY));
      }
    }
    return ProtobufRequestUtilities.createGetAllRequest(TEST_REGION, testKeys);
  }

  private Map<String, String> convertEntryListToMap(List<BasicTypes.Entry> entriesList) {
    Map<String, String> result = new HashMap<>();
    for (BasicTypes.Entry entry : entriesList) {
      BasicTypes.EncodedValue encodedKey = entry.getKey();
      assertEquals(BasicTypes.EncodedValue.ValueCase.STRINGRESULT, encodedKey.getValueCase());
      String key = encodedKey.getStringResult();
      BasicTypes.EncodedValue encodedValue = entry.getValue();
      assertEquals(BasicTypes.EncodedValue.ValueCase.STRINGRESULT, encodedValue.getValueCase());
      String value = encodedValue.getStringResult();
      result.put(key, value);
    }
    return result;
  }
}
