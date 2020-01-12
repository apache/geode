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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.internal.protocol.TestExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class RemoveRequestOperationHandlerJUnitTest extends OperationHandlerJUnitTest {
  private final String TEST_KEY = "my key";
  private final String TEST_VALUE = "my value";
  private final String TEST_REGION = "test region";
  private final String MISSING_REGION = "missing region";
  private final String MISSING_KEY = "missing key";
  private Region regionStub;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
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
    Result result = operationHandler.process(serializationService, removeRequest,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Success);
    verify(regionStub).remove(TEST_KEY);
  }

  @Test
  public void processReturnsUnsucessfulResponseForInvalidRegion() throws Exception {
    RegionAPI.RemoveRequest removeRequest = generateTestRequest(true, false).getRemoveRequest();
    expectedException.expect(RegionDestroyedException.class);
    Result result = operationHandler.process(serializationService, removeRequest,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));
  }

  @Test
  public void processReturnsSuccessWhenKeyIsNotFound() throws Exception {
    RegionAPI.RemoveRequest removeRequest = generateTestRequest(false, true).getRemoveRequest();
    Result result = operationHandler.process(serializationService, removeRequest,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertTrue(result instanceof Success);
  }

  @Test(expected = DecodingException.class)
  public void processThrowsExceptionWhenUnableToDecodeRequest() throws Exception {
    Exception exception = new DecodingException("error finding codec for type");
    ProtobufSerializationService serializationServiceStub =
        mock(ProtobufSerializationService.class);
    when(serializationServiceStub.decode(any())).thenThrow(exception);

    BasicTypes.EncodedValue encodedKey = BasicTypes.EncodedValue.newBuilder()
        .setJsonObjectResult("{\"someKey\":\"someValue\"}").build();

    RegionAPI.RemoveRequest removeRequest =
        ProtobufRequestUtilities.createRemoveRequest(TEST_REGION, encodedKey).getRemoveRequest();;
    operationHandler.process(serializationServiceStub, removeRequest,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));
  }

  private ClientProtocol.Message generateTestRequest(boolean missingRegion, boolean missingKey)
      throws EncodingException {
    String region = missingRegion ? MISSING_REGION : TEST_REGION;
    String key = missingKey ? MISSING_KEY : TEST_KEY;
    BasicTypes.EncodedValue testKey = serializationService.encode(key);
    return ProtobufRequestUtilities.createRemoveRequest(region, testKey);
  }
}
