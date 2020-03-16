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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
public class RemoveRequestOperationHandlerJUnitTest
    extends OperationHandlerJUnitTest<RegionAPI.RemoveRequest, RegionAPI.RemoveResponse> {
  private static final String TEST_KEY = "my key";
  private static final String TEST_VALUE = "my value";
  private static final String TEST_REGION = "test region";
  private static final String MISSING_REGION = "missing region";
  private static final String MISSING_KEY = "missing key";
  private Region<String, String> regionStub;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    regionStub = mock(Region.class);
    when(regionStub.remove(TEST_KEY)).thenReturn(TEST_VALUE);
    when(regionStub.containsKey(TEST_KEY)).thenReturn(true);
    when(regionStub.containsKey(MISSING_KEY)).thenReturn(false);

    when(cacheStub.<String, String>getRegion(TEST_REGION)).thenReturn(regionStub);
    when(cacheStub.getRegion(MISSING_REGION)).thenReturn(null);
    operationHandler = new RemoveRequestOperationHandler();
  }

  @Test
  public void processValidKeyRemovesTheEntryAndReturnSuccess() throws Exception {
    RegionAPI.RemoveRequest removeRequest = generateTestRequest(false, false).getRemoveRequest();
    Result<?> result = operationHandler.process(serializationService, removeRequest,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertThat(result).isInstanceOf(Success.class);
    verify(regionStub).remove(TEST_KEY);
  }

  @Test
  public void processReturnsUnsuccessfulResponseForInvalidRegion() {
    RegionAPI.RemoveRequest removeRequest = generateTestRequest(true, false).getRemoveRequest();
    assertThatThrownBy(() -> operationHandler.process(serializationService, removeRequest,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub)))
            .isInstanceOf(RegionDestroyedException.class);
  }

  @Test
  public void processReturnsSuccessWhenKeyIsNotFound() throws Exception {
    RegionAPI.RemoveRequest removeRequest = generateTestRequest(false, true).getRemoveRequest();
    Result<?> result = operationHandler.process(serializationService, removeRequest,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub));

    assertThat(result).isInstanceOf(Success.class);
  }

  @Test
  public void processThrowsExceptionWhenUnableToDecodeRequest() {
    Exception exception = new DecodingException("error finding codec for type");
    ProtobufSerializationService serializationServiceStub =
        mock(ProtobufSerializationService.class);
    when(serializationServiceStub.decode(any())).thenThrow(exception);

    BasicTypes.EncodedValue encodedKey = BasicTypes.EncodedValue.newBuilder()
        .setJsonObjectResult("{\"someKey\":\"someValue\"}").build();

    RegionAPI.RemoveRequest removeRequest =
        ProtobufRequestUtilities.createRemoveRequest(TEST_REGION, encodedKey).getRemoveRequest();
    assertThatThrownBy(() -> operationHandler.process(serializationServiceStub, removeRequest,
        TestExecutionContext.getNoAuthCacheExecutionContext(cacheStub)))
            .isInstanceOf(DecodingException.class);
  }

  private ClientProtocol.Message generateTestRequest(boolean missingRegion, boolean missingKey)
      throws EncodingException {
    String region = missingRegion ? MISSING_REGION : TEST_REGION;
    String key = missingKey ? MISSING_KEY : TEST_KEY;
    BasicTypes.EncodedValue testKey = serializationService.encode(key);
    return ProtobufRequestUtilities.createRemoveRequest(region, testKey);
  }
}
