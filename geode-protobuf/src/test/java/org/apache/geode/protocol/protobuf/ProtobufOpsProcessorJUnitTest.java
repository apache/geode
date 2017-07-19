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
package org.apache.geode.protocol.protobuf;


import org.apache.geode.cache.Cache;
import org.apache.geode.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.protocol.operations.OperationHandler;
import org.apache.geode.protocol.operations.registry.OperationsHandlerRegistry;
import org.apache.geode.protocol.operations.registry.exception.OperationHandlerNotRegisteredException;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class ProtobufOpsProcessorJUnitTest {
  @Test
  public void testOpsProcessor()
      throws OperationHandlerNotRegisteredException, InvalidProtocolMessageException {
    OperationsHandlerRegistry opsHandlerRegistryStub = mock(OperationsHandlerRegistry.class);
    OperationHandler operationHandlerStub = mock(OperationHandler.class);
    SerializationService serializationServiceStub = mock(SerializationService.class);
    Cache dummyCache = mock(Cache.class);
    int operationID = ClientProtocol.Request.RequestAPICase.GETREQUEST.getNumber();

    ClientProtocol.Request messageRequest = ClientProtocol.Request.newBuilder()
        .setGetRequest(RegionAPI.GetRequest.newBuilder()).build();

    ClientProtocol.Response expectedResponse = ClientProtocol.Response.newBuilder()
        .setGetResponse((RegionAPI.GetResponse.newBuilder())).build();

    when(opsHandlerRegistryStub.getOperationHandlerForOperationId(operationID))
        .thenReturn(operationHandlerStub);
    when(operationHandlerStub.process(serializationServiceStub, messageRequest, dummyCache))
        .thenReturn(expectedResponse);

    ProtobufOpsProcessor processor =
        new ProtobufOpsProcessor(opsHandlerRegistryStub, serializationServiceStub);
    ClientProtocol.Response response = processor.process(messageRequest, dummyCache);
    Assert.assertEquals(expectedResponse, response);
  }
}
