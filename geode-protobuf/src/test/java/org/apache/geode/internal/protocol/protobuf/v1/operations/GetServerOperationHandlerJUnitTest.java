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

import static org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.ErrorCode.NO_AVAILABLE_SERVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.internal.locator.ClientConnectionRequest;
import org.apache.geode.cache.client.internal.locator.ClientConnectionResponse;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.ServerLocator;
import org.apache.geode.internal.protocol.TestExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.LocatorAPI.GetServerRequest;
import org.apache.geode.internal.protocol.protobuf.v1.LocatorAPI.GetServerResponse;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class GetServerOperationHandlerJUnitTest
    extends OperationHandlerJUnitTest<GetServerRequest, GetServerResponse> {
  final String HOSTNAME = "hostname";
  final int PORT = 12345;
  final String EXISTENT_GROUP = "existent";
  final String NONEXISTENT_GROUP = "nonexistent";
  InternalLocator internalLocatorMock;
  ServerLocator serverLocatorAdviseeMock;

  @Before
  public void setUp() {
    operationHandler = new GetServerOperationHandler();
    internalLocatorMock = mock(InternalLocator.class);
    serverLocatorAdviseeMock = mock(ServerLocator.class);

    when(internalLocatorMock.getServerLocatorAdvisee()).thenReturn(serverLocatorAdviseeMock);
  }

  @Test
  public void testServerReturnedFromHandler() throws Exception {
    when(serverLocatorAdviseeMock.processRequest(any(Object.class)))
        .thenReturn(new ClientConnectionResponse(new ServerLocation(HOSTNAME, PORT)));

    GetServerRequest getServerRequest =
        ProtobufRequestUtilities.createGetServerRequest();
    Result<?> result = getOperationHandlerResult(getServerRequest);
    assertThat(result).isInstanceOf(Success.class);
    validateGetServerResponse((GetServerResponse) result.getMessage());
  }

  @Test
  public void testErrorReturnedWhenNoServers() throws Exception {
    when(serverLocatorAdviseeMock.processRequest(any(Object.class))).thenReturn(null);

    GetServerRequest getServerRequest =
        ProtobufRequestUtilities.createGetServerRequest();
    Result<?> result = getOperationHandlerResult(getServerRequest);
    assertThat(result).isInstanceOf(Failure.class);
    Failure<?> failure = (Failure<?>) result;
    ClientProtocol.ErrorResponse errorResponse = failure.getErrorMessage();
    assertThat(errorResponse.getError().getErrorCode()).isEqualTo(NO_AVAILABLE_SERVER);
  }

  @Test
  public void testServerReturnedForExistentGroup() throws Exception {
    when(
        serverLocatorAdviseeMock.processRequest(new ClientConnectionRequest(any(), EXISTENT_GROUP)))
            .thenReturn(new ClientConnectionResponse(new ServerLocation(HOSTNAME, PORT)));

    GetServerRequest getServerRequest =
        ProtobufRequestUtilities.createGetServerRequest(EXISTENT_GROUP);
    Result<?> result = getOperationHandlerResult(getServerRequest);
    assertThat(result).isInstanceOf(Success.class);
    validateGetServerResponse((GetServerResponse) result.getMessage());
  }

  @Test
  public void testErrorReturnedForNonexistentGroup() throws Exception {
    when(serverLocatorAdviseeMock
        .processRequest(new ClientConnectionRequest(any(), NONEXISTENT_GROUP)))
            .thenReturn(new ClientConnectionResponse(null));

    GetServerRequest getServerRequest =
        ProtobufRequestUtilities.createGetServerRequest(NONEXISTENT_GROUP);
    Result<?> result = getOperationHandlerResult(getServerRequest);
    assertThat(result).isInstanceOf(Failure.class);
    Failure<?> failure = (Failure<?>) result;
    ClientProtocol.ErrorResponse errorResponse = failure.getErrorMessage();
    assertThat(errorResponse.getError().getErrorCode()).isEqualTo(NO_AVAILABLE_SERVER);
    assertThat(errorResponse.getError().getMessage()).contains(NONEXISTENT_GROUP);
  }

  private Result<GetServerResponse> getOperationHandlerResult(GetServerRequest GetServerRequest)
      throws Exception {
    return operationHandler.process(serializationService, GetServerRequest,
        TestExecutionContext.getLocatorExecutionContext(internalLocatorMock));
  }

  private void validateGetServerResponse(GetServerResponse getServerResponse) {
    assertThat(getServerResponse.hasServer()).isTrue();
    BasicTypes.Server server = getServerResponse.getServer();
    assertThat(server.getHostname()).isEqualTo(HOSTNAME);
    assertThat(server.getPort()).isEqualTo(PORT);
  }
}
