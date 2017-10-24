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

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.LocatorLoadSnapshot;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.ServerLocator;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.ProtobufTestExecutionContext;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.protobuf.ServerAPI;
import org.apache.geode.internal.protocol.protobuf.ServerAPI.GetAvailableServersResponse;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.test.junit.categories.UnitTest;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class GetAvailableServersOperationHandlerJUnitTest extends OperationHandlerJUnitTest {

  private final String HOSTNAME_1 = "hostname1";
  private final int PORT_1 = 12345;

  private final String HOSTNAME_2 = "hostname2";
  private final int PORT_2 = 23456;

  private InternalLocator internalLocatorMock;
  private LocatorLoadSnapshot locatorLoadSnapshot;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    operationHandler = new GetAvailableServersOperationHandler();
    internalLocatorMock = mock(InternalLocator.class);
    ServerLocator serverLocatorAdviseeMock = mock(ServerLocator.class);
    locatorLoadSnapshot = mock(LocatorLoadSnapshot.class);

    when(internalLocatorMock.getServerLocatorAdvisee()).thenReturn(serverLocatorAdviseeMock);
    when(serverLocatorAdviseeMock.getLoadSnapshot()).thenReturn(locatorLoadSnapshot);
  }

  @Test
  public void testServerReturnedFromHandler() throws Exception {
    ArrayList<Object> serverList = new ArrayList<>();
    serverList.add(new ServerLocation(HOSTNAME_1, PORT_1));
    serverList.add(new ServerLocation(HOSTNAME_2, PORT_2));
    when(locatorLoadSnapshot.getServers(null)).thenReturn(serverList);

    ServerAPI.GetAvailableServersRequest getAvailableServersRequest =
        ProtobufRequestUtilities.createGetAvailableServersRequest();
    Result operationHandlerResult = getOperationHandlerResult(getAvailableServersRequest);
    assertTrue(operationHandlerResult instanceof Success);
    ValidateGetAvailableServersResponse(
        (GetAvailableServersResponse) operationHandlerResult.getMessage());
  }

  @Test
  public void testWhenServersFromSnapshotAreNullReturnsEmtpy()
      throws InvalidExecutionContextException {
    when(locatorLoadSnapshot.getServers(any())).thenReturn(null);

    ServerAPI.GetAvailableServersRequest getAvailableServersRequest =
        ProtobufRequestUtilities.createGetAvailableServersRequest();
    Result operationHandlerResult = getOperationHandlerResult(getAvailableServersRequest);
    assertTrue(operationHandlerResult instanceof Success);
    GetAvailableServersResponse availableServersResponse =
        (GetAvailableServersResponse) operationHandlerResult.getMessage();
    assertEquals(0, availableServersResponse.getServersCount());
  }

  private Result getOperationHandlerResult(
      ServerAPI.GetAvailableServersRequest getAvailableServersRequest)
      throws InvalidExecutionContextException {
    return operationHandler.process(serializationServiceStub, getAvailableServersRequest,
        ProtobufTestExecutionContext.getLocatorExecutionContext(internalLocatorMock));
  }

  private void ValidateGetAvailableServersResponse(
      GetAvailableServersResponse getAvailableServersResponse) {
    assertEquals(2, getAvailableServersResponse.getServersCount());
    BasicTypes.Server server = getAvailableServersResponse.getServers(0);
    assertEquals(HOSTNAME_1, server.getHostname());
    assertEquals(PORT_1, server.getPort());
    server = getAvailableServersResponse.getServers(1);
    assertEquals(HOSTNAME_2, server.getHostname());
    assertEquals(PORT_2, server.getPort());
  }
}
