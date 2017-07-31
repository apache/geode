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

import org.apache.geode.cache.client.internal.locator.GetAllServersResponse;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.Failure;
import org.apache.geode.protocol.protobuf.Result;
import org.apache.geode.protocol.protobuf.ServerAPI;
import org.apache.geode.protocol.protobuf.ServerAPI.GetAvailableServersResponse;
import org.apache.geode.protocol.protobuf.Success;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class GetAvailableServersOperationHandlerJUnitTest extends OperationHandlerJUnitTest {

  private TcpClient mockTCPClient;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    operationHandler = mock(GetAvailableServersOperationHandler.class);
    cacheStub = mock(GemFireCacheImpl.class);
    when(operationHandler.process(any(), any(), any())).thenCallRealMethod();
    InternalDistributedSystem mockDistributedSystem = mock(InternalDistributedSystem.class);
    when(cacheStub.getDistributedSystem()).thenReturn(mockDistributedSystem);
    Properties mockProperties = mock(Properties.class);
    when(mockDistributedSystem.getProperties()).thenReturn(mockProperties);
    String locatorString = "testLocator1Host[12345],testLocator2Host[23456]";
    when(mockProperties.getProperty(ConfigurationProperties.LOCATORS)).thenReturn(locatorString);
    mockTCPClient = mock(TcpClient.class);
    when(((GetAvailableServersOperationHandler) operationHandler).getTcpClient())
        .thenReturn(mockTCPClient);
  }

  @Test
  public void testServerReturnedFromHandler() throws Exception {
    when(mockTCPClient.requestToServer(any(), any(), anyInt(), anyBoolean()))
        .thenReturn(new GetAllServersResponse(new ArrayList<ServerLocation>() {
          {
            add(new ServerLocation("hostname1", 12345));
            add(new ServerLocation("hostname2", 23456));
          }
        }));

    ServerAPI.GetAvailableServersRequest getAvailableServersRequest =
        ProtobufRequestUtilities.createGetAvailableServersRequest();
    Result operationHandlerResult =
        operationHandler.process(serializationServiceStub, getAvailableServersRequest, cacheStub);
    assertTrue(operationHandlerResult instanceof Success);
    ValidateGetAvailableServersResponse(
        (GetAvailableServersResponse) operationHandlerResult.getMessage());
  }

  @Test
  public void testServerReturnedFromSecondLocatorIfFirstDown() throws Exception {
    when(mockTCPClient.requestToServer(any(), any(), anyInt(), anyBoolean()))
        .thenThrow(new IOException("BOOM!!!"))
        .thenReturn(new GetAllServersResponse(new ArrayList<ServerLocation>() {
          {
            add(new ServerLocation("hostname1", 12345));
            add(new ServerLocation("hostname2", 23456));
          }
        }));

    ServerAPI.GetAvailableServersRequest getAvailableServersRequest =
        ProtobufRequestUtilities.createGetAvailableServersRequest();
    Result operationHandlerResult =
        operationHandler.process(serializationServiceStub, getAvailableServersRequest, cacheStub);
    assertTrue(operationHandlerResult instanceof Success);
    ValidateGetAvailableServersResponse(
        (GetAvailableServersResponse) operationHandlerResult.getMessage());
  }

  private void ValidateGetAvailableServersResponse(
      GetAvailableServersResponse getAvailableServersResponse) {
    assertEquals(2, getAvailableServersResponse.getServersCount());
    BasicTypes.Server server = getAvailableServersResponse.getServers(0);
    assertEquals("hostname1", server.getHostname());
    assertEquals(12345, server.getPort());
    server = getAvailableServersResponse.getServers(1);
    assertEquals("hostname2", server.getHostname());
    assertEquals(23456, server.getPort());
  }

  @Test
  public void testProcessFailsIfNoLocatorsAvailable() throws Exception {
    when(mockTCPClient.requestToServer(any(), any(), anyInt(), anyBoolean()))
        .thenThrow(new IOException("BOOM!!!"));

    ServerAPI.GetAvailableServersRequest getAvailableServersRequest =
        ProtobufRequestUtilities.createGetAvailableServersRequest();
    Result operationHandlerResult =
        operationHandler.process(serializationServiceStub, getAvailableServersRequest, cacheStub);
    assertTrue(operationHandlerResult instanceof Failure);
  }
}
