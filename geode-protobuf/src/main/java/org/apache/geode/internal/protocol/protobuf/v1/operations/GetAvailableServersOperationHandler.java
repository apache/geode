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

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.operations.OperationHandler;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.LocatorAPI;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.internal.protocol.state.ConnectionTerminatingStateProcessor;

@Experimental
public class GetAvailableServersOperationHandler implements
    ProtobufOperationHandler<LocatorAPI.GetAvailableServersRequest, LocatorAPI.GetAvailableServersResponse> {

  @Override
  public Result<LocatorAPI.GetAvailableServersResponse, ClientProtocol.ErrorResponse> process(
      ProtobufSerializationService serializationService,
      LocatorAPI.GetAvailableServersRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {

    messageExecutionContext.setConnectionStateProcessor(new ConnectionTerminatingStateProcessor());
    InternalLocator internalLocator = (InternalLocator) messageExecutionContext.getLocator();
    ArrayList serversFromSnapshot =
        internalLocator.getServerLocatorAdvisee().getLoadSnapshot().getServers(null);
    if (serversFromSnapshot == null) {
      serversFromSnapshot = new ArrayList();
    }

    Collection<BasicTypes.Server> servers = (Collection<BasicTypes.Server>) serversFromSnapshot
        .stream().map(serverLocation -> getServerProtobufMessage((ServerLocation) serverLocation))
        .collect(Collectors.toList());
    LocatorAPI.GetAvailableServersResponse.Builder builder =
        LocatorAPI.GetAvailableServersResponse.newBuilder().addAllServers(servers);
    return Success.of(builder.build());
  }

  private BasicTypes.Server getServerProtobufMessage(ServerLocation serverLocation) {
    BasicTypes.Server.Builder serverBuilder = BasicTypes.Server.newBuilder();
    serverBuilder.setHostname(serverLocation.getHostName()).setPort(serverLocation.getPort());
    return serverBuilder.build();
  }
}
