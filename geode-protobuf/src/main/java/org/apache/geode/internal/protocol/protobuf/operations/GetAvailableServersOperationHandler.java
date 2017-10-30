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

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.OperationHandler;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.ServerAPI;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.serialization.SerializationService;

@Experimental
public class GetAvailableServersOperationHandler implements
    OperationHandler<ServerAPI.GetAvailableServersRequest, ServerAPI.GetAvailableServersResponse, ClientProtocol.ErrorResponse> {

  @Override
  public Result<ServerAPI.GetAvailableServersResponse, ClientProtocol.ErrorResponse> process(
      SerializationService serializationService, ServerAPI.GetAvailableServersRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {

    InternalLocator internalLocator = (InternalLocator) messageExecutionContext.getLocator();
    ArrayList serversFromSnapshot =
        internalLocator.getServerLocatorAdvisee().getLoadSnapshot().getServers(null);
    if (serversFromSnapshot == null) {
      serversFromSnapshot = new ArrayList();
    }

    Collection<BasicTypes.Server> servers = (Collection<BasicTypes.Server>) serversFromSnapshot
        .stream().map(serverLocation -> getServerProtobufMessage((ServerLocation) serverLocation))
        .collect(Collectors.toList());
    ServerAPI.GetAvailableServersResponse.Builder builder =
        ServerAPI.GetAvailableServersResponse.newBuilder().addAllServers(servers);
    return Success.of(builder.build());
  }

  private BasicTypes.Server getServerProtobufMessage(ServerLocation serverLocation) {
    BasicTypes.Server.Builder serverBuilder = BasicTypes.Server.newBuilder();
    serverBuilder.setHostname(serverLocation.getHostName()).setPort(serverLocation.getPort());
    return serverBuilder.build();
  }
}
