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

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.LocatorAPI;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.state.TerminateConnection;

@Experimental
public class GetServerOperationHandler
    implements ProtobufOperationHandler<LocatorAPI.GetServerRequest, LocatorAPI.GetServerResponse> {

  @Override
  public Result<LocatorAPI.GetServerResponse> process(
      ProtobufSerializationService serializationService, LocatorAPI.GetServerRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {

    // A client may send a set of servers to exclude and/or a server-group.
    Set<ServerLocation> excludedServers = new HashSet<>();
    List<BasicTypes.Server> excludedServersList = request.getExcludedServersList();
    for (BasicTypes.Server server : excludedServersList) {
      excludedServers.add(new ServerLocation(server.getHostname(), server.getPort()));
    }

    // note: an empty string is okay - the ServerLocator code checks for this
    String serverGroup = request.getServerGroup();

    messageExecutionContext.setState(new TerminateConnection());

    ServerLocation serverLocation =
        messageExecutionContext.getSecureLocator().findServer(excludedServers, serverGroup);

    if (serverLocation == null) {
      StringBuilder builder = new StringBuilder("Unable to find a server");
      if (!Objects.isNull(serverGroup) && !serverGroup.isEmpty()) {
        builder.append(" in server group ");
        builder.append(serverGroup);
      }
      return Failure.of(NO_AVAILABLE_SERVER, builder.toString());
    } else {
      LocatorAPI.GetServerResponse.Builder builder = LocatorAPI.GetServerResponse.newBuilder();
      BasicTypes.Server.Builder serverBuilder = BasicTypes.Server.newBuilder();
      serverBuilder.setHostname(serverLocation.getHostName()).setPort(serverLocation.getPort());
      BasicTypes.Server server = serverBuilder.build();
      builder.setServer(server);
      return Success.of(builder.build());
    }
  }
}
