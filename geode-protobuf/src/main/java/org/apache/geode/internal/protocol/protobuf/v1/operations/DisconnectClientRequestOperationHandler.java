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

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.state.TerminateConnection;
import org.apache.geode.logging.internal.LogService;

public class DisconnectClientRequestOperationHandler implements
    ProtobufOperationHandler<ConnectionAPI.DisconnectClientRequest, ConnectionAPI.DisconnectClientResponse> {
  private static final Logger logger = LogService.getLogger();

  @Override
  public Result<ConnectionAPI.DisconnectClientResponse> process(
      ProtobufSerializationService serializationService,
      ConnectionAPI.DisconnectClientRequest request,
      MessageExecutionContext messageExecutionContext) {
    logger.info("Client disconnecting due to {}", request.getReason());
    messageExecutionContext.setState(new TerminateConnection());

    return Success.of(ConnectionAPI.DisconnectClientResponse.newBuilder().build());
  }
}
