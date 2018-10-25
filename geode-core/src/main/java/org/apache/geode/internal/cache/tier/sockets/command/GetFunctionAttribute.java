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
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

public class GetFunctionAttribute extends BaseCommand {

  private static final GetFunctionAttribute singleton = new GetFunctionAttribute();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException {
    serverConnection.setAsTrue(REQUIRES_RESPONSE);

    String functionId = clientMessage.getPart(0).getString();
    if (functionId == null) {
      String message =
          String.format("The input %s for GetFunctionAttributes request is null",
              "functionId");
      logger.warn("{}: {}", serverConnection.getName(), message);
      sendError(clientMessage, message, serverConnection);
      return;
    }

    Function function = FunctionService.getFunction(functionId);
    if (function == null) {
      String message = null;
      message =
          String.format("The function is not registered for function id %s",
              functionId);
      logger.warn("{}: {}", serverConnection.getName(), message);
      sendError(clientMessage, message, serverConnection);
      return;
    }

    byte[] functionAttributes = new byte[3];
    functionAttributes[0] = (byte) (function.hasResult() ? 1 : 0);
    functionAttributes[1] = (byte) (function.isHA() ? 1 : 0);
    functionAttributes[2] = (byte) (function.optimizeForWrite() ? 1 : 0);
    writeResponseWithFunctionAttribute(functionAttributes, clientMessage, serverConnection);
  }

  private void sendError(Message msg, String message, ServerConnection servConn)
      throws IOException {
    synchronized (msg) {
      writeErrorResponse(msg, MessageType.REQUESTDATAERROR, message, servConn);
      servConn.setAsTrue(RESPONDED);
    }
  }

}
