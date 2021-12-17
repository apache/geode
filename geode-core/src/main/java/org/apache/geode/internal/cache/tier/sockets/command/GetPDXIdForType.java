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

import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;

public class GetPDXIdForType extends BaseCommand {

  @Immutable
  private static final GetPDXIdForType singleton = new GetPDXIdForType();

  public static Command getCommand() {
    return singleton;
  }

  private GetPDXIdForType() {}

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, long start)
      throws IOException, ClassNotFoundException {
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received get pdx id for type request ({} parts) from {}",
          serverConnection.getName(), clientMessage.getNumberOfParts(),
          serverConnection.getSocketString());
    }

    if (!ServerConnection.allowInternalMessagesWithoutCredentials) {
      serverConnection.getAuthzRequest();
    }

    int noOfParts = clientMessage.getNumberOfParts();

    PdxType type = (PdxType) clientMessage.getPart(0).getObject();

    int pdxId;
    try {
      InternalCache cache = serverConnection.getCache();
      TypeRegistry registry = cache.getPdxRegistry();
      PdxType pdxType = registry.defineType(type);
      pdxId = pdxType.getTypeId();
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    Message responseMsg = serverConnection.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setNumberOfParts(1);
    responseMsg.setTransactionId(clientMessage.getTransactionId());
    responseMsg.addIntPart(pdxId);
    responseMsg.send(serverConnection);
    serverConnection.setAsTrue(RESPONDED);
  }
}
