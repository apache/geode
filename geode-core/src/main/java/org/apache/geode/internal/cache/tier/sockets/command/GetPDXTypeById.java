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

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.MessageFromClient;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;

public class GetPDXTypeById extends BaseCommand {

  private static final GetPDXTypeById singleton = new GetPDXTypeById();

  public static Command getCommand() {
    return singleton;
  }

  private GetPDXTypeById() {}

  @Override
  public void cmdExecute(final MessageFromClient clientMessage,
      final ServerConnection serverConnection, final SecurityService securityService, long start)
      throws IOException, ClassNotFoundException {
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received get pdx type by id request ({} parts) from {}",
          serverConnection.getName(), clientMessage.getNumberOfParts(),
          serverConnection.getSocketString());
    }

    if (!ServerConnection.allowInternalMessagesWithoutCredentials) {
      serverConnection.getAuthzRequest();
    }

    int pdxId = clientMessage.getPart(0).getInt();

    PdxType type;
    try {
      InternalCache cache = serverConnection.getCache();
      TypeRegistry registry = cache.getPdxRegistry();
      type = registry.getType(pdxId);
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    Message responseMsg = serverConnection.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setNumberOfParts(1);
    responseMsg.setTransactionId(clientMessage.getTransactionId());
    responseMsg.addObjPart(type);
    responseMsg.send(serverConnection);
    serverConnection.setAsTrue(RESPONDED);
  }
}
