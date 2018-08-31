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

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.TypeRegistry;

public class AddPdxEnum extends BaseCommand {
  private static final Logger logger = LogService.getLogger();

  private static final AddPdxEnum singleton = new AddPdxEnum();

  public static Command getCommand() {
    return singleton;
  }

  private AddPdxEnum() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start)
      throws IOException, ClassNotFoundException {
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received get pdx id for enum request ({} parts) from {}",
          serverConnection.getName(), clientMessage.getNumberOfParts(),
          serverConnection.getSocketString());
    }

    EnumInfo enumInfo = (EnumInfo) clientMessage.getPart(0).getObject();
    int enumId = clientMessage.getPart(1).getInt();

    try {
      InternalCache cache = serverConnection.getCache();
      TypeRegistry registry = cache.getPdxRegistry();
      registry.addRemoteEnum(enumId, enumInfo);
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    writeReply(clientMessage, serverConnection);
    serverConnection.setAsTrue(RESPONDED);
  }
}
