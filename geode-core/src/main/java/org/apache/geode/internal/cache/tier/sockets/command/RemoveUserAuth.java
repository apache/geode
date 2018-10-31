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

import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.GemFireSecurityException;

public class RemoveUserAuth extends BaseCommand {

  private static final RemoveUserAuth singleton = new RemoveUserAuth();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start)
      throws IOException, ClassNotFoundException, InterruptedException {
    boolean isSecureMode = clientMessage.isSecureMode();

    if (!isSecureMode) {
      // need to throw exception
      return;
    }

    try {
      serverConnection.setAsTrue(REQUIRES_RESPONSE);
      Part keepalivePart = clientMessage.getPart(0);
      byte[] keepaliveByte = keepalivePart.getSerializedForm();
      boolean keepalive = (keepaliveByte == null || keepaliveByte[0] == 0) ? false : true;
      serverConnection.getSecurityLogWriter().fine("remove user auth keep alive " + keepalive);
      serverConnection.removeUserAuth(clientMessage, keepalive);
      writeReply(clientMessage, serverConnection);
    } catch (GemFireSecurityException gfse) {
      if (serverConnection.getSecurityLogWriter().warningEnabled()) {
        serverConnection.getSecurityLogWriter().warning(String.format("%s",
            serverConnection.getName() + ": Security exception: " + gfse.getMessage()));
      }
      writeException(clientMessage, gfse, false, serverConnection);
    } catch (Exception ex) {
      // TODO Auto-generated catch block
      if (serverConnection.getLogWriter().warningEnabled()) {
        serverConnection.getLogWriter().warning(
            String.format("An exception was thrown for client [%s]. %s",
                serverConnection.getProxyID(), ""),
            ex);
      }
      writeException(clientMessage, ex, false, serverConnection);
    } finally {
      serverConnection.setAsTrue(RESPONDED);
    }
  }

}
