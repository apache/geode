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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.IOException;
import java.net.Socket;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.security.SecurityService;

/**
 * Handles everything but the new client protocol.
 *
 * Legacy is therefore a bit of a misnomer; do you have a better name?
 */
public class OriginalServerConnection extends ServerConnection {
  /**
   * Set to false once handshake has been done
   */
  private boolean doHandshake = true;

  /**
   * Creates a new <code>ServerConnection</code> that processes messages received from an edge
   * client over a given <code>Socket</code>.
   *
   */
  public OriginalServerConnection(Socket socket, InternalCache internalCache,
      CachedRegionHelper helper, CacheServerStats stats, int hsTimeout, int socketBufferSize,
      String communicationModeStr, byte communicationMode, Acceptor acceptor,
      SecurityService securityService) {
    super(socket, internalCache, helper, stats, hsTimeout, socketBufferSize, communicationModeStr,
        communicationMode, acceptor, securityService);

    initStreams(socket, socketBufferSize, stats);
  }

  @Override
  protected boolean doHandShake(byte endpointType, int queueSize) {
    try {
      handshake.handshakeWithClient(theSocket.getOutputStream(), theSocket.getInputStream(),
          endpointType, queueSize, this.communicationMode, this.principal);
    } catch (IOException ioe) {
      if (!crHelper.isShutdown() && !isTerminated()) {
        logger.warn("{}: Handshake accept failed on socket {}: {}",
            this.name, this.theSocket, ioe);
      }
      cleanup();
      return false;
    }
    return true;
  }

  protected void doOneMessage() {
    if (this.doHandshake) {
      doHandshake();
      this.doHandshake = false;
    } else {
      this.resetTransientData();
      doNormalMessage();
    }
  }
}
