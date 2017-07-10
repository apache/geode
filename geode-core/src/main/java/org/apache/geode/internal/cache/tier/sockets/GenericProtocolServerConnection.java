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

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.security.SecurityService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Holds the socket and protocol handler for the new client protocol. TODO: Currently unimplemented
 * due the the protocol not being there.
 */
public class GenericProtocolServerConnection extends ServerConnection {
  // The new protocol lives in a separate module and gets loaded when this class is instantiated.
  // TODO implement this.
  private final ClientProtocolMessageHandler messageHandler;

  /**
   * Creates a new <code>GenericProtocolServerConnection</code> that processes messages received
   * from an edge client over a given <code>Socket</code>.
   *
   * @param s
   * @param c
   * @param helper
   * @param stats
   * @param hsTimeout
   * @param socketBufferSize
   * @param communicationModeStr
   * @param communicationMode
   * @param acceptor
   */
  public GenericProtocolServerConnection(Socket s, InternalCache c, CachedRegionHelper helper,
      CacheServerStats stats, int hsTimeout, int socketBufferSize, String communicationModeStr,
      byte communicationMode, Acceptor acceptor, ClientProtocolMessageHandler newClientProtocol,
      SecurityService securityService) {
    super(s, c, helper, stats, hsTimeout, socketBufferSize, communicationModeStr, communicationMode,
        acceptor, securityService);
    this.messageHandler = newClientProtocol;
  }

  @Override
  protected void doOneMessage() {
    try {
      Socket socket = this.getSocket();
      InputStream inputStream = socket.getInputStream();
      OutputStream outputStream = socket.getOutputStream();

      // TODO serialization types?
      messageHandler.receiveMessage(inputStream, outputStream, this.getCache());
    } catch (IOException e) {
      logger.warn(e);
      // TODO?
    }
    return;
  }

  @Override
  protected boolean doHandShake(byte epType, int qSize) {
    // no handshake for new client protocol.
    return true;
  }

  @Override
  public boolean isClientServerConnection() {
    return true;
  }
}
