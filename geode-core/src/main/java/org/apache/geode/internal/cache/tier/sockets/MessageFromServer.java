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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.geode.internal.Version;

/**
 * This class extends @see Message which encapsulates the wire protocol. It provides accessors to
 * encode and decode a message
 * and serialize it out to the wire from server to client.
 *
 * @see Message
 */
public class MessageFromServer extends Message {
  /**
   * Creates a new message with the given number of parts
   *
   * @param numberOfParts
   * @param destVersion
   */
  public MessageFromServer(int numberOfParts, Version destVersion) {
    super(numberOfParts, destVersion);
  }

  public byte[] getSecureBytes() throws IOException, ClassNotFoundException {
    return (byte[]) this.securePart.getObject();
  }

  public void setVersion(Version clientVersion) {
    this.version = clientVersion;
  }

  // Set up a message on the server side.
  void setComms(ServerConnection sc, Socket socket, ByteBuffer bb, MessageStats msgStats)
      throws IOException {
    serverConnection = sc;
    socketChannel = socket.getChannel();
    InputStream in = null;
    OutputStream os = null;
    if (socketChannel == null) {
      in = socket.getInputStream();
      os = socket.getOutputStream();
    }
    setComms(socket, in, os, bb, msgStats);
  }

}
