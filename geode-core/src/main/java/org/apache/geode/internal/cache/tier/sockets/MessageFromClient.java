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
import java.nio.ByteBuffer;

import org.apache.geode.internal.Version;

/**
 * This class extends @see Message which encapsulates the wire protocol. It provides accessors to
 * encode and decode a message
 * and serialize it out to the wire from client to server.
 *
 * @see Message
 */
public class MessageFromClient extends Message {
  /**
   * Creates a new message with the given number of parts
   *
   * @param numberOfParts
   * @param destVersion
   */
  public MessageFromClient(int numberOfParts, Version destVersion) {
    super(numberOfParts, destVersion);

  }

  public void setMessageHasSecurePartFlag() {
    this.flags |= MESSAGE_HAS_SECURE_PART;
  }

  public void clearMessageHasSecurePartFlag() {
    this.flags &= MESSAGE_HAS_SECURE_PART;
  }

  public void setIsRetry() {
    this.isRetry = true;
  }

  /**
   * This returns true if the message has been marked as having been previously transmitted to a
   * different server.
   */
  public boolean isRetry() {
    return this.isRetry;
  }

  // Set up a message on the client side.
  void setComms(Socket socket, ByteBuffer bb, MessageStats msgStats) throws IOException {
    socketChannel = socket.getChannel();
    if (this.socketChannel == null) {
      setComms(socket, socket.getInputStream(), socket.getOutputStream(), bb, msgStats);
    } else {
      setComms(socket, null, null, bb, msgStats);
    }
  }

}
