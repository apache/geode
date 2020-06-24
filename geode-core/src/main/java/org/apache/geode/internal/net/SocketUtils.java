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

package org.apache.geode.internal.net;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

import javax.net.ssl.SSLSocket;

import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * The SocketUtils class is utility class for performing operations on Sockets and ServerSockets.
 * <p/>
 *
 * @see java.net.ServerSocket
 * @see java.net.Socket
 * @since GemFire 7.0
 */
public abstract class SocketUtils {
  private static final Logger logger = LogService.getLogger();

  /**
   * Closes the specified Socket silently ignoring any IOException, guarding against null Object
   * references.
   * <p/>
   *
   * @param socket the Socket to close.
   * @return boolean value indicating whether the Socket was successfully closed. If the Socket
   *         Object reference is null, then this method will return true.
   * @see java.net.Socket#close()
   */
  public static boolean close(final Socket socket) {
    if (socket != null) {
      try {
        socket.close();
      } catch (IOException ignore) {
        return false;
      }
    }

    return true;
  }

  /**
   * Closes the specified ServerSocket silently ignoring any IOException, guarding against null
   * Object references.
   * <p/>
   *
   * @param serverSocket the ServerSocket to close.
   * @return boolean value indicating whether the ServerSocket was successfully closed. If the
   *         ServerSocket Object reference is null, then this method will return true.
   * @see java.net.ServerSocket#close()
   */
  public static boolean close(final ServerSocket serverSocket) {
    if (serverSocket != null) {
      try {
        serverSocket.close();
      } catch (IOException ignore) {
        return false;
      }
    }

    return true;
  }

  /**
   * Read data from the given socket into the given ByteBuffer. If NIO is supported
   * we use Channel.read(ByteBuffer). If not we use byte arrays to read available
   * bytes or buffer.remaining() bytes, whichever is smaller. If buffer.limit is zero
   * and buffer.remaining is also zero the limit is changed to be buffer.capacity
   * before reading.
   *
   * @return the number of bytes read, which may be -1 for EOF
   */
  public static int readFromSocket(Socket socket, ByteBuffer inputBuffer,
      InputStream socketInputStream) throws IOException {
    int amountRead;
    if (socket instanceof SSLSocket) {
      amountRead = readFromStream(socketInputStream, inputBuffer);
//      amountRead = Channels.newChannel(socketInputStream).read(inputBuffer);
    } else {
      amountRead = socket.getChannel().read(inputBuffer);
    }
    return amountRead;
  }

  private static int readFromStream(InputStream stream, ByteBuffer inputBuffer) throws IOException {
    int amountRead;
//    if (inputBuffer.position() == 0 && inputBuffer.limit() == 0) {
      inputBuffer.limit(inputBuffer.capacity());
//    }
    // if bytes are available we read that number of bytes. Otherwise we do a blocking read
    // of buffer.remaining() bytes
    int amountToRead =
        stream.available() > 0 ? Math.min(stream.available(), inputBuffer.remaining())
            : inputBuffer.remaining();
    if (inputBuffer.hasArray()) {
      amountRead = stream.read(inputBuffer.array(),
              inputBuffer.arrayOffset() + inputBuffer.position(), amountToRead);
      if (amountRead > 0) {
        inputBuffer.position(inputBuffer.position() + amountRead);
      }
    } else {
      byte[] buffer = new byte[amountToRead];
      amountRead = stream.read(buffer);
      if (amountRead > 0) {
        inputBuffer.put(buffer, 0, amountRead);
      }
    }
    return amountRead;
  }


}
