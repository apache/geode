/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.internal.net;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * The SocketUtils class is utility class for performing operations on Sockets and ServerSockets.
 * <p/>
 * @see java.net.ServerSocket
 * @see java.net.Socket
 * @since GemFire 7.0
 */
public abstract class SocketUtils {

  /**
   * Closes the specified Socket silently ignoring any IOException, guarding against null Object references.
   * <p/>
   * @param socket the Socket to close.
   * @return boolean value indicating whether the Socket was successfully closed.  If the Socket Object reference
   * is null, then this method will return true.
   * @see java.net.Socket#close()
   */
  public static boolean close(final Socket socket) {
    if (socket != null) {
      try {
        socket.close();
      }
      catch (IOException ignore) {
        return false;
      }
    }

    return true;
  }

  /**
   * Closes the specified ServerSocket silently ignoring any IOException, guarding against null Object references.
   * <p/>
   * @param serverSocket the ServerSocket to close.
   * @return boolean value indicating whether the ServerSocket was successfully closed.  If the ServerSocket Object
   * reference is null, then this method will return true.
   * @see java.net.ServerSocket#close()
   */
  public static boolean close(final ServerSocket serverSocket) {
    if (serverSocket != null) {
      try {
        serverSocket.close();
      }
      catch (IOException ignore) {
        return false;
      }
    }

    return true;
  }

}
