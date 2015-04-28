/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.net;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * The SocketUtils class is utility class for performing operations on Sockets and ServerSockets.
 * <p/>
 * @author John Blum
 * @see java.net.ServerSocket
 * @see java.net.Socket
 * @since 7.0
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
