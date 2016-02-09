/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Pivotal Additions:
 * Flag to enable/disable selector pooling for test purposes
 * 
 */
package com.gemstone.gemfire.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.channels.SocketChannel;

public class SocketUtils {

  //used for testing
  public static boolean USE_SELECTOR_POOLING = Boolean.valueOf(System.getProperty("gemfire.useSelectorPooling", "true")).booleanValue();
  /**
   * This is a drop-in replacement for 
   * {@link Socket#connect(SocketAddress, int)}.
   * In the case of normal sockets that don't have associated channels, this 
   * just invokes <code>socket.connect(endpoint, timeout)</code>. If 
   * <code>socket.getChannel()</code> returns a non-null channel,
   * connect is implemented using Hadoop's selectors. This is done mainly
   * to avoid Sun's connect implementation from creating thread-local 
   * selectors, since Hadoop does not have control on when these are closed
   * and could end up taking all the available file descriptors.
   * 
   * @see java.net.Socket#connect(java.net.SocketAddress, int)
   * 
   * @param socket
   * @param address the remote address
   * @param timeout timeout in milliseconds
   */
  public static void connect(Socket socket,
      SocketAddress address,
      int timeout) throws IOException {
    connect(socket, address, null, timeout);
  }
  
  /**
   * Like SocketUtils.connect(Socket, SocketAddress, int) but
   * also takes a local address and port to bind the socket to. 
   * 
   * @param socket
   * @param endpoint the remote address
   * @param localAddr the local address to bind the socket to
   * @param timeout timeout in milliseconds
   */
  public static void connect(Socket socket, 
                             SocketAddress endpoint,
                             SocketAddress localAddr,
                             int timeout) throws IOException {
    if (socket == null || endpoint == null || timeout < 0) {
      throw new IllegalArgumentException("Illegal argument for connect()");
    }
    SocketChannel ch = socket.getChannel();
    
    if (localAddr != null) {
      socket.bind(localAddr);
    }

    try {
      if (ch == null) {
        // let the default implementation handle it.
        socket.connect(endpoint, timeout);
      } else {
        if (USE_SELECTOR_POOLING) {
          SocketIOWithTimeout.connect(ch, endpoint, timeout);
        }
        else {
          socket.connect(endpoint, timeout);
        }

      }
    } catch (SocketTimeoutException ste) {
      throw new IOException(ste.getMessage());
    }

    /*
     Pivotal Change: due to ticket #50734
    // There is a very rare case allowed by the TCP specification, such that
    // if we are trying to connect to an endpoint on the local machine,
    // and we end up choosing an ephemeral port equal to the destination port,
    // we will actually end up getting connected to ourself (ie any data we
    // send just comes right back). This is only possible if the target
    // daemon is down, so we'll treat it like connection refused.
    if (socket.getLocalPort() == socket.getPort() &&
        socket.getLocalAddress().equals(socket.getInetAddress())) {
      socket.close();
      throw new ConnectException(
        "Localhost targeted connection resulted in a loopback. " +
        "No daemon is listening on the target port.");
    }
    */
  }
  
  /**
   * Same as <code>getInputStream(socket, socket.getSoTimeout()).</code>
   * <br><br>
   * 
   * @see #getInputStream(Socket, long)
   */
  public static InputStream getInputStream(Socket socket) 
                                           throws IOException {
    return getInputStream(socket, socket.getSoTimeout());
  }

  /**
   * Return a {@link SocketInputWrapper} for the socket and set the given
   * timeout. If the socket does not have an associated channel, then its socket
   * timeout will be set to the specified value. Otherwise, a
   * {@link SocketInputStream} will be created which reads with the configured
   * timeout.
   * 
   * Any socket created using socket factories returned by {@link #SocketUtils},
   * must use this interface instead of {@link Socket#getInputStream()}.
   * 
   * In general, this should be called only once on each socket: see the note
   * in {@link SocketInputWrapper#setTimeout(long)} for more information.
   *
   * @see Socket#getChannel()
   * 
   * @param socket
   * @param timeout timeout in milliseconds. zero for waiting as
   *                long as necessary.
   * @return SocketInputWrapper for reading from the socket.
   * @throws IOException
   */
  /*Pivotal Addition
   * Return type changed to InputStream instead of SocketInputWrapper
   * Returning the regular inputstream if a channel is not present and does
   * not wrap that around an input wrapper
   */
  public static InputStream getInputStream(Socket socket, long timeout) 
                                           throws IOException {
    if (socket.getChannel() == null || ! USE_SELECTOR_POOLING) {
      return socket.getInputStream();
    }
    else {
      SocketInputWrapper w = new SocketInputWrapper(socket, new SocketInputStream(socket));
      w.setTimeout(timeout);
      return w;
    }
  }
  
  /**
   * Same as getOutputStream(socket, 0). Timeout of zero implies write will
   * wait until data is available.<br><br>
   * 
   * From documentation for {@link #getOutputStream(Socket, long)} : <br>
   * Returns OutputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a 
   * {@link SocketOutputStream} with the given timeout. If the socket does not
   * have a channel, {@link Socket#getOutputStream()} is returned. In the later
   * case, the timeout argument is ignored and the write will wait until 
   * data is available.<br><br>
   * 
   * Any socket created using socket factories returned by {@link SocketUtils},
   * must use this interface instead of {@link Socket#getOutputStream()}.
   * 
   * @see #getOutputStream(Socket, long)
   * 
   * @param socket
   * @return OutputStream for writing to the socket.
   * @throws IOException
   */  
  public static OutputStream getOutputStream(Socket socket) 
                                             throws IOException {
    return getOutputStream(socket, 0);
  }
  
  /**
   * Returns OutputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a 
   * {@link SocketOutputStream} with the given timeout. If the socket does not
   * have a channel, {@link Socket#getOutputStream()} is returned. In the later
   * case, the timeout argument is ignored and the write will wait until 
   * data is available.<br><br>
   * 
   * Any socket created using socket factories returned by {@link SocketUtils},
   * must use this interface instead of {@link Socket#getOutputStream()}.
   * 
   * @see Socket#getChannel()
   * 
   * @param socket
   * @param timeout timeout in milliseconds. This may not always apply. zero
   *        for waiting as long as necessary.
   * @return OutputStream for writing to the socket.
   * @throws IOException   
   */
  public static OutputStream getOutputStream(Socket socket, long timeout) 
                                             throws IOException {
    if (socket.getChannel() == null || !USE_SELECTOR_POOLING) {
      return socket.getOutputStream();      
    }
    else {
      return new SocketOutputStream(socket, timeout); 
    }
  }
}
