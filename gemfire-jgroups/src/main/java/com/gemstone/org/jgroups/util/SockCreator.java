package com.gemstone.org.jgroups.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/** This interface does not define how to create socks
 * but defines a factory for creating sockets.
 * One of its implementations had already used
 * the name "SocketCreator" at the time the interface
 * was created.
 */
public interface SockCreator {

  boolean useSSL();

  Socket connect(InetAddress ipAddress, int port, int connectTimeout,
      ConnectionWatcher watcher, boolean clientToServer, int timeout, boolean useSSL) throws IOException;

  Socket connect(InetAddress ipAddress, int port, int timeout,
      ConnectionWatcher watcher, boolean clientToServer
      ) throws IOException;

}
