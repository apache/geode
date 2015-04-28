package com.gemstone.org.jgroups.stack;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketImpl;

import com.gemstone.org.jgroups.util.ConnectionWatcher;
import com.gemstone.org.jgroups.util.SockCreator;

public class SockCreatorImpl implements SockCreator {

  @Override
  public boolean useSSL() {
    return false;
  }

  @Override
  public Socket connect(InetAddress ipAddress, int port, int connectTimeout,
      ConnectionWatcher watcher, boolean clientToServer, int bufferSize, boolean useSSL_ignored)
      throws IOException {
    Socket socket = new Socket();
    SocketAddress addr = new InetSocketAddress(ipAddress, port);
    if (connectTimeout > 0) {
      socket.connect(addr, connectTimeout);
    } else {
      socket.connect(addr);
    }
    return socket;
  }

  @Override
  public boolean isHostReachable(InetAddress ipAddress) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Socket connect(InetAddress ipAddress, int port, int i,
      ConnectionWatcher watcher, boolean clientToServer) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
