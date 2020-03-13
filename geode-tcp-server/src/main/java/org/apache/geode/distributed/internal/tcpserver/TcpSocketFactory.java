package org.apache.geode.distributed.internal.tcpserver;

import java.io.IOException;
import java.net.Socket;

public interface TcpSocketFactory {

  TcpSocketFactory DEFAULT = Socket::new;

  Socket createSocket() throws IOException;
}
