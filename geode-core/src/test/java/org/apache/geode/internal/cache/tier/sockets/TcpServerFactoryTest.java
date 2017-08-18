package org.apache.geode.internal.cache.tier.sockets;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TcpServerFactoryTest {
  @Test
  public void createsATcpServer() {
    TcpServerFactory factory = new TcpServerFactory();
    TcpServer server = factory.makeTcpServer(80, null, null, null, null, null, null, null, null);
    assertTrue(server != null);
  }
}
