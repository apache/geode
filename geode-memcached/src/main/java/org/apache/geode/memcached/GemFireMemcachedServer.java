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
package org.apache.geode.memcached;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.LogWriter;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.memcached.ConnectionHandler;
import org.apache.geode.internal.net.SocketCreator;

/**
 * This is the Server that listens for incoming memcached client connections. This server
 * understands the memcached ASCII protocol documented in
 * <a href="https://github.com/memcached/memcached/blob/master/doc/protocol.txt">memcached source
 * control</a> It then translates these commands to the corresponding GemFire commands, and stores
 * the data in GemFire in a {@link Region} named "gemcached".
 * <p>
 * "gemcached" region is {@link RegionShortcut#PARTITION} by default, though a cache.xml can be
 * provided to override region attributes.
 *
 * This class has a Main method that can be used to start the server.
 *
 *
 */
public class GemFireMemcachedServer {

  /**
   * The protocol used by GemFireMemcachedServer
   */
  public enum Protocol {
    ASCII, BINARY
  }

  @MakeNotStatic
  private static LogWriter logger;

  /**
   * Name of the GemFire region in which data is stored, value id "gemcached"
   */
  public static final String REGION_NAME = "gemcached";

  /**
   * version of gemcached server
   */
  public static final String version = "0.2";

  /**
   * the configured address to listen for client connections
   */
  private final String bindAddress;

  /**
   * the port to listen for client connections
   */
  private final int serverPort;

  private final int DEFAULT_PORT = 11212;

  /**
   * the thread executor pool to handle requests from clients. We create one thread for each client.
   */
  private final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setName("Gemcached-" + counter.incrementAndGet());
      t.setDaemon(true);
      return t;
    }
  });

  /**
   * GemFire cache where data will be stored
   */
  private Cache cache;

  /**
   * thread that listens for client connections
   */
  private Thread acceptor;

  /**
   * The protocol that this server understands, ASCII by default
   */
  private final Protocol protocol;

  /**
   * Create an instance of the server. to start the server {@link #start()} must be called.
   *
   * @param port the port on which the server listens for new memcached client connections.
   */
  public GemFireMemcachedServer(int port) {
    bindAddress = "";
    if (port <= 0) {
      serverPort = DEFAULT_PORT;
    } else {
      serverPort = port;
    }
    protocol = Protocol.ASCII;
  }

  /**
   * Create an instance of the server. to start the server {@link #start()} must be called.
   *
   * @param bindAddress the address on which the server listens for new memcached client
   *        connections.
   * @param port the port on which the server listens for new memcached client connections.
   * @param protocol the protocol that this server should understand
   * @see Protocol
   */
  public GemFireMemcachedServer(String bindAddress, int port, Protocol protocol) {
    this.bindAddress = bindAddress;
    if (port <= 0) {
      serverPort = DEFAULT_PORT;
    } else {
      serverPort = port;
    }
    this.protocol = protocol;
  }

  /**
   * Starts an embedded GemFire caching node, and then listens for new memcached client connections.
   */
  public void start() {
    startGemFire();
    try {
      startMemcachedServer();
    } catch (IOException e) {
      throw new RuntimeException("Could not start Server", e);
    } catch (InterruptedException e) {
      throw new RuntimeException("Could not start Server", e);
    }
  }

  private void startGemFire() {
    cache = GemFireCacheImpl.getInstance();
    if (cache == null) {
      CacheFactory cacheFactory = new CacheFactory();
      cache = cacheFactory.create();
    }
    logger = cache.getLogger();
  }

  private void startMemcachedServer() throws IOException, InterruptedException {
    ServerSocketChannel channel = ServerSocketChannel.open();
    final ServerSocket serverSocket = channel.socket();
    serverSocket.setReceiveBufferSize(getSocketBufferSize());
    serverSocket.setReuseAddress(true);
    serverSocket.bind(new InetSocketAddress(getBindAddress(), serverPort));
    if (logger.fineEnabled()) {
      logger.fine("GemFireMemcachedServer configured socket buffer size:" + getSocketBufferSize());
    }
    final CountDownLatch latch = new CountDownLatch(1);
    acceptor = new Thread(() -> {
      for (;;) {
        Socket s = null;
        try {
          latch.countDown();
          s = serverSocket.accept();
          s.setKeepAlive(SocketCreator.ENABLE_TCP_KEEP_ALIVE);
          handleNewClient(s);
        } catch (ClosedByInterruptException e) {
          try {
            serverSocket.close();
          } catch (IOException e1) {
            e1.printStackTrace();
          }
          break;
        } catch (IOException e) {
          e.printStackTrace();
          break;
        }
      }
    }, "AcceptorThread");
    acceptor.setDaemon(true);
    acceptor.start();
    latch.await();
    logger.config("GemFireMemcachedServer server started on host:" + LocalHostUtil.getLocalHost()
        + " port: " + serverPort);
  }

  private InetAddress getBindAddress() throws UnknownHostException {
    return bindAddress == null || bindAddress.isEmpty() ? LocalHostUtil.getLocalHost()
        : InetAddress.getByName(bindAddress);
  }

  private int getSocketBufferSize() {
    InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();
    return system.getConfig().getSocketBufferSize();
  }

  private void handleNewClient(Socket s) {
    ConnectionHandler connHandler = new ConnectionHandler(s, cache, protocol);
    executor.execute(connHandler);
  }

  /**
   * shuts down this server and closes the embedded GemFire caching node
   */
  public void shutdown() {
    if (acceptor != null) {
      acceptor.interrupt();
    }
    executor.shutdownNow();
    cache.close();
  }

  public static void main(String[] args) {
    int port = getPort(args);
    GemFireMemcachedServer server = new GemFireMemcachedServer(port);
    server.start();
    while (true) {
      try {
        System.in.read();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private static int getPort(String[] args) {
    int port = 0;
    if (args != null && args.length > 0) {
      for (final String arg : args) {
        if (arg.startsWith("-port")) {
          String p = arg.substring(arg.indexOf('='));
          p = p.trim();
          port = Integer.parseInt(p);
        }
      }
    }
    return port;
  }
}
