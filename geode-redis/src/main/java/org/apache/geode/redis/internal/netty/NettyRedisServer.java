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
 *
 */

package org.apache.geode.redis.internal.netty;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import javax.net.ssl.KeyManagerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.timeout.WriteTimeoutHandler;
import io.netty.util.concurrent.Future;
import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.ManagementException;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.pubsub.PubSub;

public class NettyRedisServer {

  /**
   * System property name that can be used to set the number of threads to be used by the
   * GeodeRedisServer
   */
  private static final String NUM_THREADS_SYS_PROP_NAME = "gemfireredis.numthreads";

  private static final int RANDOM_PORT_INDICATOR = 0;

  private static final Logger logger = LogService.getLogger();
  /**
   * Connection timeout in milliseconds
   */
  private static final int connectTimeoutMillis = 1000;

  /**
   * The number of threads that will work on handling requests
   */
  private final int numWorkerThreads;

  /**
   * The number of threads that will work socket selectors
   */
  private final int numSelectorThreads;

  /**
   * The address to bind to
   */
  private final String bindAddress;
  private final Supplier<DistributionConfig> configSupplier;
  private final RegionProvider regionProvider;
  private final PubSub pubsub;
  private final Supplier<Boolean> allowUnsupportedSupplier;
  private final Runnable shutdownInvoker;
  private int serverPort;

  /**
   * Temporary constant whether to use old single thread per connection model for worker group
   */
  private boolean singleThreadPerConnection;

  /**
   * Channel to be closed when shutting down
   */
  private Channel serverChannel;

  private EventLoopGroup bossGroup;

  private EventLoopGroup workerGroup;
  private EventLoopGroup subscriberGroup;


  public NettyRedisServer(Supplier<DistributionConfig> configSupplier,
      RegionProvider regionProvider, PubSub pubsub, Supplier<Boolean> allowUnsupportedSupplier,
      Runnable shutdownInvoker, int port, String bindAddress) {
    this.configSupplier = configSupplier;
    this.regionProvider = regionProvider;
    this.pubsub = pubsub;
    this.allowUnsupportedSupplier = allowUnsupportedSupplier;
    this.shutdownInvoker = shutdownInvoker;
    if (port < RANDOM_PORT_INDICATOR) {
      throw new IllegalArgumentException("Redis port cannot be less than 0");
    }
    serverPort = port;
    this.bindAddress = bindAddress;
    numWorkerThreads = setNumWorkerThreads();
    singleThreadPerConnection = numWorkerThreads == 0;
    numSelectorThreads = 1;
  }

  public void start() {
    try {
      startListening();
    } catch (IOException | InterruptedException e) {
      throw new ManagementException("Could not start Server", e);
    }
  }

  public void stop() {
    ChannelFuture closeFuture = null;
    if (serverChannel != null) {
      closeFuture = serverChannel.closeFuture();
    }
    workerGroup.shutdownGracefully();
    Future<?> bossFuture = bossGroup.shutdownGracefully();
    if (serverChannel != null) {
      serverChannel.close();
    }
    bossFuture.syncUninterruptibly();
    if (closeFuture != null) {
      closeFuture.syncUninterruptibly();
    }
  }

  public int getPort() {
    return serverPort;
  }

  /**
   * Helper method to set the number of worker threads
   *
   * @return If the System property {@value #NUM_THREADS_SYS_PROP_NAME} is set then that number is
   *         used, otherwise {@link Runtime#availableProcessors()}.
   */
  private int setNumWorkerThreads() {
    String prop = System.getProperty(NUM_THREADS_SYS_PROP_NAME);
    int defaultThreads = Runtime.getRuntime().availableProcessors();
    if (prop == null || prop.isEmpty()) {
      return defaultThreads;
    }
    int threads;
    try {
      threads = Integer.parseInt(prop);
    } catch (NumberFormatException e) {
      return defaultThreads;
    }
    return threads;
  }

  private ChannelInitializer<SocketChannel> createChannelInitializer(byte[] redisPasswordBytes) {
    return new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel socketChannel) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "GeodeRedisServer-Connection established with " + socketChannel.remoteAddress());
        }
        ChannelPipeline pipeline = socketChannel.pipeline();
        addSSLIfEnabled(socketChannel, pipeline);
        pipeline.addLast(ByteToCommandDecoder.class.getSimpleName(), new ByteToCommandDecoder());
        pipeline.addLast(new WriteTimeoutHandler(10));
        pipeline.addLast(ExecutionHandlerContext.class.getSimpleName(),
            new ExecutionHandlerContext(socketChannel, regionProvider, pubsub, subscriberGroup,
                allowUnsupportedSupplier, shutdownInvoker, redisPasswordBytes));
      }
    };
  }

  private void addSSLIfEnabled(SocketChannel ch, ChannelPipeline p) {

    SSLConfig sslConfigForComponent =
        SSLConfigurationFactory.getSSLConfigForComponent(configSupplier.get(),
            SecurableCommunicationChannel.SERVER);

    if (!sslConfigForComponent.isEnabled()) {
      return;
    }

    SslContext sslContext;
    try {
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(new FileInputStream(sslConfigForComponent.getKeystore()),
          sslConfigForComponent.getKeystorePassword().toCharArray()/**/);

      // Set up key manager factory to use our key store
      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, sslConfigForComponent.getKeystorePassword().toCharArray());

      SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(kmf);
      sslContext = sslContextBuilder.build();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    p.addLast(sslContext.newHandler(ch.alloc()));
  }

  /**
   * Helper method to start the server listening for connections. The server is bound to the port
   * specified by {@link GeodeRedisServer#serverPort}
   */
  private void startListening() throws IOException, InterruptedException {
    Class<? extends ServerChannel> socketClass = initializeEventLoopGroups();

    String redisPassword = configSupplier.get().getRedisPassword();
    final byte[] redisPasswordBytes = Coder.stringToBytes(redisPassword);
    ServerBootstrap serverBootstrap = new ServerBootstrap();

    serverBootstrap.group(bossGroup, workerGroup).channel(socketClass)
        .childHandler(createChannelInitializer(redisPasswordBytes))
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_RCVBUF, getBufferSize())
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    serverChannel = createBoundChannel(serverBootstrap);
  }

  @SuppressWarnings("deprecation")
  private Class<? extends ServerChannel> initializeEventLoopGroups() {
    ThreadFactory selectorThreadFactory =
        new LoggingThreadFactory("GeodeRedisServer-SelectorThread-", false);

    ThreadFactory workerThreadFactory =
        new LoggingThreadFactory("GeodeRedisServer-WorkerThread-", true);

    ThreadFactory subscriberThreadFactory =
        new LoggingThreadFactory("GeodeRedisServer-SubscriberThread-", true);

    Class<? extends ServerChannel> socketClass;
    if (singleThreadPerConnection) {
      bossGroup =
          new io.netty.channel.oio.OioEventLoopGroup(Integer.MAX_VALUE, selectorThreadFactory);
      workerGroup =
          new io.netty.channel.oio.OioEventLoopGroup(Integer.MAX_VALUE, workerThreadFactory);
      socketClass = io.netty.channel.socket.oio.OioServerSocketChannel.class;
    } else {
      bossGroup = new NioEventLoopGroup(numSelectorThreads, selectorThreadFactory);
      workerGroup = new NioEventLoopGroup(numWorkerThreads, workerThreadFactory);
      subscriberGroup = new NioEventLoopGroup(numWorkerThreads, subscriberThreadFactory);
      socketClass = NioServerSocketChannel.class;
    }
    return socketClass;
  }

  private Channel createBoundChannel(ServerBootstrap serverBootstrap)
      throws InterruptedException, UnknownHostException {
    InetAddress bindAddress = getBindAddress();
    int port = serverPort == RANDOM_PORT_INDICATOR ? 0 : serverPort;
    ChannelFuture channelFuture =
        serverBootstrap.bind(new InetSocketAddress(bindAddress, port)).sync();

    serverPort = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();

    logStartupMessage(bindAddress);

    return channelFuture.channel();
  }

  /**
   * Helper method to get the host name to bind to
   *
   * @return The InetAddress to bind to
   */
  private InetAddress getBindAddress() throws UnknownHostException {
    if (bindAddress == null || bindAddress.isEmpty()
        || bindAddress.equals(LocalHostUtil.getAnyLocalAddress().getHostAddress())) {
      return LocalHostUtil.getAnyLocalAddress();
    } else {
      return InetAddress.getByName(bindAddress);
    }
  }


  private void logStartupMessage(InetAddress bindAddress) {
    String logMessage = "GeodeRedisServer started {" + bindAddress + ":" + serverPort
        + "}, Selector threads: " + numSelectorThreads;
    if (singleThreadPerConnection) {
      logMessage += ", One worker thread per connection";
    } else {
      logMessage += ", Worker threads: " + numWorkerThreads;
    }
    logger.info(logMessage);
  }

  /**
   * Helper method to get GemFire set socket buffer size, possibly a default of 32k
   *
   * @return Buffer size to use for server
   */
  private int getBufferSize() {
    return configSupplier.get().getSocketBufferSize();
  }
}
