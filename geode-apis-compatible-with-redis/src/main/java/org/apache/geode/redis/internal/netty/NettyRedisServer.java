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
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.ExecutorService;
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
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.timeout.WriteTimeoutHandler;
import io.netty.util.concurrent.Future;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.ManagementException;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.pubsub.PubSub;
import org.apache.geode.redis.internal.statistics.RedisStats;

public class NettyRedisServer {

  private static final int RANDOM_PORT_INDICATOR = 0;

  private static final Logger logger = LogService.getLogger();
  /**
   * Connection timeout in milliseconds
   */
  private static final int CONNECT_TIMEOUT_MILLIS = 1000;

  private final Supplier<DistributionConfig> configSupplier;
  private final RegionProvider regionProvider;
  private final PubSub pubsub;
  private final Supplier<Boolean> allowUnsupportedSupplier;
  private final Runnable shutdownInvoker;
  private final RedisStats redisStats;
  private final ExecutorService backgroundExecutor;
  private final EventLoopGroup selectorGroup;
  private final EventLoopGroup workerGroup;
  private final EventLoopGroup subscriberGroup;
  private final InetAddress bindAddress;
  private final Channel serverChannel;
  private final int serverPort;

  public NettyRedisServer(Supplier<DistributionConfig> configSupplier,
      RegionProvider regionProvider, PubSub pubsub,
      Supplier<Boolean> allowUnsupportedSupplier,
      Runnable shutdownInvoker, int port, String requestedAddress,
      RedisStats redisStats, ExecutorService backgroundExecutor) {
    this.configSupplier = configSupplier;
    this.regionProvider = regionProvider;
    this.pubsub = pubsub;
    this.allowUnsupportedSupplier = allowUnsupportedSupplier;
    this.shutdownInvoker = shutdownInvoker;
    this.redisStats = redisStats;
    this.backgroundExecutor = backgroundExecutor;

    if (port < RANDOM_PORT_INDICATOR) {
      throw new IllegalArgumentException(
          "The compatible-with-redis-port cannot be less than " + RANDOM_PORT_INDICATOR);
    }

    selectorGroup = createEventLoopGroup("Selector", true, 1);
    workerGroup = createEventLoopGroup("Worker", true, 0);
    subscriberGroup = createEventLoopGroup("Subscriber", true, 0);

    try {
      this.bindAddress = getBindAddress(requestedAddress);
      serverChannel = createChannel(port);
    } catch (ManagementException e) {
      stop();
      throw e;
    }
    serverPort = getActualPort();
    logStartupMessage();
  }

  private Channel createChannel(int port) {
    ServerBootstrap serverBootstrap =
        new ServerBootstrap()
            .group(selectorGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(createChannelInitializer())
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.SO_RCVBUF, getBufferSize())
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    return createBoundChannel(serverBootstrap, port);
  }

  public void stop() {
    ChannelFuture closeFuture = null;
    if (serverChannel != null) {
      closeFuture = serverChannel.closeFuture();
    }
    workerGroup.shutdownGracefully();
    subscriberGroup.shutdownGracefully();
    Future<?> bossFuture = selectorGroup.shutdownGracefully();
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

  private ChannelInitializer<SocketChannel> createChannelInitializer() {
    String redisPassword = configSupplier.get().getRedisPassword();
    final byte[] redisPasswordBytes =
        StringUtils.isBlank(redisPassword) ? null : Coder.stringToBytes(redisPassword);

    return new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel socketChannel) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "GeodeRedisServer-Connection established with " + socketChannel.remoteAddress());
        }
        ChannelPipeline pipeline = socketChannel.pipeline();
        addSSLIfEnabled(socketChannel, pipeline);
        pipeline.addLast(ByteToCommandDecoder.class.getSimpleName(),
            new ByteToCommandDecoder(redisStats));
        pipeline.addLast(new WriteTimeoutHandler(10));
        pipeline.addLast(ExecutionHandlerContext.class.getSimpleName(),
            new ExecutionHandlerContext(socketChannel, regionProvider, pubsub,
                allowUnsupportedSupplier, shutdownInvoker, redisStats, backgroundExecutor,
                subscriberGroup, redisPasswordBytes, getPort()));
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
    try (FileInputStream fileInputStream =
        new FileInputStream(sslConfigForComponent.getKeystore())) {
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(fileInputStream, sslConfigForComponent.getKeystorePassword().toCharArray());
      // Set up key manager factory to use our key store
      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, sslConfigForComponent.getKeystorePassword().toCharArray());

      SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(kmf);
      sslContext = sslContextBuilder.build();

    } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException | IOException
        | CertificateException e) {
      throw new RuntimeException(e);
    }
    p.addLast(sslContext.newHandler(ch.alloc()));
  }

  private Channel createBoundChannel(ServerBootstrap serverBootstrap, int requestedPort) {
    int port = requestedPort == RANDOM_PORT_INDICATOR ? 0 : requestedPort;
    ChannelFuture channelFuture =
        serverBootstrap.bind(new InetSocketAddress(bindAddress, port));
    try {
      channelFuture.syncUninterruptibly();
    } catch (Exception ex) {
      throw new ManagementException(
          "Could not start server compatible with Redis using bind address: " + bindAddress +
              " and port: " + port + ". " +
              "Please make sure nothing else is running on this address/port combination.",
          ex);
    }
    return channelFuture.channel();
  }

  private int getActualPort() {
    return ((InetSocketAddress) serverChannel.localAddress()).getPort();
  }

  private void logStartupMessage() {
    String logMessage = "GeodeRedisServer started {" + bindAddress + ":" + serverPort + "}";
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

  /**
   * Helper method to get the host name to bind to
   *
   * @return The InetAddress to bind to
   */
  private static InetAddress getBindAddress(String requestedAddress) {
    if (requestedAddress == null || requestedAddress.isEmpty()
        || requestedAddress.equals(LocalHostUtil.getAnyLocalAddress().getHostAddress())) {
      return LocalHostUtil.getAnyLocalAddress();
    } else {
      try {
        return InetAddress.getByName(requestedAddress);
      } catch (UnknownHostException e) {
        throw new ManagementException(
            "Could not start server compatible with Redis using bind address: '" + requestedAddress
                + "'. Please make sure that this is a valid address for this host.",
            e);
      }
    }
  }

  private static EventLoopGroup createEventLoopGroup(String name, boolean isDaemon, int nThreads) {
    String fullName = "GeodeRedisServer-" + name + "Thread-";
    ThreadFactory threadFactory = new LoggingThreadFactory(fullName, isDaemon);
    return new NioEventLoopGroup(nThreads, threadFactory);
  }

}
