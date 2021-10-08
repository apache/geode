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



import static org.apache.geode.redis.internal.RedisConstants.DEFAULT_REDIS_CONNECT_TIMEOUT_MILLIS;
import static org.apache.geode.redis.internal.RedisConstants.DEFAULT_REDIS_WRITE_TIMEOUT_SECONDS;
import static org.apache.geode.redis.internal.RedisConstants.WRITE_TIMEOUT_SECONDS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

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
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.KeyManagerFactoryWrapper;
import io.netty.handler.ssl.util.TrustManagerFactoryWrapper;
import io.netty.handler.timeout.WriteTimeoutHandler;
import io.netty.util.concurrent.Future;
import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.filewatch.FileWatchingX509ExtendedKeyManager;
import org.apache.geode.internal.net.filewatch.FileWatchingX509ExtendedTrustManager;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.ManagementException;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.pubsub.PubSub;
import org.apache.geode.redis.internal.services.RedisSecurityService;
import org.apache.geode.redis.internal.statistics.RedisStats;

public class NettyRedisServer {

  private static final int RANDOM_PORT_INDICATOR = 0;

  private static final Logger logger = LogService.getLogger();

  private final Supplier<DistributionConfig> configSupplier;
  private final RegionProvider regionProvider;
  private final PubSub pubsub;
  private final Supplier<Boolean> allowUnsupportedSupplier;
  private final Runnable shutdownInvoker;
  private final RedisStats redisStats;
  private final EventLoopGroup selectorGroup;
  private final EventLoopGroup workerGroup;
  private final InetAddress bindAddress;
  private final Channel serverChannel;
  private final int serverPort;
  private final DistributedMember member;
  private final RedisSecurityService securityService;
  private final int connectTimeoutMillis;
  private final int writeTimeoutSeconds;

  public NettyRedisServer(Supplier<DistributionConfig> configSupplier,
      RegionProvider regionProvider, PubSub pubsub, Supplier<Boolean> allowUnsupportedSupplier,
      Runnable shutdownInvoker, int port, String requestedAddress, RedisStats redisStats,
      DistributedMember member, RedisSecurityService securityService) {
    this.configSupplier = configSupplier;
    this.regionProvider = regionProvider;
    this.pubsub = pubsub;
    this.allowUnsupportedSupplier = allowUnsupportedSupplier;
    this.shutdownInvoker = shutdownInvoker;
    this.redisStats = redisStats;
    this.member = member;
    this.securityService = securityService;

    int tempTimeout;
    // get connect timeout from system property
    tempTimeout = Integer.getInteger(RedisConstants.CONNECT_TIMEOUT_MILLIS,
        DEFAULT_REDIS_CONNECT_TIMEOUT_MILLIS);
    if (tempTimeout <= 0) {
      tempTimeout = DEFAULT_REDIS_CONNECT_TIMEOUT_MILLIS;
    }
    this.connectTimeoutMillis = tempTimeout;

    // get write timeout from system property
    tempTimeout = Integer.getInteger(WRITE_TIMEOUT_SECONDS, DEFAULT_REDIS_WRITE_TIMEOUT_SECONDS);
    if (tempTimeout <= 0) {
      tempTimeout = DEFAULT_REDIS_WRITE_TIMEOUT_SECONDS;
    }
    this.writeTimeoutSeconds = tempTimeout;

    selectorGroup = createEventLoopGroup("Selector", true, 1);
    workerGroup = createEventLoopGroup("Worker", true, 0);

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
            .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, DEFAULT_REDIS_CONNECT_TIMEOUT_MILLIS)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    return createBoundChannel(serverBootstrap, port);
  }

  public void stop() {
    ChannelFuture closeFuture = null;
    if (serverChannel != null) {
      closeFuture = serverChannel.closeFuture();
    }
    workerGroup.shutdownGracefully();
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

  public int getConnectTimeoutMillis() {
    return connectTimeoutMillis;
  }

  public int getWriteTimeoutSeconds() {
    return writeTimeoutSeconds;
  }

  private ChannelInitializer<SocketChannel> createChannelInitializer() {
    String redisUsername = configSupplier.get().getRedisUsername();

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
            new ByteToCommandDecoder(redisStats, securityService, socketChannel.id()));
        pipeline.addLast(new WriteTimeoutHandler(writeTimeoutSeconds));
        pipeline.addLast(ExecutionHandlerContext.class.getSimpleName(),
            new ExecutionHandlerContext(socketChannel, regionProvider, pubsub,
                allowUnsupportedSupplier, shutdownInvoker, redisStats, redisUsername,
                getPort(), member, securityService));
      }
    };
  }

  private void addSSLIfEnabled(SocketChannel ch, ChannelPipeline p) {

    SSLConfig sslConfigForServer =
        SSLConfigurationFactory.getSSLConfigForComponent(configSupplier.get(),
            SecurableCommunicationChannel.SERVER);

    if (!sslConfigForServer.isEnabled()) {
      return;
    }

    if (sslConfigForServer.getKeystore() == null) {
      throw new IllegalStateException(
          "Cannot start netty as no key manager is configured. Please ensure that the GemFire property 'ssl-keystore' is set.");
    }

    SslContext sslContext;
    try {
      KeyManagerFactory keyManagerFactory = new KeyManagerFactoryWrapper(
          FileWatchingX509ExtendedKeyManager.newFileWatchingKeyManager(sslConfigForServer));

      TrustManagerFactory trustManagerFactory = null;
      if (sslConfigForServer.getTruststore() != null) {
        trustManagerFactory = new TrustManagerFactoryWrapper(
            FileWatchingX509ExtendedTrustManager.newFileWatchingTrustManager(sslConfigForServer));
      }

      SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(keyManagerFactory);
      sslContextBuilder.trustManager(trustManagerFactory);

      if (!sslConfigForServer.isAnyCiphers()) {
        sslContextBuilder.ciphers(Arrays.asList(sslConfigForServer.getCiphersAsStringArray()));
      }

      if (!sslConfigForServer.isAnyProtocols()) {
        sslContextBuilder.protocols(
            Arrays.asList(sslConfigForServer.getProtocolsAsStringArray()));
      }

      if (sslConfigForServer.isRequireAuth()) {
        sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
      }
      sslContext = sslContextBuilder.build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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
