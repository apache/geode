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
package org.apache.geode.redis.internal;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.logging.internal.executors.LoggingExecutors.newSingleThreadScheduledExecutor;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.ManagementException;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.executor.CommandFunction;
import org.apache.geode.redis.internal.executor.StripedExecutor;
import org.apache.geode.redis.internal.executor.SynchronizedStripedExecutor;
import org.apache.geode.redis.internal.executor.key.RedisKeyCommands;
import org.apache.geode.redis.internal.executor.key.RedisKeyCommandsFunctionExecutor;
import org.apache.geode.redis.internal.executor.key.RenameFunction;
import org.apache.geode.redis.internal.gfsh.RedisCommandFunction;
import org.apache.geode.redis.internal.netty.ByteToCommandDecoder;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.pubsub.PubSub;
import org.apache.geode.redis.internal.pubsub.PubSubImpl;
import org.apache.geode.redis.internal.pubsub.Subscriptions;

/**
 * The GeodeRedisServer is a server that understands the Redis protocol. As commands are sent to the
 * server, each command is picked up by a thread, interpreted and then executed and a response is
 * sent back to the client. The default connection port is 6379 but that can be altered when run
 * through gfsh or started through the provided static main class.
 */

public class GeodeRedisServer {
  /**
   * The default Redis port as specified by their protocol, {@code DEFAULT_REDIS_SERVER_PORT}
   */
  public static final int DEFAULT_REDIS_SERVER_PORT = 6379;

  private static final int RANDOM_PORT_INDICATOR = 0;

  public static final String ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM =
      "enable-redis-unsupported-commands";

  private final boolean ENABLE_REDIS_UNSUPPORTED_COMMANDS =
      Boolean.getBoolean(ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM);

  /**
   * The number of threads that will work on handling requests
   */
  private final int numWorkerThreads;

  /**
   * The number of threads that will work socket selectors
   */
  private final int numSelectorThreads;

  /**
   * The actual port being used by the server
   */
  private int serverPort;

  /**
   * The address to bind to
   */
  private final String bindAddress;

  /**
   * Connection timeout in milliseconds
   */
  private static final int connectTimeoutMillis = 1000;

  /**
   * Temporary constant whether to use old single thread per connection model for worker group
   */
  private boolean singleThreadPerConnection;

  /**
   * The cache instance pointer on this vm
   */
  private InternalCache cache;

  /**
   * Channel to be closed when shutting down
   */
  private Channel serverChannel;

  private static final Logger logger = LogService.getLogger();

  private RegionProvider regionProvider;

  private EventLoopGroup bossGroup;

  private EventLoopGroup workerGroup;
  private EventLoopGroup subscriberGroup;
  private final ScheduledExecutorService expirationExecutor;
  /**
   * The name of the region that holds data stored in redis.
   */
  public static final String REDIS_DATA_REGION = "__REDIS_DATA";
  public static final String REDIS_CONFIG_REGION = "__REDIS_CONFIG";

  /**
   * System property name that can be used to set the number of threads to be used by the
   * GeodeRedisServer
   */
  public static final String NUM_THREADS_SYS_PROP_NAME = "gemfireredis.numthreads";

  /**
   * The default region type
   */
  public final RegionShortcut DEFAULT_REGION_TYPE = RegionShortcut.PARTITION_REDUNDANT;

  private boolean shutdown;

  private boolean started;
  private PubSub pubSub;


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

  /**
   * Constructor for {@code GeodeRedisServer} that will configure the server to bind to the given
   * address and port.
   *
   * @param bindAddress The address to which the server will attempt to bind to; null
   *        causes it to bind to all local addresses.
   * @param port The port the server will bind to, will throw an IllegalArgumentException if
   *        argument is less than 0. If the port is
   *        {@value #RANDOM_PORT_INDICATOR} a random port is assigned.
   */
  public GeodeRedisServer(String bindAddress, int port, InternalCache cache) {
    if (port < RANDOM_PORT_INDICATOR) {
      throw new IllegalArgumentException("Redis port cannot be less than 0");
    }
    this.cache = cache;
    serverPort = port;
    this.bindAddress = bindAddress;
    numWorkerThreads = setNumWorkerThreads();
    singleThreadPerConnection = numWorkerThreads == 0;
    numSelectorThreads = 1;
    expirationExecutor = newSingleThreadScheduledExecutor("GemFireRedis-PassiveExpiration-");
    shutdown = false;
    started = false;

    if (ENABLE_REDIS_UNSUPPORTED_COMMANDS) {
      logUnsupportedCommandWarning();
    }
  }

  public void setAllowUnsupportedCommands(boolean allowUnsupportedCommands) {
    Region<String, Object> configRegion = regionProvider.getConfigRegion();
    configRegion.put(GeodeRedisServer.ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM,
        allowUnsupportedCommands);
    if (allowUnsupportedCommands) {
      logUnsupportedCommandWarning();
    }
  }

  /**
   * Precedence of the internal property overrides the global system property.
   */
  public boolean allowUnsupportedCommands() {
    return (boolean) regionProvider.getConfigRegion()
        .getOrDefault(ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM, ENABLE_REDIS_UNSUPPORTED_COMMANDS);
  }

  private void logUnsupportedCommandWarning() {
    logger.warn("Unsupported commands enabled. Unsupported commands have not been fully tested.");
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

  /**
   * This is function to call on a {@code GeodeRedisServer} instance to start it running
   */
  public synchronized void start() {
    if (!started) {
      try {
        initializeRedis();
        startRedisServer();
      } catch (IOException | InterruptedException e) {
        throw new ManagementException("Could not start Server", e);
      }
      started = true;
    }
  }

  public RegionProvider getRegionProvider() {
    return regionProvider;
  }

  public PubSub getPubSub() {
    return pubSub;
  }

  public EventLoopGroup getSubscriberGroup() {
    return subscriberGroup;
  }

  private void initializeRedis() {
    synchronized (cache) {

      Region<ByteArrayWrapper, RedisData> redisData;
      InternalRegionFactory<ByteArrayWrapper, RedisData> redisDataRegionFactory =
          cache.createInternalRegionFactory(DEFAULT_REGION_TYPE);
      redisDataRegionFactory.setInternalRegion(true).setIsUsedForMetaRegion(true);
      redisData = redisDataRegionFactory.create(REDIS_DATA_REGION);

      InternalRegionFactory<String, Object> redisConfigRegionFactory =
          cache.createInternalRegionFactory(RegionShortcut.REPLICATE);
      redisConfigRegionFactory.setInternalRegion(true).setIsUsedForMetaRegion(true);
      Region<String, Object> redisConfig = redisConfigRegionFactory.create(REDIS_CONFIG_REGION);

      pubSub = new PubSubImpl(new Subscriptions());
      regionProvider = new RegionProvider(redisData, redisConfig);

      StripedExecutor stripedExecutor = new SynchronizedStripedExecutor();

      CommandFunction.register(stripedExecutor);
      RenameFunction.register(stripedExecutor);
      RedisCommandFunction.register();
      scheduleDataExpiration(redisData);
    }
  }

  private void scheduleDataExpiration(
      Region<ByteArrayWrapper, RedisData> redisData) {
    int INTERVAL = 1;
    expirationExecutor.scheduleAtFixedRate(() -> doDataExpiration(redisData), INTERVAL, INTERVAL,
        SECONDS);
  }

  private void doDataExpiration(
      Region<ByteArrayWrapper, RedisData> redisData) {
    try {
      final long now = System.currentTimeMillis();
      Region<ByteArrayWrapper, RedisData> localPrimaryData =
          PartitionRegionHelper.getLocalPrimaryData(redisData);
      RedisKeyCommands redisKeyCommands = new RedisKeyCommandsFunctionExecutor(redisData);
      for (Map.Entry<ByteArrayWrapper, RedisData> entry : localPrimaryData.entrySet()) {
        if (entry.getValue().hasExpired(now)) {
          // pttl will do its own check using active expiration and expire the key if needed
          redisKeyCommands.pttl(entry.getKey());
        }
      }
    } catch (CacheClosedException | EntryDestroyedException ignore) {
    } catch (RuntimeException | Error ex) {
      logger.warn("Passive Redis expiration failed. Will try again in 1 second.", ex);
    }
  }

  /**
   * Helper method to start the server listening for connections. The server is bound to the port
   * specified by {@link GeodeRedisServer#serverPort}
   */
  private void startRedisServer() throws IOException, InterruptedException {
    Class<? extends ServerChannel> socketClass = initializeEventLoopGroups();

    InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();
    String redisPassword = system.getConfig().getRedisPassword();
    final byte[] redisPasswordBytes = Coder.stringToBytes(redisPassword);
    ServerBootstrap serverBootstrap = new ServerBootstrap();

    serverBootstrap.group(bossGroup, workerGroup).channel(socketClass)
        .childHandler(createChannelInitializer(redisPasswordBytes))
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_RCVBUF, getBufferSize())
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, GeodeRedisServer.connectTimeoutMillis)
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

  private void logStartupMessage(InetAddress bindAddress) throws UnknownHostException {
    String logMessage = "GeodeRedisServer started {" + bindAddress + ":" + serverPort
        + "}, Selector threads: " + numSelectorThreads;
    if (singleThreadPerConnection) {
      logMessage += ", One worker thread per connection";
    } else {
      logMessage += ", Worker threads: " + numWorkerThreads;
    }
    logger.info(logMessage);
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
            new ExecutionHandlerContext(socketChannel, GeodeRedisServer.this,
                redisPasswordBytes));
      }
    };
  }

  private void addSSLIfEnabled(SocketChannel ch, ChannelPipeline p) {

    SSLConfig sslConfigForComponent =
        SSLConfigurationFactory.getSSLConfigForComponent(
            ((InternalDistributedSystem) cache.getDistributedSystem()).getConfig(),
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
   * Helper method to get GemFire set socket buffer size, possibly a default of 32k
   *
   * @return Buffer size to use for server
   */
  private int getBufferSize() {
    InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();
    return system.getConfig().getSocketBufferSize();
  }

  /**
   * Shutdown method for {@code GeodeRedisServer}. This closes the {@link Cache}, interrupts all
   * execution and forcefully closes all connections.
   */
  public synchronized void shutdown() {
    if (!shutdown) {
      logger.info("GeodeRedisServer shutting down");
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
      expirationExecutor.shutdownNow();
      if (closeFuture != null) {
        closeFuture.syncUninterruptibly();
      }
      shutdown = true;
    }
  }

  public int getPort() {
    return serverPort;
  }
}
