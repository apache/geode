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

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.redis.internal.RedisLockServiceMBean.OBJECTNAME__REDISLOCKSERVICE_MBEAN;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
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

import org.apache.geode.GemFireConfigException;
import org.apache.geode.annotations.Experimental;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.hll.HyperLogLogPlus;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.redis.internal.executor.CommandFunction;
import org.apache.geode.redis.internal.serverinitializer.NamedThreadFactory;

/**
 * The GeodeRedisServer is a server that understands the Redis protocol. As commands are sent to the
 * server, each command is picked up by a thread, interpreted and then executed and a response is
 * sent back to the client. The default connection port is 6379 but that can be altered when run
 * through gfsh or started through the provided static main class.
 */

@Experimental
public class GeodeRedisServer {
  /**
   * Thread used to start main method
   */
  @MakeNotStatic
  private static Thread mainThread = null;

  /**
   * The default Redis port as specified by their protocol, {@code DEFAULT_REDIS_SERVER_PORT}
   */
  public static final int DEFAULT_REDIS_SERVER_PORT = 6379;

  private static final int RANDOM_PORT_INDICATOR = 0;

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
   * Logging level
   */
  private final String logLevel;

  /**
   * The cache instance pointer on this vm
   */
  private Cache cache;

  /**
   * Channel to be closed when shutting down
   */
  private Channel serverChannel;

  /**
   * Gem logwriter
   */
  @SuppressWarnings("deprecation")
  private org.apache.geode.LogWriter logger;

  private RegionProvider regionProvider;

  private final MetaCacheListener metaListener;

  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private static final int numExpirationThreads = 1;
  private final ScheduledExecutorService expirationExecutor;

  /**
   * Map of futures to be executed for key expirations
   */
  private final ConcurrentMap<ByteArrayWrapper, ScheduledFuture<?>> expirationFutures;


  /**
   * The field that defines the name of the {@link Region} which holds all of the strings. The
   * current value of this field is {@code STRING_REGION}.
   */
  public static final String STRING_REGION = "ReDiS_StRiNgS";

  /**
   * The field that defines the name of the {@link Region} which holds all of the HyperLogLogs. The
   * current value of this field is {@code HLL_REGION}.
   */
  public static final String HLL_REGION = "ReDiS_HlL";

  /**
   * The name of the region that holds data stored in redis.
   * Currently this is the meta data but at some point the value for a particular
   * type will be changed to an instance of {@link RedisData}
   */
  public static final String REDIS_DATA_REGION = "ReDiS_DaTa";

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

  private KeyRegistrar keyRegistrar;
  private PubSub pubSub;
  private RedisLockService hashLockService;

  @VisibleForTesting
  public KeyRegistrar getKeyRegistrar() {
    return keyRegistrar;
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

  /**
   * Constructor for {@code GeodeRedisServer} that will start the server on a random port and bind
   * to the first non-loopback address
   */
  public GeodeRedisServer() {
    this(null, RANDOM_PORT_INDICATOR, null);
  }

  /**
   * Constructor for {@code GeodeRedisServer} that will start the server on the given port and bind
   * to the first non-loopback address
   *
   * @param port The port the server will bind to, will use {@value #DEFAULT_REDIS_SERVER_PORT} by
   *        default
   */
  public GeodeRedisServer(int port) {
    this(null, port, null);
  }

  /**
   * Constructor for {@code GeodeRedisServer} that will start the server and bind to the given
   * address and port
   *
   * @param bindAddress The address to which the server will attempt to bind to
   * @param port The port the server will bind to, will use {@value #DEFAULT_REDIS_SERVER_PORT}
   *        by default, and will throw IllegalArgumentException if argument is less than 0
   */
  public GeodeRedisServer(String bindAddress, int port) {
    this(bindAddress, port, null);
  }

  /**
   * Constructor for {@code GeodeRedisServer} that will start the server and bind to the given
   * address and port. Keep in mind that the log level configuration will only be set if a {@link
   * Cache} does not already exist, if one already exists then setting that property will have no
   * effect.
   *
   * @param bindAddress The address to which the server will attempt to bind to
   * @param port The port the server will bind to, will throw an IllegalArgumentException if
   *        argument is less than 0. If the port is
   *        {@value #RANDOM_PORT_INDICATOR} a random port is assigned.
   * @param logLevel The logging level to be used by GemFire
   */
  public GeodeRedisServer(String bindAddress, int port, String logLevel) {
    if (port < RANDOM_PORT_INDICATOR) {
      throw new IllegalArgumentException("Redis port cannot be less than 0");
    }
    serverPort = port;
    this.bindAddress = bindAddress;
    this.logLevel = logLevel;
    numWorkerThreads = setNumWorkerThreads();
    singleThreadPerConnection = numWorkerThreads == 0;
    numSelectorThreads = 1;
    metaListener = new MetaCacheListener();
    expirationFutures = new ConcurrentHashMap<>();
    expirationExecutor =
        Executors.newScheduledThreadPool(numExpirationThreads,
            new NamedThreadFactory("GemFireRedis-ScheduledExecutor-", true));
    shutdown = false;
    started = false;
  }

  /**
   * Helper method to get the host name to bind to
   *
   * @return The InetAddress to bind to
   */
  private InetAddress getBindAddress() throws UnknownHostException {
    return bindAddress == null || bindAddress.isEmpty() ? LocalHostUtil.getLocalHost()
        : InetAddress.getByName(bindAddress);
  }

  /**
   * This is function to call on a {@code GeodeRedisServer} instance to start it running
   */
  public synchronized void start() {
    if (!started) {
      try {
        startGemFire();
        initializeRedis();
        startRedisServer();
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException("Could not start Server", e);
      }
      started = true;
    }
  }

  /**
   * Initializes the {@link Cache}, and creates Redis necessities Region and protects declares that
   * {@link Region} to be protected. Also, every {@code GeodeRedisServer} will check for entries
   * already in the meta data Region.
   */
  @SuppressWarnings("deprecation")
  private void startGemFire() {
    Cache cache = GemFireCacheImpl.getInstance();
    if (cache == null) {
      synchronized (GeodeRedisServer.class) {
        cache = GemFireCacheImpl.getInstance();
        if (cache == null) {
          CacheFactory cacheFactory = new CacheFactory();
          if (logLevel != null) {
            cacheFactory.set(LOG_LEVEL, logLevel);
          }
          cache = cacheFactory.create();
        }
      }
    }
    this.cache = cache;
    logger = cache.getLogger();
  }

  @VisibleForTesting
  public RegionProvider getRegionProvider() {
    return regionProvider;
  }

  private void initializeRedis() {
    synchronized (cache) {
      Region<ByteArrayWrapper, ByteArrayWrapper> stringsRegion;

      Region<ByteArrayWrapper, HyperLogLogPlus> hLLRegion;
      Region<ByteArrayWrapper, RedisData> redisData;
      InternalCache gemFireCache = (InternalCache) cache;

      if ((stringsRegion = cache.getRegion(STRING_REGION)) == null) {
        RegionFactory<ByteArrayWrapper, ByteArrayWrapper> regionFactory =
            gemFireCache.createRegionFactory(DEFAULT_REGION_TYPE);
        stringsRegion = regionFactory.create(STRING_REGION);
      }
      if ((hLLRegion = cache.getRegion(HLL_REGION)) == null) {
        RegionFactory<ByteArrayWrapper, HyperLogLogPlus> regionFactory =
            gemFireCache.createRegionFactory(DEFAULT_REGION_TYPE);
        hLLRegion = regionFactory.create(HLL_REGION);
      }

      if ((redisData = cache.getRegion(REDIS_DATA_REGION)) == null) {
        InternalRegionFactory<ByteArrayWrapper, RedisData> redisMetaDataFactory =
            gemFireCache.createInternalRegionFactory(DEFAULT_REGION_TYPE);
        redisMetaDataFactory.addCacheListener(metaListener);
        redisMetaDataFactory.setInternalRegion(true).setIsUsedForMetaRegion(true);
        redisData = redisMetaDataFactory.create(REDIS_DATA_REGION);
      }

      keyRegistrar = new KeyRegistrar(redisData);
      hashLockService = new RedisLockService();
      pubSub = new PubSubImpl(new Subscriptions());
      regionProvider = new RegionProvider(stringsRegion, hLLRegion, keyRegistrar,
          expirationFutures, expirationExecutor, DEFAULT_REGION_TYPE, redisData);
      keyRegistrar.register(Coder.stringToByteArrayWrapper(REDIS_DATA_REGION),
          RedisDataType.REDIS_PROTECTED);
      keyRegistrar.register(Coder.stringToByteArrayWrapper(HLL_REGION),
          RedisDataType.REDIS_PROTECTED);
      keyRegistrar.register(Coder.stringToByteArrayWrapper(STRING_REGION),
          RedisDataType.REDIS_PROTECTED);

      CommandFunction.register(regionProvider);
    }

    checkForRegions();
    registerLockServiceMBean();
  }

  public static final int PROTECTED_KEY_COUNT = 3;

  @VisibleForTesting
  public RedisLockService getLockService() {
    return hashLockService;
  }

  private void registerLockServiceMBean() {
    ManagementService sms = SystemManagementService.getManagementService(cache);

    try {
      ObjectName mbeanON = new ObjectName(OBJECTNAME__REDISLOCKSERVICE_MBEAN);
      sms.registerMBean(hashLockService, mbeanON);
      MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

      Set<ObjectName> names = platformMBeanServer.queryNames(mbeanON, null);
      if (names.isEmpty()) {
        platformMBeanServer.registerMBean(hashLockService, mbeanON);
        logger.info("Registered RedisLockServiceMBean on " + mbeanON);
      }
    } catch (InstanceAlreadyExistsException | MBeanRegistrationException
        | NotCompliantMBeanException | MalformedObjectNameException e) {
      throw new GemFireConfigException("Error while configuring RedisLockServiceMBean", e);
    }
  }

  private void checkForRegions() {
    Collection<Entry<ByteArrayWrapper, RedisData>> entrySet = keyRegistrar.keyInfos();
    for (Entry<ByteArrayWrapper, RedisData> entry : entrySet) {
      ByteArrayWrapper key = entry.getKey();
      RedisDataType type = entry.getValue().getType();
      if (!regionProvider.typeUsesDynamicRegions(type)) {
        continue;
      }
      if (cache.getRegion(key.toString()) != null) {
        // TODO: this seems to be correct (i.e. no need to call createRemoteRegionReferenceLocally
        // if region already exists).
        // HOWEVER: createRemoteRegionReferenceLocally ends up doing nothing if the region does not
        // exist. So this caller of createRemoteRegionReferenceLocally basically does nothing.
        // createRemoteRegionReferenceLocally might be needed even if the region exists because
        // local state needs to be initialized (like indexes and queries).
        continue;
      }
      try {
        regionProvider.createRemoteRegionReferenceLocally(key, type);
      } catch (Exception e) {
        // TODO: this eats the exception so if something really is wrong we don't fail but just log.
        if (logger.errorEnabled()) {
          logger.error(e);
        }
      }
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
        new NamedThreadFactory("GeodeRedisServer-SelectorThread-", true);

    ThreadFactory workerThreadFactory =
        new NamedThreadFactory("GeodeRedisServer-WorkerThread-", false);

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
      socketClass = NioServerSocketChannel.class;
    }
    return socketClass;
  }

  private Channel createBoundChannel(ServerBootstrap serverBootstrap)
      throws InterruptedException, UnknownHostException {
    ChannelFuture channelFuture =
        serverBootstrap.bind(new InetSocketAddress(getBindAddress(),
            serverPort == RANDOM_PORT_INDICATOR ? 0 : serverPort))
            .sync();

    serverPort = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();

    logStartupMessage();

    return channelFuture.channel();
  }

  private void logStartupMessage() throws UnknownHostException {
    if (logger.infoEnabled()) {
      String logMessage = "GeodeRedisServer started {" + getBindAddress() + ":" + serverPort
          + "}, Selector threads: " + numSelectorThreads;
      if (singleThreadPerConnection) {
        logMessage += ", One worker thread per connection";
      } else {
        logMessage += ", Worker threads: " + numWorkerThreads;
      }
      logger.info(logMessage);
    }
  }

  private ChannelInitializer<SocketChannel> createChannelInitializer(byte[] redisPasswordBytes) {
    return new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel socketChannel) {
        if (logger.fineEnabled()) {
          logger.fine(
              "GeodeRedisServer-Connection established with " + socketChannel.remoteAddress());
        }
        ChannelPipeline pipeline = socketChannel.pipeline();
        addSSLIfEnabled(socketChannel, pipeline);
        pipeline.addLast(ByteToCommandDecoder.class.getSimpleName(), new ByteToCommandDecoder());
        pipeline.addLast(new WriteTimeoutHandler(10));
        pipeline.addLast(ExecutionHandlerContext.class.getSimpleName(),
            new ExecutionHandlerContext(socketChannel, cache, regionProvider, GeodeRedisServer.this,
                redisPasswordBytes,
                keyRegistrar, pubSub, hashLockService));
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
   * Takes an entry event and processes it. If the entry denotes that a {@link
   * RedisDataType#REDIS_LIST} or {@link RedisDataType#REDIS_SORTEDSET} was created then this
   * function will call the necessary calls to create the parameterized queries for those keys.
   *
   * @param event EntryEvent from meta data region
   */
  private void afterKeyCreate(EntryEvent<ByteArrayWrapper, RedisData> event) {
    if (event.isOriginRemote()) {
      final ByteArrayWrapper key = event.getKey();
      final RedisData value = event.getNewValue();
      try {
        regionProvider.createRemoteRegionReferenceLocally(key, value.getType());
      } catch (RegionDestroyedException ignore) { // Region already destroyed, ignore
      }
    }
  }

  /**
   * When a key is removed then this function will make sure the associated queries with the key are
   * also removed from each vm to avoid unnecessary data retention
   */
  private void afterKeyDestroy(EntryEvent<ByteArrayWrapper, RedisData> event) {
    if (event.isOriginRemote()) {
      final ByteArrayWrapper key = event.getKey();
      final RedisData value = event.getOldValue();
      regionProvider.removeRegionReferenceLocally(key, value.getType());
    }
  }

  private class MetaCacheListener extends CacheListenerAdapter<ByteArrayWrapper, RedisData> {
    @Override
    public void afterCreate(EntryEvent<ByteArrayWrapper, RedisData> event) {
      afterKeyCreate(event);
    }

    @Override
    public void afterDestroy(EntryEvent<ByteArrayWrapper, RedisData> event) {
      afterKeyDestroy(event);
    }
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
      if (logger.infoEnabled()) {
        logger.info("GeodeRedisServer shutting down");
      }
      ChannelFuture closeFuture = serverChannel.closeFuture();
      Future<?> c = workerGroup.shutdownGracefully();
      Future<?> c2 = bossGroup.shutdownGracefully();
      serverChannel.close();
      c.syncUninterruptibly();
      c2.syncUninterruptibly();
      regionProvider.close();
      if (mainThread != null) {
        mainThread.interrupt();
      }
      for (ScheduledFuture<?> f : expirationFutures.values()) {
        f.cancel(true);
      }
      expirationFutures.clear();
      expirationExecutor.shutdownNow();
      closeFuture.syncUninterruptibly();
      shutdown = true;
    }
  }

  public int getPort() {
    return serverPort;
  }

  /**
   * Static main method that allows the {@code GeodeRedisServer} to be started from the command
   * line. The supported command line arguments are
   * <p>
   * -port= <br>
   * -bind-address= <br>
   * -log-level=
   *
   * @param args Command line args
   */
  public static void main(String[] args) {
    int port = DEFAULT_REDIS_SERVER_PORT;
    String bindAddress = null;
    String logLevel = null;
    for (String arg : args) {
      if (arg.startsWith("-port")) {
        port = getPort(arg);
      } else if (arg.startsWith("-bind-address")) {
        bindAddress = getBindAddress(arg);
      } else if (arg.startsWith("-log-level")) {
        logLevel = getLogLevel(arg);
      }
    }
    mainThread = Thread.currentThread();
    GeodeRedisServer server = new GeodeRedisServer(bindAddress, port, logLevel);
    server.start();
    while (true) {
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException e1) {
        break;
      } catch (Exception ignored) {
      }
    }
  }

  /**
   * Helper method to parse the port to a number
   *
   * @param arg String where the argument is
   * @return The port number when the correct syntax was used, otherwise will return {@link
   *         #DEFAULT_REDIS_SERVER_PORT}
   */
  private static int getPort(String arg) {
    int port = DEFAULT_REDIS_SERVER_PORT;
    if (arg != null && arg.length() > 6) {
      if (arg.startsWith("-port")) {
        String p = arg.substring(arg.indexOf('=') + 1);
        p = p.trim();
        try {
          port = Integer.parseInt(p);
        } catch (NumberFormatException e) {
          System.out.println("Unable to parse port, using default port");
        }
      }
    }
    return port;
  }

  /**
   * Helper method to parse bind address
   *
   * @param arg String holding bind address
   * @return Bind address
   */
  private static String getBindAddress(String arg) {
    String address = null;
    if (arg != null && arg.length() > 14) {
      if (arg.startsWith("-bind-address")) {
        String p = arg.substring(arg.indexOf('=') + 1);
        address = p.trim();
      }
    }
    return address;
  }

  /**
   * Helper method to parse log level
   *
   * @param arg String holding log level
   * @return Log level
   */
  private static String getLogLevel(String arg) {
    String logLevel = null;
    if (arg != null && arg.length() > 11) {
      if (arg.startsWith("-log-level")) {
        String p = arg.substring(arg.indexOf('=') + 1);
        logLevel = p.trim();
      }
    }
    return logLevel;
  }

}
