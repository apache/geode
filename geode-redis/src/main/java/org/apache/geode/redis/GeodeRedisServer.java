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
package org.apache.geode.redis;

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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

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
import io.netty.util.concurrent.Future;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.annotations.Experimental;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
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
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ByteToCommandDecoder;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.KeyRegistrar;
import org.apache.geode.redis.internal.PubSub;
import org.apache.geode.redis.internal.PubSubImpl;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.RedisLockService;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.Subscriptions;

/**
 * The GeodeRedisServer is a server that understands the Redis protocol. As commands are sent to the
 * server, each command is picked up by a thread, interpreted and then executed and a response is
 * sent back to the client. The default connection port is 6379 but that can be altered when run
 * through GFSH or started through the provided static main class.
 * <p>
 * Each Redis data type instance is stored in a separate {@link Region} except for the Strings and
 * HyperLogLogs which are collectively stored in one Region respectively. That Region along with a
 * meta data region used internally are protected so the client may not store keys with the name
 * {@link GeodeRedisServer#REDIS_META_DATA_REGION} or {@link GeodeRedisServer#STRING_REGION}. The
 * default Region type is {@link RegionShortcut#PARTITION} although this can be changed by
 * specifying the SystemProperty {@value #DEFAULT_REGION_SYS_PROP_NAME} to a type defined by {@link
 * RegionShortcut}. If the {@link GeodeRedisServer#NUM_THREADS_SYS_PROP_NAME} system property is set
 * to 0, one thread per client will be created. Otherwise a worker thread pool of specified size is
 * used or a default size of 4 * {@link Runtime#availableProcessors()} if the property is not set.
 * <p>
 * Setting the AUTH password requires setting the property "redis-password" just as "redis-port"
 * would be in xml or through GFSH.
 * <p>
 * The supported commands are as follows:
 * <p>
 * Supported String commands - APPEND, BITCOUNT, BITOP, BITPOS, DECR, DECRBY, GET, GETBIT, GETRANGE,
 * GETSET, INCR, INCRBY, INCRBYFLOAT, MGET, MSET, MSETNX, PSETEX, SET, SETBIT, SETEX, SETNX, STRLEN
 * <p>
 * Supported List commands - LINDEX, LLEN, LPOP, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM, RPOP,
 * RPUSH, RPUSHX
 * <p>
 * Supported Hash commands - HDEL, HEXISTS, HGET, HGETALL, HINCRBY, HINCRBYFLOAT, HKEYS, HMGET,
 * HMSET, HSETNX, HLEN, HSCAN, HSET, HVALS
 * <p>
 * Supported Set commands - SADD, SCARD, SDIFF, SDIFFSTORE, SINTER, SINTERSTORE, SISMEMBER,
 * SMEMBERS, SMOVE, SREM, SPOP, SRANDMEMBER, SCAN, SUNION, SUNIONSTORE
 * <p>
 * Supported SortedSet commands - ZADD, ZCARD, ZCOUNT, ZINCRBY, ZLEXCOUNT, ZRANGE, ZRANGEBYLEX,
 * ZRANGEBYSCORE, ZRANK, ZREM, ZREMRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREVRANGE,
 * ZREVRANGEBYSCORE, ZREVRANK, ZSCAN, ZSCORE
 * <p>
 * Supported HyperLogLog commands - PFADD, PFCOUNT, PFMERGE
 * <p>
 * Supported Keys commands - DEL, DBSIZE, EXISTS, EXPIRE, EXPIREAT, FLUSHDB, FLUSHALL, KEYS,
 * PERSIST, PEXPIRE, PEXPIREAT, PTTL, SCAN, TTL
 * <p>
 * Supported Transaction commands - DISCARD, EXEC, MULTI
 * <p>
 * Supported Server commands - AUTH, ECHO, PING, TIME, QUIT
 * <p>
 * <p>
 * The command executors are not explicitly documented but the functionality can be found at
 * <a href="http://redis.io/commands">Redis Commands</a>
 * <p>
 * Exceptions to the Redis Commands Documents:
 * <p>
 * <ul>
 * <li>Any command that removes keys and returns a count of removed entries will not return a total
 * remove count but rather a count of how many entries have been removed that existed on the local
 * vm, though all entries will be removed</li>
 * <li>Any command that returns a count of newly set members has an unspecified return value. The
 * command will work just as the Redis protocol states but the count will not necessary reflect the
 * number set compared to overridden.</li>
 * <li>Transactions work just as they would on a Redis instance, they are local transaction.
 * Transactions cannot be executed on data that is not local to the executing server, that is on a
 * partitioned region in a different server instance or on a persistent region that does not have
 * transactions enabled. Also, you cannot watch or unwatch keys as all keys within a GemFire
 * transaction are watched by default.</li>
 * </ul>
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
  private final int serverPort;

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

  private RegionProvider regionCache;

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
   * TThe field that defines the name of the {@link Region} which holds non-named hash. The current
   * value of this field is {@value #HASH_REGION}.
   */
  public static final String HASH_REGION = "ReDiS_HASH";

  /**
   * TThe field that defines the name of the {@link Region} which holds sets. The current value of
   * this field is {@value #SET_REGION}.
   */
  public static final String SET_REGION = "ReDiS_SET";


  /**
   * The field that defines the name of the {@link Region} which holds all of the HyperLogLogs. The
   * current value of this field is {@code HLL_REGION}.
   */
  public static final String HLL_REGION = "ReDiS_HlL";

  /**
   * The field that defines the name of the {@link Region} which holds all of the Redis meta data.
   * The current value of this field is {@code REDIS_META_DATA_REGION}.
   */
  public static final String REDIS_META_DATA_REGION = "ReDiS_MeTa_DaTa";

  /**
   * The system property name used to set the default {@link Region} creation type. The property
   * name is {@code DEFAULT_REGION_SYS_PROP_NAME} and the acceptable values are types defined by
   * {@link RegionShortcut}, i.e. "PARTITION" would be used for {@link RegionShortcut#PARTITION}.
   */
  public static final String DEFAULT_REGION_SYS_PROP_NAME = "gemfireredis.regiontype";

  /**
   * System property name that can be used to set the number of threads to be used by the
   * GeodeRedisServer
   */
  public static final String NUM_THREADS_SYS_PROP_NAME = "gemfireredis.numthreads";

  /**
   * The actual {@link RegionShortcut} type specified by the system property {@value
   * #DEFAULT_REGION_SYS_PROP_NAME}.
   */
  public final RegionShortcut DEFAULT_REGION_TYPE;

  private boolean shutdown;
  private boolean started;
  private KeyRegistrar keyRegistrar;
  private PubSub pubSub;
  private RedisLockService hashLockService;


  /**
   * Determine the {@link RegionShortcut} type from a String value. If the String value doesn't map
   * to a RegionShortcut type then {@link RegionShortcut#PARTITION} will be used by default.
   *
   * @return {@link RegionShortcut}
   */
  private static RegionShortcut setRegionType() {
    String regionType = System.getProperty(DEFAULT_REGION_SYS_PROP_NAME, "PARTITION");
    RegionShortcut type;
    try {
      type = RegionShortcut.valueOf(regionType);
    } catch (Exception e) {
      type = RegionShortcut.PARTITION;
    }
    return type;
  }

  /**
   * Helper method to set the number of worker threads
   *
   * @return If the System property {@value #NUM_THREADS_SYS_PROP_NAME} is set then that number is
   *         used, otherwise 4 * # of cores
   */
  private int setNumWorkerThreads() {
    String prop = System.getProperty(NUM_THREADS_SYS_PROP_NAME);
    int numCores = Runtime.getRuntime().availableProcessors();
    int def = 4 * numCores;
    if (prop == null || prop.isEmpty()) {
      return def;
    }
    int threads;
    try {
      threads = Integer.parseInt(prop);
    } catch (NumberFormatException e) {
      return def;
    }
    return threads;
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
   *        by default if argument is less than or equal to 0
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
   * @param port The port the server will bind to, will use {@value #DEFAULT_REDIS_SERVER_PORT}
   *        by default if argument is less than or equal to 0
   * @param logLevel The logging level to be used by GemFire
   */
  public GeodeRedisServer(String bindAddress, int port, String logLevel) {
    serverPort = port <= 0 ? DEFAULT_REDIS_SERVER_PORT : port;
    this.bindAddress = bindAddress;
    this.logLevel = logLevel;
    numWorkerThreads = setNumWorkerThreads();
    singleThreadPerConnection = numWorkerThreads == 0;
    numSelectorThreads = 1;
    metaListener = new MetaCacheListener();
    expirationFutures = new ConcurrentHashMap<>();
    expirationExecutor =
        Executors.newScheduledThreadPool(numExpirationThreads, new ThreadFactory() {
          private final AtomicInteger counter = new AtomicInteger();

          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("GemFireRedis-ScheduledExecutor-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
          }

        });
    DEFAULT_REGION_TYPE = setRegionType();
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

  private void initializeRedis() {
    synchronized (cache) {
      Region<ByteArrayWrapper, ByteArrayWrapper> stringsRegion;

      Region<ByteArrayWrapper, HyperLogLogPlus> hLLRegion;
      Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> redisHash;
      Region<String, RedisDataType> redisMetaData;
      Region<ByteArrayWrapper, Set<ByteArrayWrapper>> redisSet;
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

      if ((redisHash = cache.getRegion(HASH_REGION)) == null) {
        RegionFactory<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> regionFactory =
            gemFireCache.createRegionFactory(DEFAULT_REGION_TYPE);
        redisHash = regionFactory.create(HASH_REGION);
      }

      if ((redisSet = cache.getRegion(SET_REGION)) == null) {
        RegionFactory<ByteArrayWrapper, Set<ByteArrayWrapper>> regionFactory =
            gemFireCache.createRegionFactory(DEFAULT_REGION_TYPE);
        redisSet = regionFactory.create(SET_REGION);
      }

      if ((redisMetaData = cache.getRegion(REDIS_META_DATA_REGION)) == null) {
        InternalRegionFactory<String, RedisDataType> redisMetaDataFactory =
            gemFireCache.createInternalRegionFactory();
        redisMetaDataFactory.addCacheListener(metaListener);
        redisMetaDataFactory.setDataPolicy(DataPolicy.REPLICATE);
        redisMetaDataFactory.setInternalRegion(true).setIsUsedForMetaRegion(true);
        redisMetaData = redisMetaDataFactory.create(REDIS_META_DATA_REGION);
      }

      keyRegistrar = new KeyRegistrar(redisMetaData);
      hashLockService = new RedisLockService();
      pubSub = new PubSubImpl(new Subscriptions());
      regionCache = new RegionProvider(stringsRegion, hLLRegion, keyRegistrar,
          expirationFutures, expirationExecutor, DEFAULT_REGION_TYPE, redisHash, redisSet);
      redisMetaData.put(REDIS_META_DATA_REGION, RedisDataType.REDIS_PROTECTED);
      redisMetaData.put(HLL_REGION, RedisDataType.REDIS_PROTECTED);
      redisMetaData.put(STRING_REGION, RedisDataType.REDIS_PROTECTED);
      redisMetaData.put(SET_REGION, RedisDataType.REDIS_PROTECTED);
      redisMetaData.put(HASH_REGION, RedisDataType.REDIS_PROTECTED);
    }

    checkForRegions();
    registerLockServiceMBean();
  }

  private void registerLockServiceMBean() {
    try {
      ObjectName mbeanON = new ObjectName(OBJECTNAME__REDISLOCKSERVICE_MBEAN);
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
    Collection<Entry<String, RedisDataType>> entrySet = keyRegistrar.keyInfos();
    for (Entry<String, RedisDataType> entry : entrySet) {
      String regionName = entry.getKey();
      RedisDataType type = entry.getValue();
      Region<?, ?> newRegion = cache.getRegion(regionName);
      if (newRegion == null && type != RedisDataType.REDIS_STRING && type != RedisDataType.REDIS_HLL
          && type != RedisDataType.REDIS_PROTECTED) {
        try {
          regionCache
              .createRemoteRegionReferenceLocally(Coder.stringToByteArrayWrapper(regionName), type);
        } catch (Exception e) {
          if (logger.errorEnabled()) {
            logger.error(e);
          }
        }
      }
    }
  }

  /**
   * Helper method to start the server listening for connections. The server is bound to the port
   * specified by {@link GeodeRedisServer#serverPort}
   */
  private void startRedisServer() throws IOException, InterruptedException {
    ThreadFactory selectorThreadFactory = new ThreadFactory() {
      private final AtomicInteger counter = new AtomicInteger();

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName("GeodeRedisServer-SelectorThread-" + counter.incrementAndGet());
        t.setDaemon(true);
        return t;
      }
    };

    ThreadFactory workerThreadFactory = new ThreadFactory() {
      private final AtomicInteger counter = new AtomicInteger();

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName("GeodeRedisServer-WorkerThread-" + counter.incrementAndGet());
        return t;
      }
    };

    bossGroup = null;
    workerGroup = null;
    Class<? extends ServerChannel> socketClass;
    if (singleThreadPerConnection) {
      socketClass =
          startRedisServiceSingleThreadPerConnection(selectorThreadFactory, workerThreadFactory);
    } else {
      bossGroup = new NioEventLoopGroup(numSelectorThreads, selectorThreadFactory);
      workerGroup = new NioEventLoopGroup(numWorkerThreads, workerThreadFactory);
      socketClass = NioServerSocketChannel.class;
    }
    InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();
    String pwd = system.getConfig().getRedisPassword();
    final byte[] pwdB = Coder.stringToBytes(pwd);
    ServerBootstrap b = new ServerBootstrap();

    b.group(bossGroup, workerGroup).channel(socketClass)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) {
            if (logger.fineEnabled()) {
              logger.fine("GeodeRedisServer-Connection established with " + ch.remoteAddress());
            }
            ChannelPipeline p = ch.pipeline();
            addSSLIfEnabled(ch, p);
            p.addLast(ByteToCommandDecoder.class.getSimpleName(), new ByteToCommandDecoder());
            p.addLast(ExecutionHandlerContext.class.getSimpleName(),
                new ExecutionHandlerContext(ch, cache, regionCache, GeodeRedisServer.this, pwdB,
                    keyRegistrar, pubSub, hashLockService));
          }
        }).option(ChannelOption.SO_REUSEADDR, true).option(ChannelOption.SO_RCVBUF, getBufferSize())
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, GeodeRedisServer.connectTimeoutMillis)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    // Bind and start to accept incoming connections.
    ChannelFuture f = b.bind(new InetSocketAddress(getBindAddress(), serverPort)).sync();
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
    serverChannel = f.channel();
  }

  @SuppressWarnings("deprecation")
  private Class<? extends ServerChannel> startRedisServiceSingleThreadPerConnection(
      ThreadFactory selectorThreadFactory, ThreadFactory workerThreadFactory) {
    bossGroup =
        new io.netty.channel.oio.OioEventLoopGroup(Integer.MAX_VALUE, selectorThreadFactory);
    workerGroup =
        new io.netty.channel.oio.OioEventLoopGroup(Integer.MAX_VALUE, workerThreadFactory);
    return io.netty.channel.socket.oio.OioServerSocketChannel.class;
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
  private void afterKeyCreate(EntryEvent<String, RedisDataType> event) {
    if (event.isOriginRemote()) {
      final String key = event.getKey();
      final RedisDataType value = event.getNewValue();
      if (value != RedisDataType.REDIS_STRING && value != RedisDataType.REDIS_HLL
          && value != RedisDataType.REDIS_PROTECTED) {
        try {
          regionCache.createRemoteRegionReferenceLocally(Coder.stringToByteArrayWrapper(key),
              value);
        } catch (RegionDestroyedException ignore) { // Region already destroyed, ignore
        }
      }
    }
  }

  /**
   * When a key is removed then this function will make sure the associated queries with the key are
   * also removed from each vm to avoid unnecessary data retention
   */
  private void afterKeyDestroy(EntryEvent<String, RedisDataType> event) {
    if (event.isOriginRemote()) {
      final String key = event.getKey();
      final RedisDataType value = event.getOldValue();
      if (value != null && value != RedisDataType.REDIS_STRING && value != RedisDataType.REDIS_HLL
          && value != RedisDataType.REDIS_PROTECTED) {
        ByteArrayWrapper kW = Coder.stringToByteArrayWrapper(key);
        Region<?, ?> r = regionCache.getRegion(kW);
        if (r != null) {
          regionCache.removeRegionReferenceLocally(kW, value);
        }
      }
    }
  }

  private class MetaCacheListener extends CacheListenerAdapter<String, RedisDataType> {
    @Override
    public void afterCreate(EntryEvent<String, RedisDataType> event) {
      afterKeyCreate(event);
    }

    @Override
    public void afterDestroy(EntryEvent<String, RedisDataType> event) {
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
      regionCache.close();
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
