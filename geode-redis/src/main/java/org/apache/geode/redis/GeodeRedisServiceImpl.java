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
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.oio.OioServerSocketChannel;
import io.netty.util.concurrent.Future;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ByteToCommandDecoder;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.hll.HyperLogLogPlus;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The GeodeRedisServiceImpl is a server that understands the Redis protocol. As commands are sent
 * to the server, each command is picked up by a thread, interpreted and then executed and a
 * response is sent back to the client. The default connection port is 6379 but that can be altered
 * when run through GFSH or started through the provided static main class.
 * <p>
 * Each Redis data type instance is stored in a separate {@link Region} except for the Strings and
 * HyperLogLogs which are collectively stored in one Region respectively. That Region along with a
 * meta data region used internally are protected so the client may not store keys with the name
 * {@link GeodeRedisServiceImpl#REDIS_META_DATA_REGION} or
 * {@link GeodeRedisServiceImpl#STRING_REGION}. The default Region type is
 * {@link RegionShortcut#PARTITION} although this can be changed by specifying the SystemProperty
 * {@value #DEFAULT_REGION_SYS_PROP_NAME} to a type defined by {@link RegionShortcut}. If the
 * {@link GeodeRedisServiceImpl#NUM_THREADS_SYS_PROP_NAME} system property is set to 0, one thread
 * per client will be created. Otherwise a worker thread pool of specified size is used or a default
 * size of 4 * {@link Runtime#availableProcessors()} if the property is not set.
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
 * <P>
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

public class GeodeRedisServiceImpl implements GeodeRedisService {

  /**
   * The default Redis port as specified by their protocol, {@value #DEFAULT_REDIS_SERVER_PORT}
   */
  public static final int DEFAULT_REDIS_SERVER_PORT = 6379;

  /**
   * The number of threads that will work on handling requests
   */
  private int numWorkerThreads;

  /**
   * The number of threads that will work socket selectors
   */
  private final int numSelectorThreads = 1;
  private final int numExpirationThreads = 1;

  /**
   * The actual port being used by the server
   */
  private int redisPort;

  /**
   * The address to bind to
   */
  private String redisBindAddress;

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
  private Cache cache;

  /**
   * Channel to be closed when shutting down
   */
  private Channel serverChannel;

  /**
   * Gem logwriter
   */
  private Logger logger = null;

  private RegionProvider regionProvider;
  private MetaCacheListener metaListener;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ScheduledExecutorService expirationExecutor;

  /**
   * Map of futures to be executed for key expirations
   */
  private ConcurrentMap<ByteArrayWrapper, ScheduledFuture<?>> expirationFutures;


  /**
   * The field that defines the name of the {@link Region} which holds all of the strings. The
   * current value of this field is {@value #STRING_REGION}.
   */
  public static final String STRING_REGION = "ReDiS_StRiNgS";

  /**
   * The field that defines the name of the {@link Region} which holds all of the HyperLogLogs. The
   * current value of this field is {@value #HLL_REGION}.
   */
  public static final String HLL_REGION = "ReDiS_HlL";

  /**
   * The field that defines the name of the {@link Region} which holds all of the Redis meta data.
   * The current value of this field is {@value #REDIS_META_DATA_REGION}.
   */
  public static final String REDIS_META_DATA_REGION = "__ReDiS_MeTa_DaTa";

  /**
   * The system property name used to set the default {@link Region} creation type. The property
   * name is {@value #DEFAULT_REGION_SYS_PROP_NAME} and the acceptable values are types defined by
   * {@link RegionShortcut}, i.e. "PARTITION" would be used for {@link RegionShortcut#PARTITION}.
   */
  public static final String DEFAULT_REGION_SYS_PROP_NAME = "gemfireredis.regiontype";

  /**
   * System property name that can be used to set the number of threads to be used by the
   * GeodeRedisServiceImpl
   */
  public static final String NUM_THREADS_SYS_PROP_NAME = "gemfireredis.numthreads";

  /**
   * The actual {@link RegionShortcut} type specified by the system property
   * {@value #DEFAULT_REGION_SYS_PROP_NAME}.
   */
  private RegionShortcut DEFAULT_REGION_TYPE;

  private boolean shutdown = false;
  private boolean started = false;

  /**
   * Determine the {@link RegionShortcut} type from a String value. If the String value doesn't map
   * to a RegionShortcut type then {@link RegionShortcut#PARTITION} will be used by default.
   * 
   * @return {@link RegionShortcut}
   */
  private RegionShortcut setRegionType() {
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

  public GeodeRedisServiceImpl() {}

  /**
   * Helper method to get the host name to bind to
   * 
   * @return The InetAddress to bind to
   */
  private InetAddress getRedisBindAddress() throws UnknownHostException {
    return this.redisBindAddress == null || this.redisBindAddress.isEmpty()
        ? SocketCreator.getLocalHost() : InetAddress.getByName(this.redisBindAddress);
  }

  /**
   * This is function to call on a {@link GeodeRedisServiceImpl} instance to start it running
   */
  @Override
  public synchronized void start() {
    if (!started) {
      try {
        initializeRedis();
        startRedisServer();
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException("Could not start Server", e);
      }
      started = true;
    }
  }

  private void initializeRedis() {
    initializeRedisServiceInternals();
    Region<ByteArrayWrapper, ByteArrayWrapper> stringsRegion;

    Region<ByteArrayWrapper, HyperLogLogPlus> hLLRegion;
    Region<String, RedisDataType> redisMetaData;
    GemFireCacheImpl gemFireCache = (GemFireCacheImpl) cache;
    try {
      if ((stringsRegion = cache.getRegion(STRING_REGION)) == null) {
        RegionFactory<ByteArrayWrapper, ByteArrayWrapper> regionFactory =
            gemFireCache.createRegionFactory(this.DEFAULT_REGION_TYPE);
        stringsRegion = regionFactory.create(STRING_REGION);
      }
      if ((hLLRegion = cache.getRegion(HLL_REGION)) == null) {
        RegionFactory<ByteArrayWrapper, HyperLogLogPlus> regionFactory =
            gemFireCache.createRegionFactory(this.DEFAULT_REGION_TYPE);
        hLLRegion = regionFactory.create(HLL_REGION);
      }
      if ((redisMetaData = cache.getRegion(REDIS_META_DATA_REGION)) == null) {
        AttributesFactory af = new AttributesFactory();
        af.addCacheListener(metaListener);
        af.setDataPolicy(DataPolicy.REPLICATE);
        InternalRegionArguments ira =
            new InternalRegionArguments().setInternalRegion(true).setIsUsedForMetaRegion(true);
        redisMetaData = gemFireCache.createVMRegion(REDIS_META_DATA_REGION, af.create(), ira);
      }
    } catch (IOException | ClassNotFoundException e) {
      // only if loading snapshot, not here
      InternalGemFireError assErr = new InternalGemFireError(
          LocalizedStrings.GemFireCache_UNEXPECTED_EXCEPTION.toLocalizedString());
      assErr.initCause(e);
      throw assErr;
    }
    this.regionProvider = new RegionProvider(stringsRegion, hLLRegion, redisMetaData,
        expirationFutures, expirationExecutor, this.DEFAULT_REGION_TYPE);
    redisMetaData.put(REDIS_META_DATA_REGION, RedisDataType.REDIS_PROTECTED);
    redisMetaData.put(HLL_REGION, RedisDataType.REDIS_PROTECTED);
    redisMetaData.put(STRING_REGION, RedisDataType.REDIS_PROTECTED);
    checkForRegions();
  }

  private void initializeRedisServiceInternals() {
    this.DEFAULT_REGION_TYPE = setRegionType();
    this.numWorkerThreads = setNumWorkerThreads();
    if (this.numWorkerThreads == 0) {
      this.singleThreadPerConnection = true;
    }
    this.metaListener = new MetaCacheListener();
    this.expirationFutures = new ConcurrentHashMap<>();
    this.expirationExecutor =
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
  }

  private void checkForRegions() {
    Collection<Entry<String, RedisDataType>> entrySet = this.regionProvider.metaEntrySet();
    for (Entry<String, RedisDataType> entry : entrySet) {
      String regionName = entry.getKey();
      RedisDataType type = entry.getValue();
      Region<?, ?> newRegion = cache.getRegion(regionName);
      if (newRegion == null && type != RedisDataType.REDIS_STRING && type != RedisDataType.REDIS_HLL
          && type != RedisDataType.REDIS_PROTECTED) {
        try {
          this.regionProvider
              .createRemoteRegionReferenceLocally(Coder.stringToByteArrayWrapper(regionName), type);
        } catch (Exception e) {
          if (logger.isErrorEnabled()) {
            logger.error(e);
          }
        }
      }
    }
  }

  /**
   * Helper method to start the server listening for connections. The server is bound to the port
   * specified by {@link GeodeRedisServiceImpl#redisPort}
   */
  private void startRedisServer() throws IOException, InterruptedException {
    ThreadFactory selectorThreadFactory = new ThreadFactory() {
      private final AtomicInteger counter = new AtomicInteger();

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName("GeodeRedisServiceImpl-SelectorThread-" + counter.incrementAndGet());
        t.setDaemon(true);
        return t;
      }

    };

    ThreadFactory workerThreadFactory = new ThreadFactory() {
      private final AtomicInteger counter = new AtomicInteger();

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName("GeodeRedisServiceImpl-WorkerThread-" + counter.incrementAndGet());
        return t;
      }

    };

    bossGroup = null;
    workerGroup = null;
    Class<? extends ServerChannel> socketClass = null;
    if (singleThreadPerConnection) {
      bossGroup = new OioEventLoopGroup(Integer.MAX_VALUE, selectorThreadFactory);
      workerGroup = new OioEventLoopGroup(Integer.MAX_VALUE, workerThreadFactory);
      socketClass = OioServerSocketChannel.class;
    } else {
      bossGroup = new NioEventLoopGroup(this.numSelectorThreads, selectorThreadFactory);
      workerGroup = new NioEventLoopGroup(this.numWorkerThreads, workerThreadFactory);
      socketClass = NioServerSocketChannel.class;
    }
    InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();
    String pwd = system.getConfig().getRedisPassword();
    final byte[] pwdB = Coder.stringToBytes(pwd);
    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup).channel(socketClass)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
            if (logger.isDebugEnabled()) {
              logger
                  .debug("GeodeRedisServiceImpl-Connection established with " + ch.remoteAddress());
            }
            ChannelPipeline p = ch.pipeline();
            p.addLast(ByteToCommandDecoder.class.getSimpleName(), new ByteToCommandDecoder());
            p.addLast(ExecutionHandlerContext.class.getSimpleName(), new ExecutionHandlerContext(ch,
                cache, regionProvider, GeodeRedisServiceImpl.this, pwdB));
          }
        }).option(ChannelOption.SO_REUSEADDR, true).option(ChannelOption.SO_RCVBUF, getBufferSize())
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS,
            GeodeRedisServiceImpl.connectTimeoutMillis)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    // Bind and start to accept incoming connections.
    ChannelFuture f = b.bind(new InetSocketAddress(getRedisBindAddress(), redisPort)).sync();
    if (this.logger.isInfoEnabled()) {
      String logMessage = "GeodeRedisServiceImpl started {" + getRedisBindAddress() + ":"
          + redisPort + "}, Selector threads: " + this.numSelectorThreads;
      if (this.singleThreadPerConnection) {
        logMessage += ", One worker thread per connection";
      } else {
        logMessage += ", Worker threads: " + this.numWorkerThreads;
      }
      this.logger.info(logMessage);
    }
    this.serverChannel = f.channel();
  }

  /**
   * Takes an entry event and processes it. If the entry denotes that a
   * {@link RedisDataType#REDIS_LIST} or {@link RedisDataType#REDIS_SORTEDSET} was created then this
   * function will call the necessary calls to create the parameterized queries for those keys.
   * 
   * @param event EntryEvent from meta data region
   */
  private void afterKeyCreate(EntryEvent<String, RedisDataType> event) {
    if (event.isOriginRemote()) {
      final String key = (String) event.getKey();
      final RedisDataType value = event.getNewValue();
      if (value != RedisDataType.REDIS_STRING && value != RedisDataType.REDIS_HLL
          && value != RedisDataType.REDIS_PROTECTED) {
        try {
          this.regionProvider
              .createRemoteRegionReferenceLocally(Coder.stringToByteArrayWrapper(key), value);
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
      final String key = (String) event.getKey();
      final RedisDataType value = event.getOldValue();
      if (value != null && value != RedisDataType.REDIS_STRING && value != RedisDataType.REDIS_HLL
          && value != RedisDataType.REDIS_PROTECTED) {
        ByteArrayWrapper kW = Coder.stringToByteArrayWrapper(key);
        Region<?, ?> r = this.regionProvider.getRegion(kW);
        if (r != null) {
          this.regionProvider.removeRegionReferenceLocally(kW, value);
        }
      }
    }
  }

  @Override
  public void init(Cache cache) {
    this.cache = cache;
    logger = LogService.getLogger();

    InternalDistributedSystem internalDistributedSystem = ((GemFireCacheImpl) cache).getSystem();
    DistributionConfig internalDistributedSystemConfig = internalDistributedSystem.getConfig();
    this.redisPort = internalDistributedSystemConfig.getRedisPort();
    if (redisPort <= 0) { // unset
      this.redisPort = DEFAULT_REDIS_SERVER_PORT;
    }
    this.redisBindAddress = internalDistributedSystemConfig.getRedisBindAddress();
  }

  @Override
  public Class<? extends CacheService> getInterface() {
    return GeodeRedisService.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    // TODO This needs to be implemented in the GEODE-2449
    throw new RuntimeException("This still needs to be implemented");
  }

  private final class MetaCacheListener extends CacheListenerAdapter<String, RedisDataType> {

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
   * Shutdown method for {@link GeodeRedisServiceImpl}. This closes the {@link Cache}, interrupts
   * all execution and forcefully closes all connections.
   */
  @Override
  public void stop() {
    if (!shutdown) {
      if (logger.isInfoEnabled()) {
        logger.info("GeodeRedisServiceImpl shutting down");
      }

      Future<?> workerGroupFuture = workerGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      Future<?> bossGroupFuture = bossGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      ChannelFuture closeFuture = this.serverChannel.close();

      // TODO we need to investigate how the shutdown should work.
      // We are likely brought here by a channel read reading a shutdown message, in which case
      // calling await or sync can cause a deadlock.
      // workerGroupFuture.syncUninterruptibly();
      // bossGroupFuture.syncUninterruptibly();
      this.regionProvider.close();
      for (ScheduledFuture<?> f : this.expirationFutures.values()) {
        f.cancel(true);
      }
      this.expirationFutures.clear();
      this.expirationExecutor.shutdownNow();
      // closeFuture.syncUninterruptibly();
      shutdown = true;
    }
  }
}
