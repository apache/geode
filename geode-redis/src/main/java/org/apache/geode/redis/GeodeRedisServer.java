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

import org.apache.geode.GemFireCacheException;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.GemFireCacheImpl;

import java.util.Properties;

/**
 * The GeodeRedisServer is a server that understands the Redis protocol. As commands are sent to the
 * server, each command is picked up by a thread, interpreted and then executed and a response is
 * sent back to the client. The default connection port is 6379 but that can be altered when run
 * through GFSH or started through the provided static main class.
 * <p>
 * Each Redis data type instance is stored in a separate {@link Region} except for the Strings and
 * HyperLogLogs which are collectively stored in one Region respectively. That Region along with a
 * meta data region used internally are protected so the client may not store keys with the name
 * {@link GeodeRedisServiceImpl#REDIS_META_DATA_REGION} or
 * {@link GeodeRedisServiceImpl#STRING_REGION}. The default Region type is
 * {@link RegionShortcut#PARTITION} although this can be changed by specifying the SystemProperty
 * {@value GeodeRedisServiceImpl#DEFAULT_REGION_SYS_PROP_NAME} to a type defined by
 * {@link RegionShortcut}. If the {@link GeodeRedisServiceImpl#NUM_THREADS_SYS_PROP_NAME} system
 * property is set to 0, one thread per client will be created. Otherwise a worker thread pool of
 * specified size is used or a default size of 4 * {@link Runtime#availableProcessors()} if the
 * property is not set.
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
 *
 * @deprecated as of Geode 1.2.0
 */
public class GeodeRedisServer {
  private Properties properties;
  private boolean started = false;
  private GeodeRedisService redisService;

  /**
   * Constructor for {@link GeodeRedisServer} that will start the server on the given port and bind
   * to the first non-loopback address
   *
   * @param port The port the server will bind to, will use
   *        {@link GeodeRedisService#DEFAULT_REDIS_SERVER_PORT} by default
   */
  public GeodeRedisServer(int port) {
    this(null, port, null);
  }

  /**
   * Constructor for {@link GeodeRedisServer} that will start the server and bind to the given
   * address and port
   *
   * @param bindAddress The address to which the server will attempt to bind to
   * @param port The port the server will bind to, will use
   *        {@link GeodeRedisService#DEFAULT_REDIS_SERVER_PORT} by default if argument is less than
   *        or equal to 0
   */
  public GeodeRedisServer(String bindAddress, int port) {
    this(bindAddress, port, null);
  }


  /**
   * Constructor for {@link GeodeRedisServer} that will start the server and bind to the given
   * address and port. Keep in mind that the log level configuration will only be set if a
   * {@link Cache} does not already exist, if one already exists then setting that property will
   * have no effect.
   *
   * @param bindAddress The address to which the server will attempt to bind to
   * @param port The port the server will bind to, will use
   *        {@link GeodeRedisService#DEFAULT_REDIS_SERVER_PORT} default if argument is less than or
   *        equal to 0
   * @param logLevel The logging level to be used by GemFire
   */
  public GeodeRedisServer(String bindAddress, int port, String logLevel) {
    this.properties = new Properties();
    if (bindAddress != null) {
      this.properties.setProperty(ConfigurationProperties.REDIS_BIND_ADDRESS, bindAddress);
    }
    this.properties.setProperty(ConfigurationProperties.REDIS_PORT, String.valueOf(port));
    if (logLevel != null) {
      this.properties.setProperty(ConfigurationProperties.LOG_LEVEL, logLevel);
    }
  }

  /**
   * This is function to call on a {@link GeodeRedisServer} instance to start it running
   */
  public synchronized void start() {
    if (!started) {
      GemFireCacheImpl cache = startGemFire(properties);
      redisService = cache.getService(GeodeRedisService.class);
      redisService.start();
      started = true;
    }
  }

  /**
   * Initializes the {@link Cache}, and creates Redis necessities Region and protects declares that
   * {@link Region} to be protected. Also, every {@link GeodeRedisServer} will check for entries
   * already in the meta data Region.
   */
  private synchronized GemFireCacheImpl startGemFire(final Properties properties) {
    Cache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      throw new CacheExistsException(cache,
          "Cache must not be running to use the deprecated GeodeRedisServer class. Consider using the GeodeRedisService as part of a cache.");
    } else {
      CacheFactory cacheFactory = new CacheFactory(properties);
      cache = cacheFactory.create();
    }
    return (GemFireCacheImpl) cache;
  }

  /**
   * Shutdown method for {@link GeodeRedisServer}. This closes the {@link Cache}, interrupts all
   * execution and forcefully closes all connections.
   */
  public synchronized void shutdown() {
    redisService.stop();
  }
}
