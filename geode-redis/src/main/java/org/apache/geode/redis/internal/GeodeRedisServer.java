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

import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsClockFactory;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.executor.CommandFunction;
import org.apache.geode.redis.internal.executor.StripedExecutor;
import org.apache.geode.redis.internal.executor.SynchronizedStripedExecutor;
import org.apache.geode.redis.internal.executor.key.RenameFunction;
import org.apache.geode.redis.internal.gfsh.RedisCommandFunction;
import org.apache.geode.redis.internal.netty.NettyRedisServer;
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
  public static final String ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM =
      "enable-redis-unsupported-commands";

  private static final Logger logger = LogService.getLogger();
  private final boolean ENABLE_REDIS_UNSUPPORTED_COMMANDS =
      Boolean.getBoolean(ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM);

  private final PassiveExpirationManager passiveExpirationManager;
  private final NettyRedisServer nettyRedisServer;
  private final RegionProvider regionProvider;
  private final PubSub pubSub;
  private final RedisStats redisStats;
  private final ExecutorService redisCommandExecutor;
  private boolean shutdown;

  /**
   * Constructor for {@code GeodeRedisServer} that will configure the server to bind to the given
   * address and port.
   *
   * @param bindAddress The address to which the server will attempt to bind to; null causes it to
   *        bind to all local addresses.
   * @param port The port the server will bind to, will throw an IllegalArgumentException if
   *        argument is less than 0. If the port is 0 a random port is assigned.
   */
  public GeodeRedisServer(String bindAddress, int port, InternalCache cache) {
    if (ENABLE_REDIS_UNSUPPORTED_COMMANDS) {
      logUnsupportedCommandWarning();
    }

    pubSub = new PubSubImpl(new Subscriptions());
    redisStats = createStats(cache);
    StripedExecutor stripedExecutor = new SynchronizedStripedExecutor();
    regionProvider = new RegionProvider(cache);

    CommandFunction.register(regionProvider.getDataRegion(), stripedExecutor, redisStats);
    RenameFunction.register(regionProvider.getDataRegion(), stripedExecutor, redisStats);
    RedisCommandFunction.register();

    passiveExpirationManager =
        new PassiveExpirationManager(regionProvider.getDataRegion(), redisStats);
    redisCommandExecutor =
        LoggingExecutors.newCachedThreadPool("Redis Command", true);
    nettyRedisServer =
        new NettyRedisServer(() -> cache.getInternalDistributedSystem().getConfig(),
            regionProvider,
            pubSub,
            this::allowUnsupportedCommands,
            this::shutdown,
            port,
            bindAddress,
            redisStats,
            redisCommandExecutor);
  }

  @VisibleForTesting
  public int getSubscriptionCount() {
    return ((PubSubImpl) pubSub).getSubscriptionCount();
  }

  public void setAllowUnsupportedCommands(boolean allowUnsupportedCommands) {
    Region<String, Object> configRegion = regionProvider.getConfigRegion();
    configRegion.put(GeodeRedisServer.ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM,
        allowUnsupportedCommands);
    if (allowUnsupportedCommands) {
      logUnsupportedCommandWarning();
    }
  }

  private static RedisStats createStats(InternalCache cache) {
    InternalDistributedSystem system = cache.getInternalDistributedSystem();
    StatisticsClock statisticsClock =
        StatisticsClockFactory.clock(system.getConfig().getEnableTimeStatistics());
    return new RedisStats(system.getStatisticsManager(), statisticsClock);
  }


  @VisibleForTesting
  RedisStats getStats() {
    return redisStats;
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

  public RegionProvider getRegionProvider() {
    return regionProvider;
  }

  public int getPort() {
    return nettyRedisServer.getPort();
  }

  /**
   * Shutdown method for {@code GeodeRedisServer}. This closes the {@link Cache}, interrupts all
   * execution and forcefully closes all connections.
   */
  public synchronized void shutdown() {
    if (!shutdown) {
      logger.info("GeodeRedisServer shutting down");
      passiveExpirationManager.stop();
      nettyRedisServer.stop();
      redisCommandExecutor.shutdown();
      redisStats.close();
      shutdown = true;
    }
  }
}
