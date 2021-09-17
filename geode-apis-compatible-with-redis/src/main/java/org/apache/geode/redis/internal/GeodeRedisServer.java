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

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsClockFactory;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.cluster.RedisMemberInfo;
import org.apache.geode.redis.internal.cluster.RedisMemberInfoRetrievalFunction;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.NettyRedisServer;
import org.apache.geode.redis.internal.pubsub.PubSub;
import org.apache.geode.redis.internal.pubsub.PubSubImpl;
import org.apache.geode.redis.internal.pubsub.Subscriptions;
import org.apache.geode.redis.internal.services.StripedCoordinator;
import org.apache.geode.redis.internal.services.SynchronizedStripedCoordinator;
import org.apache.geode.redis.internal.statistics.GeodeRedisStats;
import org.apache.geode.redis.internal.statistics.RedisStats;

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
  public static final String ENABLE_UNSUPPORTED_COMMANDS_PARAM = "enable-unsupported-commands";
  private static boolean unsupportedCommandsEnabled;
  private static final Logger logger = LogService.getLogger();
  private final PassiveExpirationManager passiveExpirationManager;
  private final NettyRedisServer nettyRedisServer;
  private final RegionProvider regionProvider;
  private final PubSub pubSub;
  private final RedisStats redisStats;
  private boolean shutdown;

  /**
   * Constructor for {@code GeodeRedisServer} that will configure the server to bind to the given
   * address and port.
   *
   * @param bindAddress The address to which the server will attempt to bind to; null
   *        causes it to bind to all local addresses.
   * @param port The port the server will bind to, will throw an IllegalArgumentException if
   *        argument is less than 0. If the port is 0 a random port is assigned.
   */
  public GeodeRedisServer(String bindAddress, int port, InternalCache cache) {

    unsupportedCommandsEnabled = Boolean.getBoolean(ENABLE_UNSUPPORTED_COMMANDS_PARAM);

    redisStats = createStats(cache);
    StripedCoordinator stripedCoordinator = new SynchronizedStripedCoordinator();
    RedisMemberInfoRetrievalFunction infoFunction = RedisMemberInfoRetrievalFunction.register();

    regionProvider = new RegionProvider(cache, stripedCoordinator, redisStats);
    pubSub = new PubSubImpl(new Subscriptions(), regionProvider);

    passiveExpirationManager = new PassiveExpirationManager(regionProvider);

    DistributedMember member = cache.getDistributedSystem().getDistributedMember();
    SecurityService securityService = cache.getSecurityService();

    nettyRedisServer = new NettyRedisServer(() -> cache.getInternalDistributedSystem().getConfig(),
        regionProvider, pubSub,
        this::allowUnsupportedCommands, this::shutdown, port, bindAddress, redisStats,
        member, securityService);

    infoFunction.initialize(member, bindAddress, nettyRedisServer.getPort());
  }

  @VisibleForTesting
  public int getSubscriptionCount() {
    return ((PubSubImpl) pubSub).getSubscriptionCount();
  }

  private static RedisStats createStats(InternalCache cache) {
    InternalDistributedSystem system = cache.getInternalDistributedSystem();
    StatisticsClock statisticsClock =
        StatisticsClockFactory.clock(true);

    return new RedisStats(statisticsClock,
        new GeodeRedisStats(system.getStatisticsManager(),
            "redisStats",
            statisticsClock));
  }

  @VisibleForTesting
  public RedisStats getStats() {
    return redisStats;
  }

  @VisibleForTesting
  public void setAllowUnsupportedCommands(boolean allowUnsupportedCommands) {
    unsupportedCommandsEnabled = allowUnsupportedCommands;
  }

  public boolean allowUnsupportedCommands() {
    return unsupportedCommandsEnabled;
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
      pubSub.close();
      passiveExpirationManager.stop();
      nettyRedisServer.stop();
      redisStats.close();
      shutdown = true;
    }
  }

  @VisibleForTesting
  public Long getDataStoreBytesInUseForDataRegion() {
    PartitionedRegion dataRegion = (PartitionedRegion) this.getRegionProvider().getDataRegion();
    return dataRegion.getPrStats().getDataStoreBytesInUse();
  }

  @VisibleForTesting
  public RedisMemberInfo getMemberInfo(String key) throws InterruptedException {
    return regionProvider.getSlotAdvisor().getMemberInfo(new RedisKey(Coder.stringToBytes(key)));
  }
}
