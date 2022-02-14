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

package org.apache.geode.redis.internal.statistics;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;
import org.apache.geode.redis.internal.commands.RedisCommandType;

public class GeodeRedisStats {

  public static final String STATS_BASENAME = "GeodeForRedisStats";
  private static final String GENERAL_CATEGORY = "General";

  @Immutable
  private static final Map<String, StatisticsType> statisticTypes = new HashMap<>();
  @Immutable
  private static final EnumMap<RedisCommandType, Integer> completedCommandStatIds =
      new EnumMap<>(RedisCommandType.class);
  @Immutable
  private static final EnumMap<RedisCommandType, Integer> timeCommandStatIds =
      new EnumMap<>(RedisCommandType.class);

  private static final int currentlyConnectedClientsId;
  private static final int activeExpirationChecksId;
  private static final int activeExpirationCheckTimeId;
  private static final int activeExpirationsId;
  private static final int expirationsId;
  private static final int expirationTimeId;
  private static final int totalConnectionsReceivedId;
  private static final int commandsProcessedId;
  private static final int keyspaceHitsId;
  private static final int keyspaceMissesId;
  private static final int totalNetworkBytesReadId;
  private static final int publishRequestsCompletedId;
  private static final int publishRequestsInProgressId;
  private static final int publishRequestTimeId;
  private static final int subscribersId;
  private static final int uniqueChannelSubscriptionsId;
  private static final int uniquePatternSubscriptionsId;
  private final Statistics generalStats;
  private final Map<String, Statistics> statistics = new HashMap<>();
  private final StatisticsClock clock;

  public GeodeRedisStats(StatisticsFactory factory, StatisticsClock clock) {
    this.clock = clock;
    generalStats =
        factory.createAtomicStatistics(statisticTypes.get(GENERAL_CATEGORY), STATS_BASENAME);
    statistics.put(GENERAL_CATEGORY, generalStats);

    for (RedisCommandType.Category category : RedisCommandType.Category.values()) {
      String statName = STATS_BASENAME + ":" + category.name();
      Statistics stats =
          factory.createAtomicStatistics(statisticTypes.get(category.name()), statName);
      statistics.put(category.name(), stats);
    }
  }

  static {
    StatisticsTypeFactory statisticsTypeFactory = StatisticsTypeFactoryImpl.singleton();

    StatisticsType generalType = statisticsTypeFactory
        .createType(STATS_BASENAME,
            "Statistics for a geode-for-redis server",
            createGeneralStatisticDescriptors(statisticsTypeFactory));

    currentlyConnectedClientsId = generalType.nameToId("connectedClients");
    activeExpirationChecksId = generalType.nameToId("activeExpirationChecks");
    activeExpirationCheckTimeId = generalType.nameToId("activeExpirationCheckTime");
    activeExpirationsId = generalType.nameToId("activeExpirations");
    expirationsId = generalType.nameToId("expirations");
    expirationTimeId = generalType.nameToId("expirationTime");
    totalConnectionsReceivedId = generalType.nameToId("totalConnectionsReceived");
    commandsProcessedId = generalType.nameToId("commandsProcessed");
    totalNetworkBytesReadId = generalType.nameToId("totalNetworkBytesRead");
    keyspaceHitsId = generalType.nameToId("keyspaceHits");
    keyspaceMissesId = generalType.nameToId("keyspaceMisses");
    publishRequestsCompletedId = generalType.nameToId("publishRequestsCompleted");
    publishRequestsInProgressId = generalType.nameToId("publishRequestsInProgress");
    publishRequestTimeId = generalType.nameToId("publishRequestTime");
    subscribersId = generalType.nameToId("subscribers");
    uniqueChannelSubscriptionsId = generalType.nameToId("uniqueChannelSubscriptions");
    uniquePatternSubscriptionsId = generalType.nameToId("uniquePatternSubscriptions");

    statisticTypes.put(GENERAL_CATEGORY, generalType);

    for (RedisCommandType.Category category : RedisCommandType.Category.values()) {
      StatisticsType type = statisticsTypeFactory
          .createType(STATS_BASENAME + ":" + category.name(),
              category.name() + " statistics for a geode-for-redis server",
              createCategoryStatisticDescriptors(statisticsTypeFactory, category));
      statisticTypes.put(category.name(), type);

      fillCompletedIdMap(category);
      fillTimeIdMap(category);
    }
  }

  private long getCurrentTimeNanos() {
    return clock.getTime();
  }

  public void endCommand(RedisCommandType command, long start) {
    Statistics stat = statistics.get(command.category().name());
    if (clock.isEnabled()) {
      stat.incLong(timeCommandStatIds.get(command), getCurrentTimeNanos() - start);
    }
    stat.incLong(completedCommandStatIds.get(command), 1);
  }

  public void addClient() {
    generalStats.incLong(currentlyConnectedClientsId, 1);
    generalStats.incLong(totalConnectionsReceivedId, 1);
  }

  public void removeClient() {
    generalStats.incLong(currentlyConnectedClientsId, -1);
  }

  public void endActiveExpirationCheck(long start, long expireCount) {
    if (expireCount > 0) {
      incActiveExpirations(expireCount);
    }
    if (clock.isEnabled()) {
      generalStats.incLong(activeExpirationCheckTimeId, getCurrentTimeNanos() - start);
    }
    generalStats.incLong(activeExpirationChecksId, 1);
  }

  private void incActiveExpirations(long count) {
    generalStats.incLong(activeExpirationsId, count);
  }

  public void endExpiration(long start) {
    if (clock.isEnabled()) {
      generalStats.incLong(expirationTimeId, getCurrentTimeNanos() - start);
    }
    generalStats.incLong(expirationsId, 1);
  }

  public void incrementCommandsProcessed() {
    generalStats.incLong(commandsProcessedId, 1);
  }

  public void incrementTotalNetworkBytesRead(long bytes) {
    generalStats.incLong(totalNetworkBytesReadId, bytes);
  }

  public void incrementKeyspaceHits() {
    generalStats.incLong(keyspaceHitsId, 1);
  }

  public void incrementKeyspaceMisses() {
    generalStats.incLong(keyspaceMissesId, 1);
  }

  public long startPublish() {
    generalStats.incLong(publishRequestsInProgressId, 1);
    return getCurrentTimeNanos();
  }

  public void endPublish(long publishCount, long time) {
    generalStats.incLong(publishRequestsInProgressId, -publishCount);
    generalStats.incLong(publishRequestsCompletedId, publishCount);
    if (clock.isEnabled()) {
      generalStats.incLong(publishRequestTimeId, time);
    }
  }

  public void changeSubscribers(long delta) {
    generalStats.incLong(subscribersId, delta);
  }

  public void changeUniqueChannelSubscriptions(long delta) {
    generalStats.incLong(uniqueChannelSubscriptionsId, delta);
  }

  public void changeUniquePatternSubscriptions(long delta) {
    generalStats.incLong(uniquePatternSubscriptionsId, delta);
  }

  public void close() {
    if (generalStats != null) {
      generalStats.close();
    }
  }

  private static StatisticDescriptor[] createCategoryStatisticDescriptors(
      StatisticsTypeFactory factory,
      RedisCommandType.Category category) {
    ArrayList<StatisticDescriptor> descriptors = new ArrayList<>();

    for (RedisCommandType command : RedisCommandType.getCommandsForCategory(category)) {
      String name = command.name().toLowerCase();
      String statCompletedName = name + "Completed";
      String statCompletedDescription = "Total number of redis '" + name
          + "' operations that have completed execution on this server.";

      descriptors.add(
          factory.createLongCounter(statCompletedName, statCompletedDescription, "operations"));

      String statTimeName = name + "Time";
      String statTimeDescription =
          "Total amount of time, in nanoseconds, spent executing redis '"
              + name + "' operations on this server.";

      descriptors.add(
          factory.createLongCounter(statTimeName, statTimeDescription, "nanoseconds"));
    }

    return descriptors.toArray(new StatisticDescriptor[0]);
  }

  private static void fillCompletedIdMap(RedisCommandType.Category category) {
    String categoryName = category.name();
    for (RedisCommandType command : RedisCommandType.getCommandsForCategory(category)) {
      String name = command.name().toLowerCase();
      String statName = name + "Completed";
      completedCommandStatIds.put(command, statisticTypes.get(categoryName).nameToId(statName));
    }
  }

  private static void fillTimeIdMap(RedisCommandType.Category category) {
    String categoryName = category.name();
    for (RedisCommandType command : RedisCommandType.getCommandsForCategory(category)) {
      String name = command.name().toLowerCase();
      String statName = name + "Time";
      timeCommandStatIds.put(command, statisticTypes.get(categoryName).nameToId(statName));
    }
  }

  private static StatisticDescriptor[] createGeneralStatisticDescriptors(
      StatisticsTypeFactory statisticsTypeFactory) {
    ArrayList<StatisticDescriptor> descriptors = new ArrayList<>();

    descriptors.add(statisticsTypeFactory.createLongGauge("connectedClients",
        "Current client connections to this redis server.",
        "clients"));

    descriptors.add(statisticsTypeFactory.createLongCounter("commandsProcessed",
        "Total number of commands processed by this redis server.",
        "commands"));

    descriptors.add(statisticsTypeFactory.createLongCounter("keyspaceHits",
        "Total number of successful key lookups on this redis server"
            + " from cache on this redis server.",
        "hits"));

    descriptors.add(statisticsTypeFactory.createLongCounter("keyspaceMisses",
        "Total number of keys requested but not found on this redis server.",
        "misses"));

    descriptors.add(statisticsTypeFactory.createLongCounter("totalNetworkBytesRead",
        "Total number of bytes read by this redis server.",
        "bytes"));

    descriptors.add(statisticsTypeFactory.createLongCounter("totalConnectionsReceived",
        "Total number of client connections received by this redis server since startup.",
        "connections"));

    descriptors.add(statisticsTypeFactory.createLongCounter("activeExpirationChecks",
        "Total number of active expiration checks that have"
            + " completed. Checks include scanning and expiring.",
        "checks"));

    descriptors.add(statisticsTypeFactory.createLongCounter("activeExpirationCheckTime",
        "Total amount of time, in nanoseconds, spent in active "
            + "expiration checks on this server.",
        "nanoseconds"));

    descriptors.add(statisticsTypeFactory.createLongCounter("activeExpirations",
        "Total number of keys that have been actively expired on this server.",
        "expirations"));

    descriptors.add(statisticsTypeFactory.createLongCounter("expirations",
        "Total number of keys that have been expired, actively or passively, on this server.",
        "expirations"));

    descriptors.add(statisticsTypeFactory.createLongCounter("expirationTime",
        "Total amount of time, in nanoseconds, spent expiring keys on this server.",
        "nanoseconds"));

    descriptors.add(statisticsTypeFactory.createLongCounter("publishRequestsCompleted",
        "Total number of publish requests received by this server that have completed processing.",
        "ops"));
    descriptors.add(statisticsTypeFactory.createLongGauge("publishRequestsInProgress",
        "Current number of publish requests received by this server that are still being processed.",
        "ops"));
    descriptors.add(statisticsTypeFactory.createLongCounter("publishRequestTime",
        "Total amount of time, in nanoseconds, processing publish requests on this server. For each request this stat measures the time elapsed between when the request arrived on the server and when the request was delivered to all subscribers.",
        "nanoseconds"));
    descriptors.add(statisticsTypeFactory.createLongGauge("subscribers",
        "Current number of subscribers connected to this server.",
        "subscribers"));
    descriptors.add(statisticsTypeFactory.createLongGauge("uniqueChannelSubscriptions",
        "Current number of unique channel subscriptions on this server. Multiple subscribers can be on the same channel.",
        "subscriptions"));
    descriptors.add(statisticsTypeFactory.createLongGauge("uniquePatternSubscriptions",
        "Current number of unique pattern subscriptions on this server. Multiple subscribers can be on the same pattern.",
        "subscriptions"));

    return descriptors.toArray(new StatisticDescriptor[0]);
  }

}
