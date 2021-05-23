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

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;
import org.apache.geode.redis.internal.RedisCommandType;

public class GeodeRedisStats {
  @Immutable
  private static final StatisticsType type;
  @Immutable
  private static final EnumMap<RedisCommandType, Integer> completedCommandStatIds =
      new EnumMap<>(RedisCommandType.class);
  @Immutable
  private static final EnumMap<RedisCommandType, Integer> timeCommandStatIds =
      new EnumMap<>(RedisCommandType.class);

  private static final int currentlyConnectedClients;
  private static final int passiveExpirationChecksId;
  private static final int passiveExpirationCheckTimeId;
  private static final int passiveExpirationsId;
  private static final int expirationsId;
  private static final int expirationTimeId;
  private static final int totalConnectionsReceived;
  private static final int commandsProcessed;
  private static final int keyspaceHits;
  private static final int keyspaceMisses;
  private static final int totalNetworkBytesRead;
  private final Statistics stats;
  private final StatisticsClock clock;

  public GeodeRedisStats(StatisticsFactory factory, String textId,
      StatisticsClock clock) {
    this.clock = clock;
    stats = factory == null ? null : factory.createAtomicStatistics(type, textId);
  }

  static {
    StatisticsTypeFactory statisticsTypeFactory = StatisticsTypeFactoryImpl.singleton();
    ArrayList<StatisticDescriptor> descriptorList = new ArrayList<>();

    fillListWithCompletedCommandDescriptors(statisticsTypeFactory, descriptorList);
    fillListWithTimeCommandDescriptors(statisticsTypeFactory, descriptorList);
    fillListWithCommandDescriptors(statisticsTypeFactory, descriptorList);

    StatisticDescriptor[] descriptorArray =
        descriptorList.toArray(new StatisticDescriptor[0]);

    type = statisticsTypeFactory
        .createType("statsForServerCompatibleWithRedis",
            "Statistics for a Geode server compatible with Redis",
            descriptorArray);

    fillCompletedIdMap(type);
    fillTimeIdMap(type);

    currentlyConnectedClients = type.nameToId("connectedClients");
    passiveExpirationChecksId = type.nameToId("passiveExpirationChecks");
    passiveExpirationCheckTimeId = type.nameToId("passiveExpirationCheckTime");
    passiveExpirationsId = type.nameToId("passiveExpirations");
    expirationsId = type.nameToId("expirations");
    expirationTimeId = type.nameToId("expirationTime");
    totalConnectionsReceived = type.nameToId("totalConnectionsReceived");
    commandsProcessed = type.nameToId("commandsProcessed");
    totalNetworkBytesRead = type.nameToId("totalNetworkBytesRead");
    keyspaceHits = type.nameToId("keyspaceHits");
    keyspaceMisses = type.nameToId("keyspaceMisses");
  }

  private long getCurrentTimeNanos() {
    return clock.getTime();
  }

  public void endCommand(RedisCommandType command, long start) {
    if (clock.isEnabled()) {
      stats.incLong(timeCommandStatIds.get(command), getCurrentTimeNanos() - start);
    }
    stats.incLong(completedCommandStatIds.get(command), 1);
  }

  public void addClient() {
    stats.incLong(currentlyConnectedClients, 1);
    stats.incLong(totalConnectionsReceived, 1);
  }

  public void removeClient() {
    stats.incLong(currentlyConnectedClients, -1);
  }

  public void endPassiveExpirationCheck(long start, long expireCount) {
    if (expireCount > 0) {
      incPassiveExpirations(expireCount);
    }
    if (clock.isEnabled()) {
      stats.incLong(passiveExpirationCheckTimeId, getCurrentTimeNanos() - start);
    }
    stats.incLong(passiveExpirationChecksId, 1);
  }

  private void incPassiveExpirations(long count) {
    stats.incLong(passiveExpirationsId, count);
  }

  public void endExpiration(long start) {
    if (clock.isEnabled()) {
      stats.incLong(expirationTimeId, getCurrentTimeNanos() - start);
    }
    stats.incLong(expirationsId, 1);
  }

  public void incrementCommandsProcessed() {
    stats.incLong(commandsProcessed, 1);
  }

  public void incrementTotalNetworkBytesRead(long bytes) {
    stats.incLong(totalNetworkBytesRead, bytes);
  }

  public void incrementKeyspaceHits() {
    stats.incLong(keyspaceHits, 1);
  }

  public void incrementKeyspaceMisses() {
    stats.incLong(keyspaceMisses, 1);
  }

  public void close() {
    if (stats != null) {
      stats.close();
    }
  }

  private static void fillListWithTimeCommandDescriptors(StatisticsTypeFactory factory,
      ArrayList<StatisticDescriptor> descriptorList) {

    for (RedisCommandType command : RedisCommandType.values()) {
      String name = command.name().toLowerCase();
      String statName = name + "Time";
      String statDescription =
          "Total amount of time, in nanoseconds, spent executing redis '"
              + name + "' operations on this server.";
      String units = "nanoseconds";

      descriptorList.add(factory.createLongCounter(statName, statDescription, units));
    }
  }

  private static void fillListWithCompletedCommandDescriptors(StatisticsTypeFactory factory,
      ArrayList<StatisticDescriptor> descriptorList) {
    for (RedisCommandType command : RedisCommandType.values()) {
      String name = command.name().toLowerCase();
      String statName = name + "Completed";
      String statDescription = "Total number of redis '" + name
          + "' operations that have completed execution on this server.";
      String units = "operations";

      descriptorList.add(factory.createLongCounter(statName, statDescription, units));
    }
  }

  private static void fillCompletedIdMap(StatisticsType type) {
    for (RedisCommandType command : RedisCommandType.values()) {
      String name = command.name().toLowerCase();
      String statName = name + "Completed";

      completedCommandStatIds.put(command, type.nameToId(statName));
    }
  }

  private static void fillListWithCommandDescriptors(StatisticsTypeFactory statisticsTypeFactory,
      ArrayList<StatisticDescriptor> descriptorList) {

    descriptorList.add(statisticsTypeFactory.createLongGauge("connectedClients",
        "Current client connections to this redis server.",
        "clients"));

    descriptorList.add(statisticsTypeFactory.createLongCounter("commandsProcessed",
        "Total number of commands processed by this redis server.",
        "commands"));

    descriptorList.add(statisticsTypeFactory.createLongCounter("keyspaceHits",
        "Total number of successful key lookups on this redis server"
            + " from cache on this redis server.",
        "hits"));

    descriptorList.add(statisticsTypeFactory.createLongCounter("keyspaceMisses",
        "Total number of keys requested but not found on this redis server.",
        "misses"));

    descriptorList.add(statisticsTypeFactory.createLongCounter("totalNetworkBytesRead",
        "Total number of bytes read by this redis server.",
        "bytes"));

    descriptorList.add(statisticsTypeFactory.createLongCounter("totalConnectionsReceived",
        "Total number of client connections received by this redis server since startup.",
        "connections"));

    descriptorList.add(statisticsTypeFactory.createLongCounter("passiveExpirationChecks",
        "Total number of passive expiration checks that have"
            + " completed. Checks include scanning and expiring.",
        "checks"));

    descriptorList.add(statisticsTypeFactory.createLongCounter("passiveExpirationCheckTime",
        "Total amount of time, in nanoseconds, spent in passive "
            + "expiration checks on this server.",
        "nanoseconds"));

    descriptorList.add(statisticsTypeFactory.createLongCounter("passiveExpirations",
        "Total number of keys that have been passively expired on this server.",
        "expirations"));

    descriptorList.add(statisticsTypeFactory.createLongCounter("expirations",
        "Total number of keys that have been expired, actively or passively, on this server.",
        "expirations"));

    descriptorList.add(statisticsTypeFactory.createLongCounter("expirationTime",
        "Total amount of time, in nanoseconds, spent expiring keys on this server.",
        "nanoseconds"));
  }

  private static void fillTimeIdMap(StatisticsType type) {
    for (RedisCommandType command : RedisCommandType.values()) {
      String name = command.name().toLowerCase();
      String statName = name + "Time";
      timeCommandStatIds.put(command, type.nameToId(statName));
    }
  }
}
