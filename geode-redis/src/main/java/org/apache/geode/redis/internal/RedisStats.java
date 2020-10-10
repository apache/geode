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

package org.apache.geode.redis.internal;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.logging.internal.executors.LoggingExecutors.newSingleThreadScheduledExecutor;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

public class RedisStats {
  @Immutable
  private static final StatisticsType type;
  @Immutable
  private static final EnumMap<RedisCommandType, Integer> completedCommandStatIds =
      new EnumMap<>(RedisCommandType.class);
  @Immutable
  private static final EnumMap<RedisCommandType, Integer> timeCommandStatIds =
      new EnumMap<>(RedisCommandType.class);

  private static final int clientId;
  private static final int passiveExpirationChecksId;
  private static final int passiveExpirationCheckTimeId;
  private static final int passiveExpirationsId;
  private static final int expirationsId;
  private static final int expirationTimeId;

  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    ArrayList<StatisticDescriptor> descriptorList = new ArrayList<>();
    fillListWithCompletedCommandDescriptors(f, descriptorList);
    fillListWithTimeCommandDescriptors(f, descriptorList);
    descriptorList.add(f.createLongGauge("clients",
        "Current number of clients connected to this redis server.", "clients"));
    descriptorList.add(f.createLongCounter("passiveExpirationChecks",
        "Total number of passive expiration checks that have completed. Checks include scanning and expiring.",
        "checks"));
    descriptorList.add(f.createLongCounter("passiveExpirationCheckTime",
        "Total amount of time, in nanoseconds, spent in passive expiration checks on this server.",
        "nanoseconds"));
    descriptorList.add(f.createLongCounter("passiveExpirations",
        "Total number of keys that have been passively expired on this server.", "expirations"));
    descriptorList.add(f.createLongCounter("expirations",
        "Total number of keys that have been expired, actively or passively, on this server.",
        "expirations"));
    descriptorList.add(f.createLongCounter("expirationTime",
        "Total amount of time, in nanoseconds, spent expiring keys on this server.",
        "nanoseconds"));
    StatisticDescriptor[] descriptorArray =
        descriptorList.toArray(new StatisticDescriptor[descriptorList.size()]);

    type = f.createType("RedisStats", "Statistics for a GemFire Redis Server", descriptorArray);

    fillCompletedIdMap();
    fillTimeIdMap();
    clientId = type.nameToId("clients");
    passiveExpirationChecksId = type.nameToId("passiveExpirationChecks");
    passiveExpirationCheckTimeId = type.nameToId("passiveExpirationCheckTime");
    passiveExpirationsId = type.nameToId("passiveExpirations");
    expirationsId = type.nameToId("expirations");
    expirationTimeId = type.nameToId("expirationTime");
  }

  private static void fillListWithCompletedCommandDescriptors(StatisticsTypeFactory f,
      ArrayList<StatisticDescriptor> descriptorList) {
    for (RedisCommandType command : RedisCommandType.values()) {
      if (command.isUnimplemented()) {
        continue;
      }
      String name = command.name().toLowerCase();
      String statName = name + "Completed";
      String statDescription = "Total number of redis '" + name
          + "' operations that have completed execution on this server.";
      String units = "operations";
      descriptorList.add(f.createLongCounter(statName, statDescription, units));
    }
  }

  private static void fillListWithTimeCommandDescriptors(StatisticsTypeFactory f,
      ArrayList<StatisticDescriptor> descriptorList) {
    for (RedisCommandType command : RedisCommandType.values()) {
      if (command.isUnimplemented()) {
        continue;
      }
      String name = command.name().toLowerCase();
      String statName = name + "Time";
      String statDescription = "Total amount of time, in nanoseconds, spent executing redis '"
          + name + "' operations on this server.";
      String units = "nanoseconds";
      descriptorList.add(f.createLongCounter(statName, statDescription, units));
    }
  }

  private static void fillCompletedIdMap() {
    for (RedisCommandType command : RedisCommandType.values()) {
      if (command.isUnimplemented()) {
        continue;
      }
      String name = command.name().toLowerCase();
      String statName = name + "Completed";
      completedCommandStatIds.put(command, type.nameToId(statName));
    }
  }

  private static void fillTimeIdMap() {
    for (RedisCommandType command : RedisCommandType.values()) {
      if (command.isUnimplemented()) {
        continue;
      }
      String name = command.name().toLowerCase();
      String statName = name + "Time";
      timeCommandStatIds.put(command, type.nameToId(statName));
    }
  }

  private final Statistics stats;

  private final StatisticsClock clock;
  private final long startTime;

  public RedisStats(StatisticsFactory factory, StatisticsClock clock) {
    this(factory, "redisStats", clock);
  }

  public RedisStats(StatisticsFactory factory, String textId, StatisticsClock clock) {
    startTime = System.currentTimeMillis();
    stats = factory == null ? null : factory.createAtomicStatistics(type, textId);
    this.clock = clock;
    perSecondExecutor = startPerSecondUpdater();
  }

  public static StatisticsType getStatisticsType() {
    return type;
  }

  public Statistics getStats() {
    return stats;
  }

  public long getTime() {
    return clock.getTime();
  }

  public long startCommand(RedisCommandType command) {
    return getTime();
  }

  public void endCommand(RedisCommandType command, long start) {
    if (clock.isEnabled()) {
      stats.incLong(timeCommandStatIds.get(command), getTime() - start);
    }
    stats.incLong(completedCommandStatIds.get(command), 1);
  }

  public void addClient() {
    connectionsReceived.incrementAndGet();
    connectedClients.incrementAndGet();
    stats.incLong(clientId, 1);
  }

  public void removeClient() {
    connectedClients.decrementAndGet();
    stats.incLong(clientId, -1);
  }

  private final AtomicLong commandsProcessed = new AtomicLong();
  private final AtomicLong opsPerSecond = new AtomicLong();
  private final AtomicLong networkBytesRead = new AtomicLong();
  private volatile double networkKilobytesReadPerSecond;
  private final AtomicLong connectionsReceived = new AtomicLong();
  private final AtomicLong connectedClients = new AtomicLong();
  private final AtomicLong expirations = new AtomicLong();

  public void incCommandsProcessed() {
    commandsProcessed.incrementAndGet();
  }

  public long getCommandsProcessed() {
    return commandsProcessed.get();
  }

  public long getOpsPerSecond() {
    return opsPerSecond.get();
  }


  public void incNetworkBytesRead(long bytesRead) {
    networkBytesRead.addAndGet(bytesRead);
  }

  public long getNetworkBytesRead() {
    return networkBytesRead.get();
  }

  public double getNetworkKilobytesReadPerSecond() {
    return networkKilobytesReadPerSecond;
  }


  public long getConnectionsReceived() {
    return connectionsReceived.get();
  }

  public long getConnectedClients() {
    return connectedClients.get();
  }

  public long getExpirations() {
    return expirations.get();
  }

  private long getUptimeInMilliseconds() {
    return System.currentTimeMillis() - startTime;
  }

  public long getUptimeInSeconds() {
    return TimeUnit.MILLISECONDS.toSeconds(getUptimeInMilliseconds());
  }

  public long getUptimeInDays() {
    return TimeUnit.MILLISECONDS.toDays(getUptimeInMilliseconds());
  }

  @VisibleForTesting
  long getClients() {
    return stats.getLong(clientId);
  }

  public long startPassiveExpirationCheck() {
    return getTime();
  }

  public void endPassiveExpirationCheck(long start, long expireCount) {
    if (expireCount > 0) {
      incPassiveExpirations(expireCount);
    }
    if (clock.isEnabled()) {
      stats.incLong(passiveExpirationCheckTimeId, getTime() - start);
    }
    stats.incLong(passiveExpirationChecksId, 1);
  }

  public long startExpiration() {
    return getTime();
  }

  public void endExpiration(long start) {
    if (clock.isEnabled()) {
      stats.incLong(expirationTimeId, getTime() - start);
    }
    stats.incLong(expirationsId, 1);
    expirations.incrementAndGet();
  }

  public void incPassiveExpirations(long count) {
    stats.incLong(passiveExpirationsId, count);
  }

  public void close() {
    if (stats != null) {
      stats.close();
    }
    stopPerSecondUpdater();
  }


  private final ScheduledExecutorService perSecondExecutor;

  private ScheduledExecutorService startPerSecondUpdater() {
    ScheduledExecutorService perSecondExecutor =
        newSingleThreadScheduledExecutor("GemFireRedis-PerSecondUpdater-");
    int INTERVAL = 1;
    perSecondExecutor.scheduleWithFixedDelay(() -> doPerSecondUpdates(), INTERVAL,
        INTERVAL,
        SECONDS);
    return perSecondExecutor;
  }

  private void stopPerSecondUpdater() {
    perSecondExecutor.shutdownNow();
  }

  private long previousCommandsProcessed;
  private long previousNetworkBytesRead;

  private void doPerSecondUpdates() {
    long currentCommandsProcessed = commandsProcessed.get();
    long currentNetworkBytesRead = networkBytesRead.get();
    opsPerSecond.set(currentCommandsProcessed - previousCommandsProcessed);
    long deltaNetworkBytesRead = currentNetworkBytesRead - previousNetworkBytesRead;
    networkKilobytesReadPerSecond = deltaNetworkBytesRead / 1024.0;
    previousCommandsProcessed = currentCommandsProcessed;
    previousNetworkBytesRead = currentNetworkBytesRead;
  }
}
