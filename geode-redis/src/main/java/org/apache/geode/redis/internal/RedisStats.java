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
import static org.apache.geode.internal.statistics.StatisticsClockFactory.getTime;
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
  private final AtomicLong commandsProcessed = new AtomicLong();
  private final AtomicLong opsPerSecond = new AtomicLong();
  private final AtomicLong totalNetworkBytesRead = new AtomicLong();
  private final AtomicLong connectionsReceived = new AtomicLong();
  private final AtomicLong expirations = new AtomicLong();
  private final AtomicLong keyspaceHits = new AtomicLong();
  private final AtomicLong keyspaceMisses = new AtomicLong();
  private final ScheduledExecutorService perSecondExecutor;
  private volatile double networkKiloBytesReadDuringLastSecond;
  private volatile long opsPerformedLastTick;
  private double opsPerformedOverLastSecond;
  private long previousNetworkBytesRead;
  private final StatisticsClock clock;
  private final Statistics stats;
  private final long START_TIME_IN_NANOS;

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

  public RedisStats(StatisticsFactory factory, StatisticsClock clock) {
    this(factory, "redisStats", clock);
  }

  public RedisStats(StatisticsFactory factory, String textId, StatisticsClock clock) {
    stats = factory == null ? null : factory.createAtomicStatistics(type, textId);
    this.clock = clock;
    this.START_TIME_IN_NANOS = this.clock.getTime();
    perSecondExecutor = startPerSecondUpdater();
  }

  public void clearAllStats() {
    commandsProcessed.set(0);
    opsPerSecond.set(0);
    totalNetworkBytesRead.set(0);
    connectionsReceived.set(0);
    expirations.set(0);
    keyspaceHits.set(0);
    keyspaceMisses.set(0);
    stats.setLong(clientId, 0);
    stats.setLong(passiveExpirationChecksId, 0);
    stats.setLong(passiveExpirationCheckTimeId,0);
    stats.setLong(passiveExpirationsId,0);
    stats.setLong(expirationsId,0);
    stats.setLong(expirationTimeId,0);
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
      String statDescription =
          "Total amount of time, in nanoseconds, spent executing redis '"
              + name +
              "' operations on this server.";

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

  public double getNetworkKiloBytesReadOverLastSecond() {
    return networkKiloBytesReadDuringLastSecond;
  }

  public double getOpsPerformedOverLastSecond() {
    return opsPerformedOverLastSecond;
  }

  private long getCurrentTimeNanos() {
    return clock.getTime();
  }

  public long startCommand(RedisCommandType command) {
    return getTime();
  }

  public void endCommand(RedisCommandType command, long start) {
    if (clock.isEnabled()) {
      stats.incLong(timeCommandStatIds.get(command), getCurrentTimeNanos() - start);
    }
    stats.incLong(completedCommandStatIds.get(command), 1);
  }

  public void addClient() {
    connectionsReceived.incrementAndGet();
    stats.incLong(clientId, 1);
  }

  public void removeClient() {
    stats.incLong(clientId, -1);
  }

  public long getConnectionsReceived() {
    return connectionsReceived.get();
  }

  public long getConnectedClients() {
    return stats.getLong(clientId);
  }

  public void incCommandsProcessed() {
    commandsProcessed.incrementAndGet();
  }

  public long getCommandsProcessed() {
    return commandsProcessed.get();
  }

  public void incNetworkBytesRead(long bytesRead) {
    totalNetworkBytesRead.addAndGet(bytesRead);
  }

  public long getTotalNetworkBytesRead() {
    return totalNetworkBytesRead.get();
  }

  private long getUptimeInMilliseconds() {
    long uptimeInNanos = getCurrentTimeNanos() - START_TIME_IN_NANOS;
    return TimeUnit.NANOSECONDS.toMillis(uptimeInNanos);
  }

  public long getUptimeInSeconds() {
    return TimeUnit.MILLISECONDS.toSeconds(getUptimeInMilliseconds());
  }

  public long getUptimeInDays() {
    return TimeUnit.MILLISECONDS.toDays(getUptimeInMilliseconds());
  }

  public void incKeyspaceHits() {
    keyspaceHits.incrementAndGet();
  }

  public long getKeyspaceHits() {
    return keyspaceHits.get();
  }

  public void incKeyspaceMisses() {
    keyspaceMisses.incrementAndGet();
  }

  public long getKeyspaceMisses() {
    return keyspaceMisses.get();
  }

  public long startPassiveExpirationCheck() {
    return getCurrentTimeNanos();
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

  public long startExpiration() {
    return getCurrentTimeNanos();
  }

  public void endExpiration(long start) {
    if (clock.isEnabled()) {
      stats.incLong(expirationTimeId, getCurrentTimeNanos() - start);
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

  private ScheduledExecutorService startPerSecondUpdater() {
    int INTERVAL = 1;

    ScheduledExecutorService perSecondExecutor =
        newSingleThreadScheduledExecutor("GemFireRedis-PerSecondUpdater-");

    perSecondExecutor.scheduleWithFixedDelay(
        () -> doPerSecondUpdates(),
        INTERVAL,
        INTERVAL,
        SECONDS);

    return perSecondExecutor;
  }

  private void stopPerSecondUpdater() {
    perSecondExecutor.shutdownNow();
  }

  private void doPerSecondUpdates() {
    updateNetworkKilobytesReadLastSecond();
    updateOpsPerformedOverLastSecond();
  }

  private void updateNetworkKilobytesReadLastSecond() {
    long totalNetworkBytesRead = getTotalNetworkBytesRead();
    long deltaNetworkBytesRead = totalNetworkBytesRead - previousNetworkBytesRead;
    networkKiloBytesReadDuringLastSecond = deltaNetworkBytesRead / 1000;
    previousNetworkBytesRead = getTotalNetworkBytesRead();
  }

  private void updateOpsPerformedOverLastSecond() {
    long totalOpsPerformed = getCommandsProcessed();
    long opsPerformedThisTick = totalOpsPerformed - opsPerformedLastTick;
    opsPerformedOverLastSecond = opsPerformedThisTick;
    opsPerformedLastTick = getCommandsProcessed();
  }
}
