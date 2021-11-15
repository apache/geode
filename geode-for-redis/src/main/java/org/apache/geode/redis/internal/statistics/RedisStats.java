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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.getTime;
import static org.apache.geode.logging.internal.executors.LoggingExecutors.newSingleThreadScheduledExecutor;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.redis.internal.RedisCommandType;

public class RedisStats {
  private final AtomicLong commandsProcessed = new AtomicLong();
  private final AtomicLong totalNetworkBytesRead = new AtomicLong();
  private final AtomicLong totalConnectionsReceived = new AtomicLong();
  private final AtomicLong currentlyConnectedClients = new AtomicLong();
  private final AtomicLong expirations = new AtomicLong();
  private final AtomicLong keyspaceHits = new AtomicLong();
  private final AtomicLong keyspaceMisses = new AtomicLong();
  private final ScheduledExecutorService perSecondExecutor;
  private volatile double networkKiloBytesReadOverLastSecond;
  private volatile long opsPerformedLastTick;
  private double opsPerformedOverLastSecond;
  private long previousNetworkBytesRead;
  private final StatisticsClock clock;
  private final GeodeRedisStats geodeRedisStats;
  private final long START_TIME_IN_NANOS;


  public RedisStats(StatisticsClock clock,
      GeodeRedisStats geodeRedisStats) {

    this.clock = clock;
    this.geodeRedisStats = geodeRedisStats;
    perSecondExecutor = startPerSecondUpdater();
    START_TIME_IN_NANOS = clock.getTime();
  }

  public void incCommandsProcessed() {
    commandsProcessed.incrementAndGet();
    geodeRedisStats.incrementCommandsProcessed();
  }

  public long getCommandsProcessed() {
    return commandsProcessed.get();
  }

  public void addClient() {
    totalConnectionsReceived.incrementAndGet();
    currentlyConnectedClients.incrementAndGet();
    geodeRedisStats.addClient();
  }

  public void removeClient() {
    currentlyConnectedClients.decrementAndGet();
    geodeRedisStats.removeClient();
  }

  public long getTotalConnectionsReceived() {
    return totalConnectionsReceived.get();
  }

  public long getConnectedClients() {
    return currentlyConnectedClients.get();
  }

  public double getNetworkKiloBytesReadOverLastSecond() {
    return networkKiloBytesReadOverLastSecond;
  }

  public double getOpsPerformedOverLastSecond() {
    return opsPerformedOverLastSecond;
  }

  public long startCommand() {
    return getTime();
  }

  public void endCommand(RedisCommandType command, long start) {
    geodeRedisStats.endCommand(command, start);
  }

  public void incNetworkBytesRead(long bytesRead) {
    totalNetworkBytesRead.addAndGet(bytesRead);
    geodeRedisStats.incrementTotalNetworkBytesRead(bytesRead);
  }

  public long getTotalNetworkBytesRead() {
    return totalNetworkBytesRead.get();
  }

  public long getCurrentTimeNanos() {
    return clock.getTime();
  }

  public long getUptimeInMilliseconds() {
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
    geodeRedisStats.incrementKeyspaceHits();
  }

  public long getKeyspaceHits() {
    return keyspaceHits.get();
  }

  public void incKeyspaceMisses() {
    keyspaceMisses.incrementAndGet();
    geodeRedisStats.incrementKeyspaceMisses();
  }

  public long getKeyspaceMisses() {
    return keyspaceMisses.get();
  }

  public long startPassiveExpirationCheck() {
    return getCurrentTimeNanos();
  }

  public void endPassiveExpirationCheck(long start, long expireCount) {
    geodeRedisStats.endPassiveExpirationCheck(start, expireCount);
  }

  public long startExpiration() {
    return getCurrentTimeNanos();
  }

  public void endExpiration(long start) {
    geodeRedisStats.endExpiration(start);
    expirations.incrementAndGet();
  }


  public long startPublish() {
    return geodeRedisStats.startPublish();
  }

  public void endPublish(long publishCount, long time) {
    geodeRedisStats.endPublish(publishCount, time);
  }

  public void changeSubscribers(long delta) {
    geodeRedisStats.changeSubscribers(delta);
  }

  public void changeUniqueChannelSubscriptions(long delta) {
    geodeRedisStats.changeUniqueChannelSubscriptions(delta);
  }

  public void changeUniquePatternSubscriptions(long delta) {
    geodeRedisStats.changeUniquePatternSubscriptions(delta);
  }

  public void close() {
    geodeRedisStats.close();
    stopPerSecondUpdater();
  }

  private ScheduledExecutorService startPerSecondUpdater() {
    int INTERVAL = 1;

    ScheduledExecutorService perSecondExecutor =
        newSingleThreadScheduledExecutor("GemFireRedis-PerSecondUpdater-");

    perSecondExecutor.scheduleWithFixedDelay(
        this::doPerSecondUpdates,
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
    final long totalNetworkBytesRead = getTotalNetworkBytesRead();
    long deltaNetworkBytesRead = totalNetworkBytesRead - previousNetworkBytesRead;
    networkKiloBytesReadOverLastSecond = deltaNetworkBytesRead / 1024.0;
    previousNetworkBytesRead = totalNetworkBytesRead;
  }

  private void updateOpsPerformedOverLastSecond() {
    final long totalOpsPerformed = getCommandsProcessed();
    opsPerformedOverLastSecond = totalOpsPerformed - opsPerformedLastTick;
    opsPerformedLastTick = totalOpsPerformed;
  }
}
