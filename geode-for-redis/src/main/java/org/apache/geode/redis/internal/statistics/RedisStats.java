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

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.getTime;
import static org.apache.geode.logging.internal.executors.LoggingExecutors.newSingleThreadScheduledExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.commands.RedisCommandType;

public class RedisStats {
  private final AtomicLong commandsProcessed = new AtomicLong();
  private final AtomicLong totalNetworkBytesRead = new AtomicLong();
  private final AtomicLong totalConnectionsReceived = new AtomicLong();
  private final AtomicLong currentlyConnectedClients = new AtomicLong();
  private final AtomicLong expirations = new AtomicLong();
  private final AtomicLong keyspaceHits = new AtomicLong();
  private final AtomicLong keyspaceMisses = new AtomicLong();
  private final AtomicLong uniqueChannelSubscriptions = new AtomicLong();
  private final AtomicLong uniquePatternSubscriptions = new AtomicLong();

  private final ScheduledExecutorService rollingAverageExecutor;
  private static final int ROLLING_AVERAGE_SAMPLES_PER_SECOND = 16;
  private final RollingAverageStat networkBytesReadRollingAverageStat =
      new RollingAverageStat(ROLLING_AVERAGE_SAMPLES_PER_SECOND, this::getTotalNetworkBytesRead);
  private volatile double networkKiloBytesReadOverLastSecond;
  private final RollingAverageStat opsPerformedRollingAverageStat =
      new RollingAverageStat(ROLLING_AVERAGE_SAMPLES_PER_SECOND, this::getCommandsProcessed);
  private volatile long opsPerformedOverLastSecond;

  private final StatisticsClock clock;
  private final GeodeRedisStats geodeRedisStats;
  private final long startTimeInNanos;

  public RedisStats(StatisticsClock clock, GeodeRedisStats geodeRedisStats) {
    this.clock = clock;
    this.geodeRedisStats = geodeRedisStats;
    rollingAverageExecutor = startRollingAverageUpdater();
    startTimeInNanos = clock.getTime();
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

  public long getOpsPerformedOverLastSecond() {
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
    long uptimeInNanos = getCurrentTimeNanos() - startTimeInNanos;
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

  public long startActiveExpirationCheck() {
    return getCurrentTimeNanos();
  }

  public void endActiveExpirationCheck(long start, long expireCount) {
    geodeRedisStats.endActiveExpirationCheck(start, expireCount);
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

  public long getUniqueChannelSubscriptions() {
    return uniqueChannelSubscriptions.get();
  }

  public long getUniquePatternSubscriptions() {
    return uniquePatternSubscriptions.get();
  }

  public void changeUniqueChannelSubscriptions(long delta) {
    uniqueChannelSubscriptions.addAndGet(delta);
    geodeRedisStats.changeUniqueChannelSubscriptions(delta);
  }

  public void changeUniquePatternSubscriptions(long delta) {
    uniquePatternSubscriptions.addAndGet(delta);
    geodeRedisStats.changeUniquePatternSubscriptions(delta);
  }

  public void close() {
    geodeRedisStats.close();
    stopRollingAverageUpdater();
  }

  private ScheduledExecutorService startRollingAverageUpdater() {
    long microsPerSecond = 1_000_000;
    final long delayMicros = microsPerSecond / ROLLING_AVERAGE_SAMPLES_PER_SECOND;

    ScheduledExecutorService rollingAverageExecutor =
        newSingleThreadScheduledExecutor("GemFireRedis-RollingAverageStatUpdater-");

    rollingAverageExecutor.scheduleWithFixedDelay(this::doRollingAverageUpdates, delayMicros,
        delayMicros, MICROSECONDS);

    return rollingAverageExecutor;
  }

  private void stopRollingAverageUpdater() {
    rollingAverageExecutor.shutdownNow();
  }

  private void doRollingAverageUpdates() {
    networkKiloBytesReadOverLastSecond = networkBytesReadRollingAverageStat.calculate() / 1024.0;
    opsPerformedOverLastSecond = opsPerformedRollingAverageStat.calculate();
  }

  private static class RollingAverageStat {
    private int tickNumber = 0;
    private long valueReadLastTick;
    private final long[] valuesReadOverLastNSamples;
    private long currentTotal = 0;
    private final Callable<Long> statCallable;

    private RollingAverageStat(int samplesPerSecond, Callable<Long> getCurrentValue) {
      valuesReadOverLastNSamples = new long[samplesPerSecond];
      statCallable = getCurrentValue;
    }

    private long calculate() {
      long currentValue;
      try {
        currentValue = statCallable.call();
      } catch (Exception e) {
        throw new RedisException("Error while calculating RollingAverage stats", e);
      }
      long delta = currentValue - valueReadLastTick;
      valueReadLastTick = currentValue;

      currentTotal = currentTotal + delta - valuesReadOverLastNSamples[tickNumber];
      valuesReadOverLastNSamples[tickNumber] = delta;
      // same as mod (%) but measurably faster...
      tickNumber = (tickNumber + 1) & (valuesReadOverLastNSamples.length - 1);

      return currentTotal;
    }
  }
}
