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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.logging.internal.executors.LoggingExecutors.newSingleThreadScheduledExecutor;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.key.RedisKeyCommands;

public class PassiveExpirationManager {
  private static final Logger logger = LogService.getLogger();

  private final ScheduledExecutorService expirationExecutor;

  @VisibleForTesting
  public static final int INTERVAL = 3;

  public PassiveExpirationManager(RegionProvider regionProvider) {
    expirationExecutor = newSingleThreadScheduledExecutor("GemFireRedis-PassiveExpiration-");
    expirationExecutor.scheduleWithFixedDelay(() -> doDataExpiration(regionProvider), INTERVAL,
        INTERVAL, MINUTES);
  }

  public void stop() {
    expirationExecutor.shutdownNow();
  }

  private void doDataExpiration(RegionProvider regionProvider) {
    final long start = regionProvider.getRedisStats().startPassiveExpirationCheck();
    long expireCount = 0;
    try {
      final long now = System.currentTimeMillis();
      Region<RedisKey, RedisData> localPrimaryData =
          PartitionRegionHelper.getLocalPrimaryData(regionProvider.getLocalDataRegion());
      RedisKeyCommands redisKeyCommands = regionProvider.getKeyCommands();
      for (Map.Entry<RedisKey, RedisData> entry : localPrimaryData.entrySet()) {
        try {
          if (entry.getValue().hasExpired(now)) {
            // pttl will do its own check using active expiration and expire the key if needed
            if (-2 == redisKeyCommands.internalPttl(entry.getKey())) {
              expireCount++;
            }
          }
        } catch (EntryDestroyedException ignore) {
        }
      }
    } catch (CacheClosedException ignore) {
    } catch (RuntimeException | Error ex) {
      logger.warn("Passive expiration failed. Will try again in 1 second.",
          ex);
    } finally {
      regionProvider.getRedisStats().endPassiveExpirationCheck(start, expireCount);
    }
  }
}
