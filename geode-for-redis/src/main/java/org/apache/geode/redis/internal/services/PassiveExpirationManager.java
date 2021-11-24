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

package org.apache.geode.redis.internal.services;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.logging.internal.executors.LoggingExecutors.newSingleThreadScheduledExecutor;
import static org.apache.geode.redis.internal.RedisProperties.EXPIRATION_INTERVAL_SECONDS;
import static org.apache.geode.redis.internal.RedisProperties.getIntegerSystemProperty;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;

public class PassiveExpirationManager {
  private static final Logger logger = LogService.getLogger();

  public static final int DEFAULT_REDIS_EXPIRATION_INTERVAL_SECONDS = 180;

  private final ScheduledExecutorService expirationExecutor;

  public PassiveExpirationManager(RegionProvider regionProvider) {
    int interval = getIntegerSystemProperty(EXPIRATION_INTERVAL_SECONDS,
        DEFAULT_REDIS_EXPIRATION_INTERVAL_SECONDS, 1);

    expirationExecutor = newSingleThreadScheduledExecutor("GemFireRedis-PassiveExpiration-");
    expirationExecutor.scheduleWithFixedDelay(() -> doDataExpiration(regionProvider), interval,
        interval, SECONDS);
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
      for (Map.Entry<RedisKey, RedisData> entry : localPrimaryData.entrySet()) {
        try {
          RedisData value = entry.getValue();
          if (value != null && value.hasExpired(now)) {
            // pttl will do its own check using active expiration and expire the key if needed

            if (-2 == internalPttl(regionProvider, entry.getKey())) {
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

  private long internalPttl(RegionProvider regionProvider, RedisKey key) {
    return regionProvider.lockedExecute(key,
        () -> regionProvider.getRedisData(key).pttl(regionProvider.getLocalDataRegion(), key));
  }
}
