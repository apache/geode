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

package org.apache.geode.redis.internal.executor;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.RegionProvider;

public class RedisKeyInRegion implements RedisKeyCommands {
  protected final Region<ByteArrayWrapper, RedisData> region;
  private final RegionProvider regionProvider;

  @SuppressWarnings("unchecked")
  public RedisKeyInRegion(Region region, RegionProvider regionProvider) {
    this.region = region;
    this.regionProvider = regionProvider;
  }

  @Override
  public boolean del(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return false;
    }
    boolean result = region.remove(key) != null;
    if (result) {
      regionProvider.cancelKeyExpiration(key);
    }
    return result;
  }

  @Override
  public boolean exists(ByteArrayWrapper key) {
    return getRedisData(key) != null;
  }

  @Override
  public long pttl(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return -2;
    }
    switch (redisData.getType()) {
      case REDIS_SET:
      case REDIS_HASH: {
        return redisData.pttl(region, key);
      }
      default:
        return regionProvider.getExpirationDelayMillis(key);
    }
  }

  @Override
  public int pexpireat(ByteArrayWrapper key, long timestamp) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return 0;
    }
    long now = System.currentTimeMillis();
    if (now >= timestamp) {
      // already expired
      del(key);
    } else {
      switch (redisData.getType()) {
        case REDIS_SET:
        case REDIS_HASH:
          redisData.setExpirationTimestamp(region, key, timestamp);
          break;
        default:
          if (regionProvider.hasExpiration(key)) {
            regionProvider.modifyExpiration(key, timestamp - now);
          } else {
            regionProvider.setExpiration(key, timestamp - now);
          }
          break;
      }
    }
    return 1;
  }

  @Override
  public int persist(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return 0;
    }
    switch (redisData.getType()) {
      case REDIS_SET:
      case REDIS_HASH:
        return redisData.persist(region, key);
      default:
        if (regionProvider.cancelKeyExpiration(key)) {
          return 1;
        } else {
          return 0;
        }
    }
  }

  protected RedisData getRedisData(ByteArrayWrapper key) {
    return getRedisDataOrDefault(key, null);
  }

  protected RedisData getRedisDataOrDefault(ByteArrayWrapper key, RedisData defaultValue) {
    RedisData result = region.get(key);
    if (result != null) {
      if (result.hasExpired()) {
        region.remove(key);
        result = null;
      }
    }
    if (result == null) {
      return defaultValue;
    } else {
      return result;
    }
  }
}
