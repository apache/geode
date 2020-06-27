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

package org.apache.geode.redis.internal.data;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.RedisStats;
import org.apache.geode.redis.internal.executor.key.RedisKeyCommands;

public class RedisKeyInRegion implements RedisKeyCommands {
  protected final Region<ByteArrayWrapper, RedisData> region;
  private final RedisStats redisStats;

  @SuppressWarnings("unchecked")
  public RedisKeyInRegion(Region region, RedisStats redisStats) {
    this.region = region;
    this.redisStats = redisStats;
  }

  @Override
  public boolean del(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return false;
    }
    return region.remove(key) != null;
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
    return redisData.pttl(region, key);
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
      doExpiration(key);
    } else {
      redisData.setExpirationTimestamp(region, key, timestamp);
    }
    return 1;
  }

  @Override
  public int persist(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return 0;
    }
    return redisData.persist(region, key);
  }

  @Override
  public String type(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return "none";
    }
    return redisData.getType().toString();
  }

  protected RedisData getRedisData(ByteArrayWrapper key) {
    return getRedisDataOrDefault(key, null);
  }

  protected RedisData getRedisDataOrDefault(ByteArrayWrapper key, RedisData defaultValue) {
    RedisData result = region.get(key);
    if (result != null) {
      if (result.hasExpired()) {
        doExpiration(key);
        result = null;
      }
    }
    if (result == null) {
      return defaultValue;
    } else {
      return result;
    }
  }

  private void doExpiration(ByteArrayWrapper key) {
    long start = redisStats.startExpiration();
    region.remove(key);
    redisStats.endExpiration(start);
  }

  public boolean rename(ByteArrayWrapper oldKey, ByteArrayWrapper newKey) {
    RedisData value = getRedisData(oldKey);
    if (value == null) {
      return false;
    }

    region.put(newKey, value);
    region.remove(oldKey);

    return true;
  }
}
