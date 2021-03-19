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

import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_HASH;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_SET;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_STRING;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_HASH;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SET;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_STRING;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.executor.StripedExecutor;
import org.apache.geode.redis.internal.statistics.RedisStats;

/**
 * Provides methods to help implement command execution.
 * This class provides resources needed to execute
 * a command on RedisData, for example the region the data
 * is stored in and the stats that need to be updated.
 * It does not keep any state changed by a command so a
 * single instance of it can be used concurrently by
 * multiple commands and a canonical instance can be used
 * to prevent garbage creation.
 */
public class CommandHelper {
  private final Region<ByteArrayWrapper, RedisData> region;
  private final RedisStats redisStats;
  private final StripedExecutor stripedExecutor;

  public Region<ByteArrayWrapper, RedisData> getRegion() {
    return region;
  }

  public RedisStats getRedisStats() {
    return redisStats;
  }

  public StripedExecutor getStripedExecutor() {
    return stripedExecutor;
  }

  public CommandHelper(
      Region<ByteArrayWrapper, RedisData> region,
      RedisStats redisStats,
      StripedExecutor stripedExecutor) {
    this.region = region;
    this.redisStats = redisStats;
    this.stripedExecutor = stripedExecutor;
  }

  RedisData getRedisData(ByteArrayWrapper key) {
    return getRedisData(key, NullRedisDataStructures.NULL_REDIS_DATA);
  }

  RedisData getRedisData(ByteArrayWrapper key, RedisData notFoundValue) {
    RedisData result = region.get(key);
    if (result != null) {
      if (result.hasExpired()) {
        result.doExpiration(this, key);
        result = null;
      }
    }
    if (result == null) {
      return notFoundValue;
    } else {
      return result;
    }
  }

  RedisSet getRedisSet(ByteArrayWrapper key, boolean updateStats) {
    RedisData redisData = getRedisData(key, NULL_REDIS_SET);
    if (updateStats) {
      if (redisData == NULL_REDIS_SET) {
        redisStats.incKeyspaceMisses();
      } else {
        redisStats.incKeyspaceHits();
      }
    }
    return checkSetType(redisData);
  }

  private RedisSet checkSetType(RedisData redisData) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != REDIS_SET) {
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }
    return (RedisSet) redisData;
  }

  RedisHash getRedisHash(ByteArrayWrapper key, boolean updateStats) {
    RedisData redisData = getRedisData(key, NULL_REDIS_HASH);
    if (updateStats) {
      if (redisData == NULL_REDIS_HASH) {
        redisStats.incKeyspaceMisses();
      } else {
        redisStats.incKeyspaceHits();
      }
    }
    return checkHashType(redisData);
  }

  private RedisHash checkHashType(RedisData redisData) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != REDIS_HASH) {
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }
    return (RedisHash) redisData;
  }

  private RedisString checkStringType(RedisData redisData, boolean ignoreTypeMismatch) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != REDIS_STRING) {
      if (ignoreTypeMismatch) {
        return NULL_REDIS_STRING;
      }
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }
    return (RedisString) redisData;
  }

  RedisString getRedisString(ByteArrayWrapper key, boolean updateStats) {
    RedisData redisData = getRedisData(key, NULL_REDIS_STRING);
    if (updateStats) {
      if (redisData == NULL_REDIS_STRING) {
        redisStats.incKeyspaceMisses();
      } else {
        redisStats.incKeyspaceHits();
      }
    }

    return checkStringType(redisData, false);
  }

  RedisString getRedisStringIgnoringType(ByteArrayWrapper key, boolean updateStats) {
    RedisData redisData = getRedisData(key, NULL_REDIS_STRING);
    if (updateStats) {
      if (redisData == NULL_REDIS_STRING) {
        redisStats.incKeyspaceMisses();
      } else {
        redisStats.incKeyspaceHits();
      }
    }

    return checkStringType(redisData, true);
  }

  RedisString setRedisString(ByteArrayWrapper key, ByteArrayWrapper value) {
    RedisString result;
    RedisData redisData = getRedisData(key);

    if (redisData.isNull() || redisData.getType() != REDIS_STRING) {
      result = new RedisString(value);
    } else {
      result = (RedisString) redisData;
      result.set(value);
    }
    region.put(key, result);
    return result;
  }
}
