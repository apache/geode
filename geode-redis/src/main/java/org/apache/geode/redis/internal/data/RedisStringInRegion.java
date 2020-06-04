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

import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_STRING;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.executor.string.RedisStringCommands;
import org.apache.geode.redis.internal.executor.string.SetOptions;

public class RedisStringInRegion extends RedisKeyInRegion implements RedisStringCommands {

  public RedisStringInRegion(Region<ByteArrayWrapper, RedisData> region) {
    super(region);
  }

  @Override
  public long append(ByteArrayWrapper key, ByteArrayWrapper valueToAppend) {
    RedisString redisString = getRedisString(key);

    if (redisString != null) {
      return redisString.append(valueToAppend, region, key);
    } else {
      region.put(key, new RedisString(valueToAppend));
      return valueToAppend.length();
    }
  }

  @Override
  public ByteArrayWrapper get(ByteArrayWrapper key) {
    RedisString redisString = getRedisString(key);

    if (redisString != null) {
      return redisString.get();
    } else {
      return null;
    }
  }

  @Override
  public boolean set(ByteArrayWrapper key, ByteArrayWrapper value, SetOptions options) {
    if (options != null) {
      if (options.isNX()) {
        return setnx(key, value, options);
      }

      if (options.isXX() && getRedisData(key) == null) {
        return false;
      }
    }

    RedisString redisString = getRedisStringForSet(key);
    if (redisString == null) {
      redisString = new RedisString(value);
    } else {
      redisString.set(value);
    }
    handleSetExpiration(redisString, options);
    region.put(key, redisString);
    return true;
  }

  @Override
  public long incr(ByteArrayWrapper key) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      byte[] newValue = {Coder.NUMBER_1_BYTE};
      redisString = new RedisString(new ByteArrayWrapper(newValue));
      region.put(key, redisString);
      return 1;
    }

    return redisString.incr(region, key);
  }

  @Override
  public long decr(ByteArrayWrapper key) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      redisString = new RedisString(new ByteArrayWrapper(Coder.stringToBytes("-1")));
      region.put(key, redisString);
      return -1;
    }

    return redisString.decr(region, key);
  }

  @Override
  public ByteArrayWrapper getset(ByteArrayWrapper key, ByteArrayWrapper value) {
    ByteArrayWrapper result = null;
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      region.put(key, new RedisString(value));
    } else {
      result = redisString.get();
      redisString.set(value);
      redisString.persistNoDelta();
      region.put(key, redisString);
    }
    return result;
  }

  @Override
  public long incrby(ByteArrayWrapper key, long increment) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      byte[] newValue = Coder.stringToBytes(Long.toString(increment));
      redisString = new RedisString(new ByteArrayWrapper(newValue));
      region.put(key, redisString);
      return increment;
    }

    return redisString.incrby(region, key, increment);
  }

  @Override
  public long decrby(ByteArrayWrapper key, long decrement) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      byte[] newValue = Coder.stringToBytes(Long.toString(-decrement));
      redisString = new RedisString(new ByteArrayWrapper(newValue));
      region.put(key, redisString);
      return -decrement;
    }

    return redisString.decrby(region, key, decrement);
  }

  private boolean setnx(ByteArrayWrapper key, ByteArrayWrapper value, SetOptions options) {
    if (getRedisData(key) != null) {
      return false;
    }
    RedisString redisString = new RedisString(value);
    handleSetExpiration(redisString, options);
    region.put(key, redisString);
    return true;
  }

  private void handleSetExpiration(RedisString redisString, SetOptions options) {
    long setExpiration = options == null ? 0L : options.getExpiration();
    if (setExpiration != 0) {
      long now = System.currentTimeMillis();
      long timestamp = now + setExpiration;
      redisString.setExpirationTimestampNoDelta(timestamp);
    } else if (options == null || !options.isKeepTTL()) {
      redisString.persistNoDelta();
    }
  }

  private RedisString checkType(RedisData redisData) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != REDIS_STRING) {
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }
    return (RedisString) redisData;
  }

  private RedisString getRedisString(ByteArrayWrapper key) {
    return checkType(getRedisData(key));
  }

  private RedisString getRedisStringForSet(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null || redisData.getType() != REDIS_STRING) {
      return null;
    }
    return (RedisString) redisData;
  }
}
