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


import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_HASH;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisStats;
import org.apache.geode.redis.internal.executor.hash.RedisHashCommands;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisHashInRegion extends RedisKeyInRegion implements RedisHashCommands {

  public RedisHashInRegion(Region<ByteArrayWrapper, RedisData> region,
      RedisStats redisStats) {
    super(region, redisStats);
  }

  @Override
  public int hset(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToSet, boolean NX) {
    RedisHash hash = checkType(getRedisData(key));
    if (hash != null) {
      return hash.hset(region, key, fieldsToSet, NX);
    } else {
      region.put(key, new RedisHash(fieldsToSet));
      return fieldsToSet.size() / 2;
    }
  }

  @Override
  public int hdel(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToRemove) {
    return getRedisHash(key).hdel(region, key, fieldsToRemove);
  }

  @Override
  public Collection<ByteArrayWrapper> hgetall(ByteArrayWrapper key) {
    return getRedisHash(key).hgetall();
  }

  @Override
  public int hexists(ByteArrayWrapper key, ByteArrayWrapper field) {
    return getRedisHash(key).hexists(field);
  }

  @Override
  public ByteArrayWrapper hget(ByteArrayWrapper key, ByteArrayWrapper field) {
    return getRedisHash(key).hget(field);
  }

  @Override
  public int hlen(ByteArrayWrapper key) {
    return getRedisHash(key).hlen();
  }

  @Override
  public int hstrlen(ByteArrayWrapper key, ByteArrayWrapper field) {
    return getRedisHash(key).hstrlen(field);
  }

  @Override
  public List<ByteArrayWrapper> hmget(ByteArrayWrapper key, List<ByteArrayWrapper> fields) {
    return getRedisHash(key).hmget(fields);
  }

  @Override
  public Collection<ByteArrayWrapper> hvals(ByteArrayWrapper key) {
    return getRedisHash(key).hvals();
  }

  @Override
  public Collection<ByteArrayWrapper> hkeys(ByteArrayWrapper key) {
    return getRedisHash(key).hkeys();
  }

  @Override
  public List<Object> hscan(ByteArrayWrapper key, Pattern matchPattern, int count, int cursor) {
    return getRedisHash(key).hscan(matchPattern, count, cursor);
  }

  @Override
  public long hincrby(ByteArrayWrapper key, ByteArrayWrapper field, long increment) {
    RedisHash hash = checkType(getRedisData(key));
    if (hash != null) {
      return hash.hincrby(region, key, field, increment);
    } else {
      region.put(key,
          new RedisHash(Arrays.asList(field, new ByteArrayWrapper(Coder.longToBytes(increment)))));
      return increment;
    }
  }

  @Override
  public double hincrbyfloat(ByteArrayWrapper key, ByteArrayWrapper field, double increment) {
    RedisHash hash = checkType(getRedisData(key));
    if (hash != null) {
      return hash.hincrbyfloat(region, key, field, increment);
    } else {
      region.put(key,
          new RedisHash(
              Arrays.asList(field, new ByteArrayWrapper(Coder.doubleToBytes(increment)))));
      return increment;
    }
  }

  private RedisHash getRedisHash(ByteArrayWrapper key) {
    return checkType(getRedisDataOrDefault(key, RedisHash.EMPTY));
  }

  public static RedisHash checkType(RedisData redisData) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != REDIS_HASH) {
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }
    return (RedisHash) redisData;
  }

}
