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


import static org.apache.geode.redis.internal.RedisDataType.REDIS_HASH;

import java.util.Collection;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.RedisDataTypeMismatchException;
import org.apache.geode.redis.internal.executor.hash.RedisHash;
import org.apache.geode.redis.internal.executor.hash.RedisHashCommands;

public class RedisHashInRegion implements RedisHashCommands {
  private final Region<ByteArrayWrapper, RedisData> region;

  public RedisHashInRegion(Region<ByteArrayWrapper, RedisData> region) {
    this.region = region;
  }

  @Override
  public int hset(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToSet, boolean NX) {
    RedisHash hash = checkType(region.get(key));
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
  public boolean del(ByteArrayWrapper key) {
    return region.remove(key) != null;
  }

  private RedisHash getRedisHash(ByteArrayWrapper key) {
    return checkType(region.getOrDefault(key, RedisHash.EMPTY));
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
