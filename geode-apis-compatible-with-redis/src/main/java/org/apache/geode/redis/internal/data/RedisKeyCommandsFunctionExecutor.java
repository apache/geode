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


import java.util.Arrays;
import java.util.List;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.executor.key.RedisKeyCommands;
import org.apache.geode.redis.internal.executor.key.RestoreOptions;

public class RedisKeyCommandsFunctionExecutor extends RedisDataCommandsFunctionExecutor implements
    RedisKeyCommands {

  public RedisKeyCommandsFunctionExecutor(RegionProvider regionProvider,
      CacheTransactionManager txManager) {
    super(regionProvider, txManager);
  }

  @Override
  public synchronized boolean del(RedisKey key) {
    return stripedExecute(key, () -> {
      // Trigger MOVED if necessary
      getRedisData(key);
      return getRegion().remove(key) != null;
    });
  }

  @Override
  public boolean exists(RedisKey key) {
    boolean keyExists = stripedExecute(key, () -> getRedisData(key).exists());

    if (keyExists) {
      getRegionProvider().getRedisStats().incKeyspaceHits();
    } else {
      getRegionProvider().getRedisStats().incKeyspaceMisses();
    }

    return keyExists;
  }

  @Override
  public long pttl(RedisKey key) {
    long result = stripedExecute(key, () -> getRedisData(key).pttl(getRegion(), key));

    if (result == -2) {
      getRegionProvider().getRedisStats().incKeyspaceMisses();
    } else {
      getRegionProvider().getRedisStats().incKeyspaceHits();
    }

    return result;
  }

  @Override
  public long internalPttl(RedisKey key) {
    return stripedExecute(key, () -> getRedisData(key).pttl(getRegion(), key));
  }

  @Override
  public int pexpireat(RedisKey key, long timestamp) {
    return stripedExecute(key,
        () -> getRedisData(key).pexpireat(getRegionProvider(), key, timestamp));
  }

  @Override
  public int persist(RedisKey key) {
    return stripedExecute(key, () -> getRedisData(key).persist(getRegion(), key));
  }

  @Override
  public String type(RedisKey key) {
    String type = stripedExecute(key, () -> getRedisData(key).type());

    if (type.equalsIgnoreCase("none")) {
      getRegionProvider().getRedisStats().incKeyspaceMisses();
    } else {
      getRegionProvider().getRedisStats().incKeyspaceHits();
    }

    return type;
  }

  @Override
  public String internalType(RedisKey key) {
    return stripedExecute(key, () -> getRedisData(key).type());
  }

  @Override
  public byte[] dump(RedisKey key) {
    byte[] dumpBytes = stripedExecute(key, () -> getRedisData(key).dump());

    if (dumpBytes == null) {
      getRegionProvider().getRedisStats().incKeyspaceMisses();
    } else {
      getRegionProvider().getRedisStats().incKeyspaceHits();
    }

    return dumpBytes;
  }

  @Override
  public void restore(RedisKey key, long ttl, byte[] data, RestoreOptions options) {
    long expireAt;
    if (ttl == 0) {
      expireAt = AbstractRedisData.NO_EXPIRATION;
    } else {
      if (options.isAbsttl()) {
        expireAt = ttl;
      } else {
        expireAt = System.currentTimeMillis() + ttl;
      }
    }

    stripedExecute(key, () -> {
      RedisData value = getRedisData(key).restore(data, options.isReplace());
      ((AbstractRedisData) value).setExpirationTimestampNoDelta(expireAt);
      getRegion().put(key, value);
      return null;
    });
  }

  @Override
  public boolean rename(RedisKey oldKey, RedisKey newKey) {
    List<RedisKey> lockOrdering = Arrays.asList(oldKey, newKey);

    return stripedExecute(oldKey, lockOrdering,
        () -> getRedisData(oldKey).rename(getRegionProvider().getLocalDataRegion(), oldKey,
            newKey));
  }

}
