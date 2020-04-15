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

package org.apache.geode.redis.internal.executor.set;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;

class GeodeRedisSetSynchronized implements RedisSet {
  private static final Logger logger = LogService.getLogger();

  private ByteArrayWrapper key;
  private Region<ByteArrayWrapper, Set<ByteArrayWrapper>> region;


  public GeodeRedisSetSynchronized(ByteArrayWrapper key,
      Region<ByteArrayWrapper, Set<ByteArrayWrapper>> region) {
    this.key = key;
    this.region = region;
  }

  @Override
  public long sadd(Collection<ByteArrayWrapper> membersToAdd) {
    boolean created;
    do {
      DeltaSet deltaSet = (DeltaSet) region().get(key);
      if (deltaSet != null) {
        logger.info("DEBUG: in GeodeRedisSetSynchronized sadd update path");
        // update existing value
        return deltaSet.customAddAll(membersToAdd, region, key);
      }
      created = region.putIfAbsent(key, new DeltaSet(membersToAdd)) == null;
      logger.info("DEBUG: in GeodeRedisSetSynchronized sadd created=" + created);
    } while (!created);
    return membersToAdd.size();
  }

  @Override
  public long srem(Collection<ByteArrayWrapper> membersToRemove) {
    AtomicLong removedCount = new AtomicLong();
    region().computeIfPresent(key, (_unusedKey_, oldValue) -> {
      Set<ByteArrayWrapper> newValue = new HashSet<>(oldValue);
      newValue.removeAll(membersToRemove);
      removedCount.set(oldValue.size() - newValue.size());

      return newValue;
    });

    if (members().isEmpty()) {
      RedisDataType type = context.getKeyRegistrar().getType(key);
      if (type == RedisDataType.REDIS_SET) {
        context.getRegionProvider().removeKey(key, type);
      }
    }

    return removedCount.longValue();
  }

  @Override
  public Set<ByteArrayWrapper> members() {
    return region().getOrDefault(key, Collections.emptySet());
  }

  Region<ByteArrayWrapper, Set<ByteArrayWrapper>> region() {
    return region;
  }

}
