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

package org.apache.geode.redis.internal.executor.hash;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;

class GeodeRedisHashSynchronized implements RedisHash {
  private final ByteArrayWrapper key;
  private final ExecutionHandlerContext context;

  public GeodeRedisHashSynchronized(ByteArrayWrapper key, ExecutionHandlerContext context) {
    this.key = key;
    this.context = context;
  }

  @Override
  public int hset(List<ByteArrayWrapper> fieldsToSet,
      boolean NX) {
    AtomicInteger fieldsAdded = new AtomicInteger();

    Map<ByteArrayWrapper, ByteArrayWrapper> computedHash =
        region().compute(key, (_unused_, oldHash) -> {

          fieldsAdded.set(0);
          HashMap<ByteArrayWrapper, ByteArrayWrapper> newHash;
          if (oldHash == null) {
            newHash = new HashMap<>();
          } else {
            newHash = new HashMap<>(oldHash);
          }

          for (int i = 0; i < fieldsToSet.size(); i += 2) {
            ByteArrayWrapper field = fieldsToSet.get(i);
            ByteArrayWrapper value = fieldsToSet.get(i + 1);

            if (NX) {
              newHash.putIfAbsent(field, value);
            } else {
              newHash.put(field, value);
            }
          }
          if (oldHash == null) {
            fieldsAdded.set(newHash.size());
          } else {
            fieldsAdded.set(newHash.size() - oldHash.size());
          }
          return newHash;
        });

    if (computedHash != null) {
      context.getKeyRegistrar().register(this.key, RedisDataType.REDIS_HASH);
    }

    return fieldsAdded.get();
  }

  @Override
  public int hdel(List<ByteArrayWrapper> subList) {
    AtomicLong numDeleted = new AtomicLong();
    region().computeIfPresent(key, (_unused_, oldHash) -> {
      HashMap<ByteArrayWrapper, ByteArrayWrapper> newHash = new HashMap<>(oldHash);
      for (ByteArrayWrapper fieldToRemove : subList) {
        newHash.remove(fieldToRemove);
      }
      numDeleted.set(oldHash.size() - newHash.size());
      return newHash;
    });
    return numDeleted.intValue();
  }

  @Override
  public Collection<Map.Entry<ByteArrayWrapper, ByteArrayWrapper>> hgetall() {
    return region().getOrDefault(key, Collections.emptyMap()).entrySet();
  }

  private Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> region() {
    return context.getRegionProvider().getHashRegion();
  }


}
