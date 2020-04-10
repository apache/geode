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

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;

class GeodeRedisSetSynchronized implements RedisSet {

  private ByteArrayWrapper key;
  private ExecutionHandlerContext context;

  public GeodeRedisSetSynchronized(ByteArrayWrapper key, ExecutionHandlerContext context) {
    this.key = key;
    this.context = context;
  }

  @Override
  public long sadd(Collection<ByteArrayWrapper> membersToAdd) {
    AtomicLong addedCount = new AtomicLong();
    region().compute(key, (_unusedKey_, currentValue) -> {
      if (currentValue == null) {
        addedCount.set(membersToAdd.size());
        return createSet(membersToAdd);
      }

      Set<ByteArrayWrapper> newValue = createSet(currentValue);
      newValue.addAll(membersToAdd);
      addedCount.set(newValue.size() - currentValue.size());
      return newValue;
    });
    return addedCount.longValue();
  }

  @Override
  public long srem(Collection<ByteArrayWrapper> membersToRemove) {
    AtomicLong removedCount = new AtomicLong();
    region().computeIfPresent(key, (_unusedKey_, oldValue) -> {
      Set<ByteArrayWrapper> newValue = createSet(oldValue);
      newValue.removeAll(membersToRemove);
      removedCount.set(oldValue.size() - newValue.size());
      return newValue;
    });
    return removedCount.longValue();
  }

  @Override
  public Set<ByteArrayWrapper> members() {
    return region().getOrDefault(key, Collections.emptySet());
  }

  Region<ByteArrayWrapper, Set<ByteArrayWrapper>> region() {
    return context.getRegionProvider().getSetRegion();
  }

  private Set<ByteArrayWrapper> createSet(Collection<ByteArrayWrapper> membersToAdd) {
    return new HashSet<>(membersToAdd);
  }
}
