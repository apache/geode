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

import org.apache.geode.Delta;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;

class GeodeRedisSetSynchronized implements RedisSet {

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
        // update existing value
        return deltaSet.customAddAll(membersToAdd, region, key);
      }
      created = region.putIfAbsent(key, new DeltaSet(membersToAdd)) == null;
    } while (!created);
    return membersToAdd.size();
  }

  @Override
  public long srem(Collection<ByteArrayWrapper> membersToRemove) {
    DeltaSet deltaSet = (DeltaSet) region().get(key);

    if(deltaSet == null) {
      return 0L;
    }

    return deltaSet.customRemoveAll(membersToRemove, region, key);
  }

  @Override
  public Set<ByteArrayWrapper> members() {
    return region().getOrDefault(key, Collections.emptySet());
  }

  Region<ByteArrayWrapper, Set<ByteArrayWrapper>> region() {
    return region;
  }

}
