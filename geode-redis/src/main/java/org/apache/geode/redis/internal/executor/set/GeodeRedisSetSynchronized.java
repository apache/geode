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
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;

class GeodeRedisSetSynchronized implements RedisSet {

  private ByteArrayWrapper key;
  private Region<ByteArrayWrapper, DeltaSet> region;

  public GeodeRedisSetSynchronized(ByteArrayWrapper key,
      Region<ByteArrayWrapper, DeltaSet> region) {
    this.key = key;
    this.region = region;
  }

  @Override
  public long sadd(Collection<ByteArrayWrapper> membersToAdd) {
    while (true) {
      DeltaSet deltaSet = region().get(key);
      if (deltaSet == null) {
        // create new set
        if (region.putIfAbsent(key, new DeltaSet(membersToAdd)) == null) {
          return membersToAdd.size();
        } else {
          // retry since another thread concurrently changed the region
        }
      } else {
        // update existing value
        try {
          return deltaSet.customAddAll(membersToAdd, region, key);
        } catch (DeltaSet.RetryDueToConcurrentModification ex) {
          // retry since another thread concurrently changed the region
        }
      }
    }
  }

  @Override
  public long srem(Collection<ByteArrayWrapper> membersToRemove) {
    while (true) {
      DeltaSet deltaSet = region().get(key);
      if (deltaSet == null) {
        return 0L;
      }
      try {
        return deltaSet.customRemoveAll(membersToRemove, region, key);
      } catch (DeltaSet.RetryDueToConcurrentModification ex) {
        // retry since another thread concurrently changed the region
      }
    }
  }

  @Override
  public boolean del() {
    while (true) {
      DeltaSet deltaSet = region().get(key);
      if (deltaSet == null) {
        return false;
      }
      if (deltaSet.delete(region, key)) {
        return true;
      } else {
        // retry since another thread concurrently changed the region
      }
    }
  }

  @Override
  public Set<ByteArrayWrapper> members() {
    DeltaSet deltaSet = region().get(key);
    if (deltaSet != null) {
      return deltaSet.members();
    } else {
      return Collections.emptySet();
    }
  }

  Region<ByteArrayWrapper, DeltaSet> region() {
    return region;
  }

}
