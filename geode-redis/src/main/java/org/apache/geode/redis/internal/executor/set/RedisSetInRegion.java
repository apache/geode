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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;

/**
 * This class still uses "synchronized" to protect the underlying HashSet even though all writers do
 * so under the {@link SynchronizedStripedExecutor}. The synchronization on this class can be
 * removed once readers are changed to also use the {@link SynchronizedStripedExecutor}.
 */
public class RedisSetInRegion implements RedisSetCommands {
  private Region<ByteArrayWrapper, RedisSet> region;

  @SuppressWarnings("unchecked")
  public RedisSetInRegion(Region<ByteArrayWrapper, RedisSet> region) {
    this.region = region;
  }

  @Override
  public long sadd(
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToAdd) {

    RedisSet redisSet = region.get(key);

    if (redisSet != null) {
      return redisSet.doSadd(membersToAdd, region, key);
    } else {
      region.create(key, new RedisSet(membersToAdd));
      return membersToAdd.size();
    }
  }

  @Override
  public long srem(
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToRemove, AtomicBoolean setWasDeleted) {
    RedisSet redisSet = region.get(key);
    if (redisSet == null) {
      return 0L;
    }
    return redisSet.doSrem(membersToRemove, region, key, setWasDeleted);
  }

  public boolean del(ByteArrayWrapper key) {
    return region.remove(key) != null;
  }

  @Override
  public Set<ByteArrayWrapper> smembers(
      ByteArrayWrapper key) {
    RedisSet redisSet = region.get(key);
    if (redisSet != null) {
      return redisSet.members();
    } else {
      return emptySet();
    }
  }

  @Override
  public int scard(ByteArrayWrapper key) {
    RedisSet redisSet = region.get(key);
    if (redisSet != null) {
      return redisSet.size();
    } else {
      return 0;
    }
  }

  @Override
  public boolean sismember(
      ByteArrayWrapper key, ByteArrayWrapper member) {
    RedisSet redisSet = region.get(key);
    if (redisSet != null) {
      return redisSet.contains(member);
    } else {
      return false;
    }
  }

  @Override
  public Collection<ByteArrayWrapper> srandmember(
      ByteArrayWrapper key, int count) {
    RedisSet redisSet = region.get(key);
    if (redisSet != null) {
      return redisSet.srandmember(count);
    } else {
      return emptyList();
    }
  }

  @Override
  public Collection<ByteArrayWrapper> spop(
      ByteArrayWrapper key, int popCount) {
    RedisSet redisSet = region.get(key);
    if (redisSet != null) {
      return redisSet.doSpop(region, key, popCount);
    } else {
      return emptyList();
    }
  }

  @Override
  public List<Object> sscan(
      ByteArrayWrapper key, Pattern matchPattern, int count, int cursor) {
    RedisSet redisSet = region.get(key);
    if (redisSet != null) {
      return redisSet.doSscan(matchPattern, count, cursor);
    } else {
      return emptyList();
    }
  }

  // private RedisSet get(ByteArrayWrapper key) {
  // RedisSet redisSet = region.get(key);
  // return redisSet == null? new RedisSet(new ArrayList<>()) : redisSet;
  // }
}
