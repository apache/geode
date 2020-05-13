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

class EmptyRedisSet extends RedisSet {

  @Override
  synchronized List<Object> sscan(Pattern matchPattern, int count, int cursor) {
    return emptyList();
  }

  @Override
  synchronized Collection<ByteArrayWrapper> spop(Region<ByteArrayWrapper, RedisSet> region,
      ByteArrayWrapper key, int popCount) {
    return emptyList();
  }

  @Override
  synchronized Collection<ByteArrayWrapper> srandmember(int count) {
    return emptyList();
  }

  @Override
  public synchronized boolean sismember(ByteArrayWrapper member) {
    return false;
  }

  @Override
  public synchronized int scard() {
    return 0;
  }

  @Override
  synchronized long sadd(ArrayList<ByteArrayWrapper> membersToAdd,
      Region<ByteArrayWrapper, RedisSet> region, ByteArrayWrapper key) {
    throw new UnsupportedOperationException();
  }

  @Override
  synchronized long srem(ArrayList<ByteArrayWrapper> membersToRemove,
      Region<ByteArrayWrapper, RedisSet> region, ByteArrayWrapper key,
      AtomicBoolean setWasDeleted) {
    return 0;
  }

  @Override
  synchronized Set<ByteArrayWrapper> smembers() {
    return emptySet();
  }
}
