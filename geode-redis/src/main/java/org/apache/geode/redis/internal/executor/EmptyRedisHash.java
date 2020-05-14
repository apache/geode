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

import static java.util.Collections.emptyList;

import java.util.Collection;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.executor.hash.RedisHash;

public class EmptyRedisHash extends RedisHash {
  @Override
  public synchronized int hset(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      List<ByteArrayWrapper> fieldsToSet, boolean nx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized int hdel(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      List<ByteArrayWrapper> fieldsToRemove) {
    return 0;
  }

  @Override
  public synchronized Collection<ByteArrayWrapper> hgetall() {
    return emptyList();
  }

  @Override
  public synchronized boolean isEmpty() {
    return true;
  }

  @Override
  public synchronized boolean containsKey(ByteArrayWrapper field) {
    return false;
  }

  @Override
  public synchronized ByteArrayWrapper get(ByteArrayWrapper field) {
    return null;
  }

  @Override
  public synchronized int size() {
    return 0;
  }

}
