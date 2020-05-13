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


import java.util.Collection;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.hash.RedisHash;
import org.apache.geode.redis.internal.executor.hash.RedisHashCommands;

public class RedisHashInRegion implements RedisHashCommands {
  private final Region<ByteArrayWrapper, RedisHash> localRegion;

  public RedisHashInRegion(Region localRegion) {
    this.localRegion = localRegion;
  }

  @Override
  public int hset(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToSet, boolean NX) {
    RedisHash hash = localRegion.get(key);
    if (hash != null) {
      return hash.hset(localRegion, key, fieldsToSet, NX);
    } else {
      localRegion.put(key, new RedisHash(fieldsToSet));
      return fieldsToSet.size() / 2;
    }
  }

  @Override
  public int hdel(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToRemove) {
    RedisHash hash = localRegion.getOrDefault(key, RedisHash.EMPTY);
    return hash.hdel(localRegion, key, fieldsToRemove);
  }

  @Override
  public Collection<ByteArrayWrapper> hgetall(ByteArrayWrapper key) {
    RedisHash hash = localRegion.getOrDefault(key, RedisHash.EMPTY);
    return hash.hgetall();
  }

  @Override
  public boolean del(ByteArrayWrapper key) {
    return localRegion.remove(key) != null;
  }
}
